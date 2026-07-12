//! [`QuicEngine`] — the main facade, one per Reactor core.
//!
//! Implements [`io::IoHandler`] so the io-layer Reactor feeds received
//! datagrams directly to the QUIC engine and calls `on_tick` for send
//! flushing.
//!
//! # Data Flow
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────┐
//! │                      QuicEngine (Reactor N)                      │
//! │                                                                  │
//! │  on_datagram(data, peer):                                        │
//! │    1. Copy data → pkt_buf                                        │
//! │    2. quiche::Header::from_slice → extract DCID                  │
//! │    3. Route by DCID:                                             │
//! │       a. Found  → conn.recv(pkt_buf, RecvInfo)                   │
//! │       b. Initial → quiche::accept() → insert into table          │
//! │    4. Process readable streams                                   │
//! │                                                                  │
//! │  on_tick(ctx):                                                   │
//! │    For each connection:                                          │
//! │      conn.send(out_buf) → (len, SendInfo) → send_blocking()      │
//! │    Clean up closed connections.                                  │
//! └──────────────────────────────────────────────────────────────────┘
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;

use buffer_pool::BufferPool;
use frame::codec;
use io::handler::{IoHandler, TickContext};

use crate::config::QuicConfig;
use crate::gso::GsoBatcher;

/// Maximum QUIC packet size — covers full IPv4 UDP jumbo payload
/// (65535 - IP/UDP overhead). Must match the io-layer receive buffer
/// and the client's `max_send_udp_payload_size`.
const MAX_PKT_SIZE: usize = 65535;

/// Maximum pending events before the engine starts dropping. Prevents
/// unbounded Vec growth if the upper layer is slow to drain_events().
/// At ~200 bytes per event, 8192 events ≈ 1.6 MiB — negligible.
pub const MAX_PENDING_EVENTS: usize = 8192;

/// Events emitted by the QUIC engine for upper-layer processing.
///
/// Collected during `on_datagram` / `on_tick` and drained by the
/// caller via [`QuicEngine::drain_events`].
#[derive(Debug)]
pub enum QuicEvent {
    /// A QUIC connection completed the TLS 1.3 handshake.
    ConnectionEstablished {
        /// Slab index of the connection.
        conn_idx: usize,
        /// Peer address.
        peer_addr: SocketAddr,
    },
    /// A QUIC connection was closed (cleanly or with error).
    ConnectionClosed {
        /// Slab index of the connection.
        conn_idx: usize,
        /// Peer address.
        peer_addr: SocketAddr,
        /// Error description from quiche, if any.
        reason: Option<String>,
    },
    /// A decoded frame was received on a stream.
    FrameReceived {
        /// Slab index of the connection.
        conn_idx: usize,
        /// QUIC stream id the frame arrived on (used for responses).
        quic_stream_id: u64,
        /// Codec-layer frame header (stream_id, seq, msg_type, flags).
        header: codec::FrameHeader,
        /// Payload backed by a pool buffer (recycled on drop).
        payload: buffer_pool::Buffer,
    },
    /// A stream signaled FIN (graceful close).
    StreamFinished {
        /// Slab index of the connection.
        conn_idx: usize,
        /// QUIC stream id.
        stream_id: u64,
    },
}

/// Errors returned by [`QuicEngine::send_stream`].
#[derive(Debug)]
pub enum QuicSendError {
    /// `conn_idx` does not map to an active connection.
    UnknownConnection,
    /// The QUIC stream is blocked by flow / congestion control.
    ///
    /// The caller may retry on a later tick. Data is NOT buffered by the
    /// engine — the application owns retry.
    FlowBlocked,
    /// A lower-level quiche error.
    Quiche(quiche::Error),
}

/// One entry in the connection slab.
struct ConnEntry {
    conn: quiche::Connection,
    peer_addr: SocketAddr,
    /// Tracks whether we've already emitted `ConnectionEstablished`.
    established: bool,
    /// Routing key derived from the server's source CID (20 bytes).
    scid_key: [u8; quiche::MAX_CONN_ID_LEN],
    /// Routing key derived from the client's Initial DCID (for Initial
    /// retransmissions that arrive after the connection exists).
    initial_dcid_key: [u8; quiche::MAX_CONN_ID_LEN],
}

/// Copies a quiche `ConnectionId` into a fixed-size zero-padded array.
///
/// Used as a `HashMap` key to avoid per-datagram heap allocation (`to_vec`).
/// Both the incoming packet DCID and the stored `source_id` are padded
/// identically, so lookups are consistent.
#[inline]
fn pad_dcid(cid: &quiche::ConnectionId) -> [u8; quiche::MAX_CONN_ID_LEN] {
    let mut key = [0u8; quiche::MAX_CONN_ID_LEN];
    let len = cid.len().min(quiche::MAX_CONN_ID_LEN);
    key[..len].copy_from_slice(&cid[..len]);
    key
}

/// The QUIC engine — one per Reactor core. Implements [`IoHandler`] so
/// the io-layer feeds packets directly.
pub struct QuicEngine {
    /// Reactor / shard identifier.
    pub reactor_id: u32,
    /// Engine-wide configuration.
    pub config: QuicConfig,
    /// quiche protocol configuration (reused for all new connections).
    quiche_config: quiche::Config,
    /// Local socket address (passed to quiche as `to` in RecvInfo).
    local_addr: SocketAddr,

    // ── Connection storage ────────────────────────────────────────
    /// Slab of connections (None = freed slot).
    connections: Vec<Option<ConnEntry>>,
    /// Free slots in `connections` — recycled when connections close
    /// so the Vec doesn't grow unbounded under connection churn.
    free_slots: Vec<usize>,
    /// DCID → slab index routing table. Uses a fixed-size zero-padded
    /// array key to avoid per-datagram heap allocation.
    dcid_to_idx: HashMap<[u8; quiche::MAX_CONN_ID_LEN], usize>,

    // ── Reusable buffers ──────────────────────────────────────────
    /// Buffer for incoming packet header parsing + recv.
    pkt_buf: Vec<u8>,
    /// Buffer for outgoing packet generation.
    out_buf: Vec<u8>,
    /// Reusable scratch for collecting readable stream IDs (avoids
    /// per-datagram Vec allocation).
    readable_buf: Vec<u64>,
    /// Reusable scratch for collecting closed connection indices (avoids
    /// per-tick Vec allocation).
    closed_buf: Vec<usize>,
    /// Per-stream reassembly buffer for frames spanning multiple
    /// `stream_recv` calls. Keyed by QUIC stream ID.
    stream_carryover: HashMap<u64, bytes::BytesMut>,

    // ── Buffer pool ───────────────────────────────────────────────
    /// Pool for frame payload buffers (zero-alloc hot path).
    pool: BufferPool,

    // ── GSO send batching ─────────────────────────────────────────
    /// Batches same-destination packets for `UDP_SEGMENT` offload.
    gso_batcher: GsoBatcher,

    // ── Event queue ───────────────────────────────────────────────
    /// Events collected during `on_datagram` / `on_tick`, drained by caller.
    events: Vec<QuicEvent>,

    // ── SCID allocation ───────────────────────────────────────────
    /// Monotonic counter for generating unique 20-byte SCIDs.
    scid_counter: u64,

    // ── Stats ─────────────────────────────────────────────────────
    /// Number of active connections.
    pub active_count: usize,
    /// Total datagrams received.
    pub datagrams_recv: u64,
    /// Total datagrams sent.
    pub datagrams_sent: u64,
}

impl std::fmt::Debug for QuicEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicEngine")
            .field("reactor_id", &self.reactor_id)
            .field("active_count", &self.active_count)
            .field("datagrams_recv", &self.datagrams_recv)
            .field("datagrams_sent", &self.datagrams_sent)
            .finish_non_exhaustive()
    }
}

impl QuicEngine {
    /// Creates a new QUIC engine bound to `local_addr`.
    ///
    /// The `quiche_config` is reused for all connections accepted by this
    /// engine. Each engine should run on a single thread (thread-per-core).
    #[must_use]
    pub fn new(
        config: QuicConfig,
        quiche_config: quiche::Config,
        reactor_id: u32,
        local_addr: SocketAddr,
        pool: BufferPool,
    ) -> Self {
        Self {
            reactor_id,
            config,
            quiche_config,
            local_addr,
            connections: Vec::new(),
            free_slots: Vec::new(),
            dcid_to_idx: HashMap::new(),
            pkt_buf: vec![0u8; 65535],
            out_buf: vec![0u8; MAX_PKT_SIZE],
            readable_buf: Vec::with_capacity(16),
            closed_buf: Vec::with_capacity(16),
            stream_carryover: HashMap::new(),
            pool,
            gso_batcher: GsoBatcher::new(),
            events: Vec::new(),
            scid_counter: 1,
            active_count: 0,
            datagrams_recv: 0,
            datagrams_sent: 0,
        }
    }

    /// Returns the number of active connections.
    #[must_use]
    pub fn active_connections(&self) -> usize {
        self.active_count
    }

    /// Current outstanding bytes in the buffer pool.
    #[must_use]
    pub fn pool_bytes_outstanding(&self) -> u64 {
        self.pool.bytes_outstanding()
    }

    /// Current memory-pressure level of the buffer pool.
    #[must_use]
    pub fn pressure_level(&self) -> buffer_pool::PressureLevel {
        self.pool.pressure_level()
    }

    /// Drains and returns all pending [`QuicEvent`]s collected since the
    /// last call. The caller (typically the io-layer handler wrapper) is
    /// responsible for dispatching them through the session/stream pipeline.
    pub fn drain_events(&mut self) -> Vec<QuicEvent> {
        std::mem::take(&mut self.events)
    }

    /// Sends application data on a QUIC stream.
    ///
    /// `stream_id` is the QUIC stream id (not the logical frame stream id).
    /// Data is written into quiche's stream buffer; the actual QUIC packets
    /// are emitted on the next `on_tick` → `flush_sends`.
    ///
    /// Returns the number of bytes accepted. On
    /// [`QuicSendError::FlowBlocked`] the caller should retry later.
    pub fn send_stream(
        &mut self,
        conn_idx: usize,
        stream_id: u64,
        data: &[u8],
        fin: bool,
    ) -> Result<usize, QuicSendError> {
        let Some(Some(entry)) = self.connections.get_mut(conn_idx) else {
            return Err(QuicSendError::UnknownConnection);
        };
        match entry.conn.stream_send(stream_id, data, fin) {
            Ok(n) => Ok(n),
            Err(quiche::Error::Done) => Err(QuicSendError::FlowBlocked),
            Err(e) => Err(QuicSendError::Quiche(e)),
        }
    }

    /// Processes a received datagram: routes by DCID and feeds to quiche.
    fn process_datagram(&mut self, data: &[u8], peer: SocketAddr) {
        self.datagrams_recv += 1;

        let len = data.len().min(self.pkt_buf.len());
        self.pkt_buf[..len].copy_from_slice(&data[..len]);

        // Parse the QUIC header from pkt_buf.  `from_slice` only advances
        // a cursor and does NOT modify the underlying bytes, so pkt_buf
        // is safe to pass to `conn.recv()` afterwards.
        let (dcid_key, dcid_len, is_initial) = {
            let hdr =
                match quiche::Header::from_slice(&mut self.pkt_buf[..len], quiche::MAX_CONN_ID_LEN)
                {
                    Ok(h) => h,
                    Err(_) => return,
                };
            let mut key = [0u8; quiche::MAX_CONN_ID_LEN];
            key[..hdr.dcid.len()].copy_from_slice(&hdr.dcid);
            (key, hdr.dcid.len(), hdr.ty == quiche::Type::Initial)
        };

        // Route by DCID (zero-alloc: dcid_key is a stack array).
        if let Some(&idx) = self.dcid_to_idx.get(&dcid_key) {
            self.feed_existing(idx, len, peer);
        } else if is_initial && !self.is_shutting_down() {
            if let Some(idx) = self.accept_new(&dcid_key[..dcid_len], peer) {
                self.feed_existing(idx, len, peer);
            }
        }
        // Non-Initial from unknown DCID → drop (stateless reset or garbage).
    }

    /// Feeds a packet to an existing connection.
    fn feed_existing(&mut self, idx: usize, len: usize, peer: SocketAddr) {
        // Disjoint field borrows: pkt_buf, events, readable_buf, pool, and
        // connections are separate fields of Self, so the borrow checker
        // allows holding all mutable references simultaneously.
        let pkt_buf = &mut self.pkt_buf;
        let events = &mut self.events;
        let readable_buf = &mut self.readable_buf;
        let pool = &self.pool;
        let local_addr = self.local_addr;
        let reactor_id = self.reactor_id;
        let carryover = &mut self.stream_carryover;

        let Some(Some(entry)) = self.connections.get_mut(idx) else {
            return;
        };

        // Update peer address (connection migration).
        entry.peer_addr = peer;

        let info = quiche::RecvInfo {
            from: peer,
            to: local_addr,
        };

        match entry.conn.recv(&mut pkt_buf[..len], info) {
            Ok(_) => {
                // Emit ConnectionEstablished on handshake completion (once).
                if !entry.established && entry.conn.is_established() {
                    entry.established = true;
                    events.push(QuicEvent::ConnectionEstablished {
                        conn_idx: idx,
                        peer_addr: peer,
                    });
                    tracing::info!(
                        reactor_id,
                        idx,
                        peer = %peer,
                        "connection established"
                    );
                }

                // Process readable streams + datagrams, collecting frame events.
                crate::stream_bridge::process_readable(
                    &mut entry.conn,
                    reactor_id,
                    idx,
                    events,
                    readable_buf,
                    pool,
                    carryover,
                );
            }
            Err(quiche::Error::Done) => {}
            Err(e) => {
                tracing::warn!(
                    reactor_id,
                    idx,
                    error = ?e,
                    pkt_len = len,
                    "conn.recv error"
                );
            }
        }
    }

    /// Generates a unique 20-byte source connection id.
    ///
    /// Using `MAX_CONN_ID_LEN` (20) for every SCID ensures that 1-RTT
    /// short-header packets — whose DCID length is not encoded in the
    /// packet — can be routed via an O(1) hash lookup. Short-header
    /// parsing uses `dcid_len = MAX_CONN_ID_LEN`, so the extracted DCID
    /// always matches the stored key byte-for-byte.
    fn generate_scid(&mut self) -> [u8; quiche::MAX_CONN_ID_LEN] {
        let mut buf = [0u8; quiche::MAX_CONN_ID_LEN];
        // reactor_id (4 bytes) + counter (8 bytes) provides per-engine
        // uniqueness; remaining 8 bytes are zero (sufficient for Phase 1).
        buf[..4].copy_from_slice(&self.reactor_id.to_le_bytes());
        buf[4..12].copy_from_slice(&self.scid_counter.to_le_bytes());
        self.scid_counter = self.scid_counter.wrapping_add(1);
        buf
    }

    /// Accepts a new QUIC connection from an Initial packet.
    ///
    /// Returns the slab index of the new connection so the caller can
    /// immediately feed it the Initial packet.
    fn accept_new(&mut self, dcid: &[u8], peer: SocketAddr) -> Option<usize> {
        if self.active_count >= self.config.max_connections {
            return None; // At capacity — drop.
        }

        // Generate a unique 20-byte server source CID.  Using MAX_CONN_ID_LEN
        // guarantees that short-header (1-RTT) packets — whose DCID length
        // is not encoded in the packet — can be routed by an exact 20-byte
        // hash lookup against `from_slice(.., MAX_CONN_ID_LEN)`.
        let scid_bytes = self.generate_scid();
        let scid = quiche::ConnectionId::from_ref(&scid_bytes);

        // The client's Initial DCID becomes the original DCID (odcid).  With
        // `Some(odcid)`, quiche records it as the
        // `original_destination_connection_id` transport parameter.
        let odcid = quiche::ConnectionId::from_ref(dcid);

        let conn = match quiche::accept(
            &scid,
            Some(&odcid),
            self.local_addr,
            peer,
            &mut self.quiche_config,
        ) {
            Ok(c) => c,
            Err(_) => return None,
        };

        // Two routing keys:
        //   scid_key        — server's source CID (20 bytes, used by 1-RTT).
        //   initial_dcid_key — client's Initial DCID (for Initial retransmissions).
        let scid_key = pad_dcid(&conn.source_id());
        let initial_dcid_key = pad_dcid(&odcid);

        // Reuse a freed slot if available; otherwise grow the slab.
        let entry = ConnEntry {
            conn,
            peer_addr: peer,
            established: false,
            scid_key,
            initial_dcid_key,
        };
        let idx = match self.free_slots.pop() {
            Some(free) => {
                self.connections[free] = Some(entry);
                free
            }
            None => {
                let idx = self.connections.len();
                self.connections.push(Some(entry));
                idx
            }
        };
        self.dcid_to_idx.insert(scid_key, idx);
        self.dcid_to_idx.insert(initial_dcid_key, idx);
        self.active_count += 1;

        tracing::debug!(
            reactor_id = self.reactor_id,
            idx,
            peer = %peer,
            "accepted new QUIC connection"
        );

        Some(idx)
    }

    /// Flushes outgoing packets for all connections.
    pub fn flush_sends(&mut self, ctx: &mut TickContext) {
        let out_buf = &mut self.out_buf;
        let connections = &mut self.connections;

        for entry_opt in connections.iter_mut() {
            let Some(entry) = entry_opt else { continue };

            loop {
                match entry.conn.send(out_buf) {
                    Ok((len, info)) => {
                        self.datagrams_sent += 1;
                        let _ = ctx.sender.send_blocking(&out_buf[..len], info.to);
                    }
                    Err(quiche::Error::Done) => break,
                    Err(_) => break, // Non-fatal: try next connection.
                }
            }
        }
    }

    /// Removes closed connections from the slab and routing table,
    /// emitting `ConnectionClosed` events for each.
    fn cleanup_closed(&mut self) {
        self.closed_buf.clear();
        for (idx, entry_opt) in self.connections.iter_mut().enumerate() {
            if let Some(entry) = entry_opt {
                if entry.conn.is_closed() {
                    self.closed_buf.push(idx);
                }
            }
        }

        // Index-based iteration: `self.closed_buf[i]` copies the usize
        // out, so the shared borrow ends before we mutate other fields.
        let n = self.closed_buf.len();
        for i in 0..n {
            let idx = self.closed_buf[i];
            if let Some(entry) = self.connections[idx].take() {
                self.dcid_to_idx.remove(&entry.scid_key);
                self.dcid_to_idx.remove(&entry.initial_dcid_key);
                self.active_count = self.active_count.saturating_sub(1);
                self.free_slots.push(idx);

                // Capture close reason before the connection is dropped.
                let reason = entry
                    .conn
                    .peer_error()
                    .map(|err| format!("peer_error={err:?}"));

                self.events.push(QuicEvent::ConnectionClosed {
                    conn_idx: idx,
                    peer_addr: entry.peer_addr,
                    reason,
                });
            }
        }
    }

    /// Returns whether the engine is shutting down.
    fn is_shutting_down(&self) -> bool {
        self.config.shutting_down
    }

    /// Initiates graceful shutdown: marks the engine as shutting down
    /// and calls `conn.close()` on every active connection. The actual
    /// CONNECTION_CLOSE frames are sent on the next `flush_sends`.
    ///
    /// After this call, new Initial packets are rejected. Existing
    /// connections continue to receive packets until quiche marks them
    /// as `is_closed()`.
    pub fn begin_shutdown(&mut self) {
        self.config.shutting_down = true;
        let reason = b"server shutting down";
        for entry in self.connections.iter_mut().flatten() {
            let _ = entry.conn.close(true, 0x00, reason);
        }
        tracing::info!(
            reactor_id = self.reactor_id,
            active_count = self.active_count,
            "graceful shutdown initiated — closing all connections"
        );
    }

    /// Closes a single connection by `conn_idx`.
    ///
    /// Sends a QUIC APPLICATION_CLOSE frame on the next `flush_sends`.
    /// The connection is fully removed on the subsequent `cleanup_closed()`
    /// call inside `on_tick`, which then emits `ConnectionClosed`.
    ///
    /// Used by the memory-pressure eviction path to selectively free
    /// buffer pool capacity held by heavy consumers.
    pub fn close_connection(&mut self, conn_idx: usize, app_error: u64, reason: &[u8]) {
        if let Some(Some(entry)) = self.connections.get_mut(conn_idx) {
            let _ = entry.conn.close(true, app_error, reason);
            tracing::debug!(
                reactor_id = self.reactor_id,
                conn_idx,
                app_error,
                "connection close requested"
            );
        }
    }
}

impl IoHandler for QuicEngine {
    fn on_datagram(&mut self, data: &[u8], peer: SocketAddr) {
        self.process_datagram(data, peer);
    }

    fn on_tick(&mut self, ctx: &mut TickContext) {
        self.flush_sends(ctx);
        self.cleanup_closed();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pool() -> BufferPool {
        BufferPool::default()
    }

    #[test]
    fn engine_constructs() {
        let mut qconfig = QuicConfig::default();
        qconfig.max_connections = 256;
        // Build a minimal quiche config without cert/key (test only).
        let mut qconfig_quiche = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
        qconfig_quiche.set_application_protos(&[b"h3-29"]).unwrap();

        let engine = QuicEngine::new(
            qconfig,
            qconfig_quiche,
            0,
            "127.0.0.1:0".parse().unwrap(),
            test_pool(),
        );

        assert_eq!(engine.reactor_id, 0);
        assert_eq!(engine.active_count, 0);
        assert_eq!(engine.datagrams_recv, 0);
    }

    #[test]
    fn on_datagram_drops_malformed() {
        let qconfig = QuicConfig::default();
        let mut qconfig_quiche = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
        qconfig_quiche.set_application_protos(&[b"h3-29"]).unwrap();

        let mut engine = QuicEngine::new(
            qconfig,
            qconfig_quiche,
            0,
            "127.0.0.1:0".parse().unwrap(),
            test_pool(),
        );

        // Feed garbage data — should not panic, should not create a connection.
        engine.on_datagram(&[0xFF; 10], "127.0.0.1:9999".parse().unwrap());
        assert_eq!(engine.active_count, 0);
        assert_eq!(engine.datagrams_recv, 1);
    }

    #[test]
    fn on_tick_with_no_connections_is_noop() {
        let qconfig = QuicConfig::default();
        let mut qconfig_quiche = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
        qconfig_quiche.set_application_protos(&[b"h3-29"]).unwrap();

        let mut engine = QuicEngine::new(
            qconfig,
            qconfig_quiche,
            0,
            "127.0.0.1:0".parse().unwrap(),
            test_pool(),
        );

        // on_tick should not panic with zero connections.
        // We can't easily construct a TickContext in tests, so just verify
        // cleanup_closed works.
        engine.cleanup_closed();
        assert_eq!(engine.active_count, 0);
    }
}

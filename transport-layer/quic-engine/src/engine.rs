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
//! │    1. quiche::Header::from_slice(data) → extract DCID            │
//! │    2. Route by DCID:                                             │
//! │       a. Found  → conn.recv(data, RecvInfo)  [zero-copy]        │
//! │       b. Initial → quiche::accept() → insert into table          │
//! │    3. Process readable streams                                   │
//! │                                                                  │
//! │  on_tick(ctx):                                                   │
//! │    For each dirty connection:                                    │
//! │      conn.send(out_buf) → (len, SendInfo) → gso_batcher.push()   │
//! │    gso_batcher.flush() → sendmsg(UDP_SEGMENT)                    │
//! │    Clean up closed connections.                                  │
//! └──────────────────────────────────────────────────────────────────┘
//! ```

use bytes::Bytes;
use rustc_hash::FxHashMap;

use crate::hash::CrcHashMap;
use std::net::SocketAddr;

use buffer_pool::BufferPool;
use delivery_policy::{DeliveryPolicyTable, FramePayload};
use frame::codec;
use frame::header::{Seq, StreamId};
use io::handler::{IoHandler, TickContext};

use crate::config::QuicConfig;
use crate::dirty::{Control, DirtyBitset};
use crate::gso::GsoBatcher;
use crate::stream_bridge::StreamCarry;

/// Maximum QUIC packet size — covers full IPv4 UDP jumbo payload
/// (65535 - IP/UDP overhead). Must match the io-layer receive buffer
/// and the client's `max_send_udp_payload_size`.
const MAX_PKT_SIZE: usize = 65535;

/// Per-tick cap on the number of dirty connections processed by
/// [`QuicEngine::flush_sends`].
///
/// A previous implementation removed this cap entirely ("process ALL
/// dirty connections per tick"). Under steady-state this is optimal,
/// but during connection storms (many simultaneous acks / flow-control
/// updates / new handshakes) one tick can attempt to flush hundreds
/// of connections, blowing the io_uring SQ budget and starving the
/// recv path of CPU. The cap below is intentionally large (8× the
/// prior 64-conn cap that hurt throughput) — it only kicks in under
/// storm conditions, leaving steady-state behavior unchanged.
const DIRTY_FLUSH_BUDGET: usize = 512;

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
        /// Refcounted payload bytes. The engine allocates from the buffer
        /// pool on the receive path, then converts to `Bytes` once via
        /// `Buffer::into_bytes` so the bytes can outlive the pool guard
        /// (e.g. when buffered in a delivery-policy reorder window).
        payload: Bytes,
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
/// Used as a `FxHashMap` key to avoid per-datagram heap allocation (`to_vec`).
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
    dcid_to_idx: CrcHashMap<[u8; quiche::MAX_CONN_ID_LEN], usize>,

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
    /// Reusable scratch recording the connections flushed this tick, so
    /// `cleanup_closed` can scan only active connections instead of the
    /// whole slab. Cleared at the start of each `flush_sends`.
    flushed_this_tick: Vec<usize>,
    /// Connections with pending sends — avoids O(N) flush scan of idle
    /// connections every tick. Populated by `feed_existing` (ACKs, flow
    /// control after recv), `send_stream` (application data), and
    /// `close_connection` / `begin_shutdown` (CONNECTION_CLOSE).
    ///
    /// Packed bitset: setting a dirty bit is O(1) and idempotent, and the
    /// flush loop clears each bit in place as it is processed (up to
    /// [`DIRTY_FLUSH_BUDGET`]). Unprocessed bits roll forward to the next
    /// tick — no sort or dedup pass is needed.
    dirty: DirtyBitset,
    /// Per-stream reassembly buffer for frames spanning multiple
    /// `stream_recv` calls. Keyed by (conn_idx, QUIC stream ID) to
    /// prevent cross-connection data mixing — without the conn_idx
    /// disambiguator, two connections sending on stream 0 would
    /// share the same carryover entry, producing decode garbage.
    stream_carryover: FxHashMap<(usize, u64), StreamCarry>,

    // ── Delivery policy ──────────────────────────────────────────
    /// Per-stream receive-side delivery semantics (ReliableOrdered /
    /// ReliableUnordered / RealtimeDropOld / LatestOnly), keyed by
    /// `(conn_idx, header.stream_id)`. Populated by `StreamOpen` frames
    /// and applied to every data frame in `stream_bridge`.
    delivery_policies: DeliveryPolicyTable,

    /// Reusable scratch for `DeliveryPolicyTable::drain_ready_for_connection`
    /// output. Avoids per-tick Vec allocation.
    policy_drain_buf: Vec<(StreamId, Seq, FramePayload)>,

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
    /// Total `stream_send` calls that returned `Error::Done` (per-stream
    /// outbound buffer full / flow-control exhausted). Each one is a
    /// dropped echo frame in the current code path — see stack.rs
    /// `encode_and_send`. Used to quantify the flow-control deadlock.
    pub stream_send_blocked: u64,
    /// Total bytes accepted into quiche's stream buffer (does NOT
    /// include dropped frames). Sampled alongside `stream_send_blocked`
    /// to compute the drop ratio.
    pub stream_send_bytes: u64,

    // ── Inter-worker forwarding ───────────────────────────────────
    /// Optional forwarding mesh for migration/misroute safety net.
    /// `None` when there is only one worker (single-worker mode) or
    /// when the builder did not configure forwarding.
    pub forward_mesh: Option<crate::forward::ForwardMesh>,
    /// Total worker count (for bounds-checking target reactor_id).
    pub(crate) num_workers: u32,
    /// Scratch buffer for draining forwarded packets (avoids per-tick
    /// allocation). Cleared at the start of each `on_tick` drain.
    forwarded_buf: Vec<crate::forward::ForwardedPacket>,
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
            dcid_to_idx: CrcHashMap::default(),
            pkt_buf: {
                // SAFETY: u8 has no destructor. The buffer is always written
                // via copy_from_slice before any read (see process_datagram).
                let mut v = Vec::<u8>::with_capacity(65535);
                unsafe { v.set_len(65535) };
                v
            },
            out_buf: {
                // SAFETY: u8 has no destructor. quiche::Connection::send
                // writes into this buffer before the caller reads [..len].
                let mut v = Vec::<u8>::with_capacity(MAX_PKT_SIZE);
                unsafe { v.set_len(MAX_PKT_SIZE) };
                v
            },
            readable_buf: Vec::with_capacity(16),
            closed_buf: Vec::with_capacity(16),
            flushed_this_tick: Vec::with_capacity(64),
            dirty: DirtyBitset::new(),
            stream_carryover: FxHashMap::default(),
            delivery_policies: DeliveryPolicyTable::new(),
            policy_drain_buf: Vec::new(),
            pool,
            gso_batcher: GsoBatcher::new(),
            events: Vec::new(),
            scid_counter: 1,
            active_count: 0,
            datagrams_recv: 0,
            datagrams_sent: 0,
            stream_send_blocked: 0,
            stream_send_bytes: 0,
            forward_mesh: None,
            num_workers: 1,
            forwarded_buf: Vec::new(),
        }
    }

    /// Returns the number of active connections.
    #[must_use]
    pub fn active_connections(&self) -> usize {
        self.active_count
    }

    /// Returns a reference to the forward mesh, if configured.
    #[must_use]
    pub fn forward_mesh(&self) -> Option<&crate::forward::ForwardMesh> {
        self.forward_mesh.as_ref()
    }

    /// Returns a mutable reference to the forward mesh, if configured.
    pub fn forward_mesh_mut(&mut self) -> Option<&mut crate::forward::ForwardMesh> {
        self.forward_mesh.as_mut()
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
            Ok(n) => {
                // Mark dirty so flush_sends picks this connection up.
                self.dirty.set(conn_idx);
                self.stream_send_bytes += n as u64;
                Ok(n)
            }
            Err(quiche::Error::Done) => {
                self.stream_send_blocked += 1;
                Err(QuicSendError::FlowBlocked)
            }
            Err(e) => Err(QuicSendError::Quiche(e)),
        }
    }

    /// Processes a received datagram: routes by DCID and feeds to quiche.
    fn process_datagram(&mut self, data: &mut [u8], peer: SocketAddr) {
        self.datagrams_recv += 1;

        let len = data.len();

        // Parse the QUIC header directly from the provided buffer — no
        // intermediate copy needed. `from_slice` only advances a cursor
        // and does NOT modify the underlying bytes, so `data` is safe to
        // pass to `conn.recv()` afterwards.
        let (dcid_key, dcid_len, is_initial) = {
            let hdr = match quiche::Header::from_slice(&mut data[..len], quiche::MAX_CONN_ID_LEN) {
                Ok(h) => h,
                Err(_) => return,
            };
            let mut key = [0u8; quiche::MAX_CONN_ID_LEN];
            key[..hdr.dcid.len()].copy_from_slice(&hdr.dcid);
            (key, hdr.dcid.len(), hdr.ty == quiche::Type::Initial)
        };

        // Route by DCID (zero-alloc: dcid_key is a stack array).
        if let Some(&idx) = self.dcid_to_idx.get(&dcid_key) {
            self.feed_existing(idx, data, len, peer);
        } else if is_initial && !self.is_shutting_down() {
            if let Some(idx) = self.accept_new(&dcid_key[..dcid_len], peer) {
                self.feed_existing(idx, data, len, peer);
            }
        } else if !is_initial && dcid_len >= 4 {
            // Non-Initial from unknown DCID → check if the DCID encodes a
            // different reactor_id (server SCID has reactor_id in bytes
            // 0-3). If so, forward to the owning worker. This handles
            // connection migration: when a client's 4-tuple changes, the
            // kernel hash routes packets to the wrong worker. Without
            // forwarding, these packets are silently dropped.
            if let Some(mesh) = &mut self.forward_mesh {
                let target =
                    u32::from_le_bytes([dcid_key[0], dcid_key[1], dcid_key[2], dcid_key[3]]);
                if target != self.reactor_id && target < self.num_workers {
                    mesh.try_forward(target, &data[..len], peer);
                    return;
                }
            }
        }
        // Non-Initial from unknown DCID with no matching reactor → drop
        // (stateless reset, garbage, or connection already closed).
    }

    /// Feeds a packet to an existing connection.
    fn feed_existing(&mut self, idx: usize, data: &mut [u8], len: usize, peer: SocketAddr) {
        // Disjoint field borrows: events, readable_buf, pool, and
        // connections are separate fields of Self, so the borrow checker
        // allows holding all mutable references simultaneously.
        // `data` is passed in directly — no pkt_buf copy needed.
        let events = &mut self.events;
        let readable_buf = &mut self.readable_buf;
        let pool = &self.pool;
        let local_addr = self.local_addr;
        let reactor_id = self.reactor_id;
        let carryover = &mut self.stream_carryover;
        let dirty = &mut self.dirty;

        let Some(Some(entry)) = self.connections.get_mut(idx) else {
            return;
        };

        // Update peer address (connection migration).
        entry.peer_addr = peer;

        let info = quiche::RecvInfo {
            from: peer,
            to: local_addr,
        };

        match entry.conn.recv(&mut data[..len], info) {
            Ok(_) => {
                // Mark dirty — recv may have generated ACKs, flow-control
                // updates, or probe packets that need to be flushed.
                dirty.set(idx);

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
                    &mut self.delivery_policies,
                    &mut self.policy_drain_buf,
                    self.config.skip_crc,
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

    /// Flushes outgoing packets for connections that have pending sends.
    ///
    /// Only iterates the dirty bitset instead of all connections — a
    /// connection enters the dirty set when it receives a datagram (ACKs /
    /// flow-control updates) or when application data is queued via
    /// [`send_stream`](Self::send_stream).
    ///
    /// At most [`DIRTY_FLUSH_BUDGET`] connections are processed per tick;
    /// any remaining dirty bits stay set and roll forward, bounding
    /// worst-case tick latency under connection storms. Each processed
    /// connection's bit is cleared in place; if `conn.send()` or a
    /// concurrent recv re-marks it dirty during the flush, that new bit
    /// survives and is revisited next tick.
    pub fn flush_sends(&mut self, ctx: &mut TickContext) {
        // Record which connections we flush this tick so cleanup_closed can
        // scan only the active set (the bitset is cleared as we drain).
        self.flushed_this_tick.clear();

        if self.dirty.is_empty() {
            return;
        }

        let connections = &mut self.connections;
        let out_buf = &mut self.out_buf;
        let gso = &mut self.gso_batcher;
        let sender = &ctx.sender;
        let flushed = &mut self.flushed_this_tick;

        // Process dirty connections up to `DIRTY_FLUSH_BUDGET` per tick.
        // The budget is sized for storm safety (handshake bursts, ack
        // cascades) — under steady-state each reactor has ≪ budget
        // dirty connections, so the cap is not the bottleneck. Any
        // connection that loses its turn this tick keeps its dirty bit
        // set and is processed first on the next tick.
        let mut budget = DIRTY_FLUSH_BUDGET;
        self.dirty.drain(|idx| {
            if budget == 0 {
                // Stop draining — leave remaining bits set for next tick.
                return Control::Break;
            }
            budget -= 1;
            flushed.push(idx);
            if let Some(Some(entry)) = connections.get_mut(idx) {
                loop {
                    match entry.conn.send(out_buf) {
                        Ok((len, info)) => {
                            self.datagrams_sent += 1;
                            let _ = gso.push(&out_buf[..len], info.to, sender);
                        }
                        Err(quiche::Error::Done) => break,
                        Err(_) => break,
                    }
                }
            }
            Control::Continue
        });
        let _ = gso.flush(sender);
    }

    /// Removes closed connections from the slab and routing table,
    /// emitting `ConnectionClosed` events for each.
    ///
    /// Only checks connections flushed this tick (recorded in
    /// [`flushed_this_tick`](Self.flushed_this_tick)) — connections that
    /// were active (received datagrams, had sends queued, or were
    /// explicitly closed). Idle connections that closed without traffic
    /// are handled by the higher-level idle-timeout reaper in
    /// `VireonStack::reap_idle`. Connections that exceeded the flush
    /// budget and kept their dirty bit set are checked on the tick they
    /// are actually flushed.
    fn cleanup_closed(&mut self) {
        self.closed_buf.clear();

        // Check only connections flushed this tick (not all slots).
        let n = self.flushed_this_tick.len();
        for i in 0..n {
            let idx = self.flushed_this_tick[i];
            if let Some(Some(entry)) = self.connections.get(idx) {
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
                // Drop all per-stream delivery-policy state for this conn
                // so the slab indices can be reused without leaking policy
                // entries that would otherwise never be cleaned up.
                self.delivery_policies.remove_connection(idx);

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
        // Mark all active connections dirty so CONNECTION_CLOSE frames
        // are flushed by the next `flush_sends`.
        self.dirty.clear();
        for (idx, entry) in self.connections.iter_mut().enumerate() {
            if let Some(e) = entry {
                let _ = e.conn.close(true, 0x00, reason);
                self.dirty.set(idx);
            }
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
            self.dirty.set(conn_idx);
            // Drop delivery-policy state immediately on explicit close —
            // the connection slab slot will be reclaimed by `cleanup_closed`
            // later, but cleaning up here bounds memory if the slab is
            // under pressure.
            self.delivery_policies.remove_connection(conn_idx);
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
    fn on_datagram(&mut self, data: &mut [u8], peer: SocketAddr) {
        self.process_datagram(data, peer);
    }

    fn on_tick(&mut self, ctx: &mut TickContext) {
        // Drain forwarded packets first — they may generate ACKs or
        // application data that flush_sends should include in this tick.
        if self.forward_mesh.is_some() {
            self.forwarded_buf.clear();
            if let Some(mesh) = &mut self.forward_mesh {
                mesh.drain_into(&mut self.forwarded_buf);
            }
            // Process each forwarded packet. Borrow checker: we borrow
            // forwarded_buf mutably to drain it, then borrow self mutably
            // for process_datagram — disjoint because forwarded_buf is
            // emptied before process_datagraph runs.
            while let Some(pkt) = self.forwarded_buf.pop() {
                let len = pkt.data.len();
                // SAFETY: we own the Box; the slice is valid for the call.
                let mut data = pkt.data;
                self.process_datagram(&mut data[..len], pkt.peer);
            }
        }

        self.flush_sends(ctx);
        self.cleanup_closed();
        // No blanket clear: flush_sends already cleared every bit it
        // processed. Bits for connections that exceeded the flush budget
        // stay set and roll forward to the next tick.
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
        engine.on_datagram(&mut [0xFF; 10], "127.0.0.1:9999".parse().unwrap());
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

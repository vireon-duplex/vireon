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

use frame::codec;
use io::handler::{IoHandler, TickContext};

use crate::config::QuicConfig;

/// Maximum QUIC packet size (typical MTU 1500 - IP header 20 - UDP header 8).
const MAX_PKT_SIZE: usize = 1472;

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
        /// The fully decoded, owned frame.
        frame: codec::Frame,
    },
    /// A stream signaled FIN (graceful close).
    StreamFinished {
        /// Slab index of the connection.
        conn_idx: usize,
        /// QUIC stream id.
        stream_id: u64,
    },
}

/// One entry in the connection slab.
struct ConnEntry {
    conn: quiche::Connection,
    peer_addr: SocketAddr,
    /// Tracks whether we've already emitted `ConnectionEstablished`.
    established: bool,
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
    /// DCID → slab index routing table.
    dcid_to_idx: HashMap<Vec<u8>, usize>,

    // ── Reusable buffers ──────────────────────────────────────────
    /// Buffer for incoming packet header parsing + recv.
    pkt_buf: Vec<u8>,
    /// Buffer for outgoing packet generation.
    out_buf: Vec<u8>,

    // ── Event queue ───────────────────────────────────────────────
    /// Events collected during `on_datagram` / `on_tick`, drained by caller.
    events: Vec<QuicEvent>,

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
    ) -> Self {
        Self {
            reactor_id,
            config,
            quiche_config,
            local_addr,
            connections: Vec::new(),
            dcid_to_idx: HashMap::new(),
            pkt_buf: vec![0u8; 65535],
            out_buf: vec![0u8; MAX_PKT_SIZE],
            events: Vec::new(),
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

    /// Drains and returns all pending [`QuicEvent`]s collected since the
    /// last call. The caller (typically the io-layer handler wrapper) is
    /// responsible for dispatching them through the session/stream pipeline.
    pub fn drain_events(&mut self) -> Vec<QuicEvent> {
        std::mem::take(&mut self.events)
    }

    /// Processes a received datagram: routes by DCID and feeds to quiche.
    fn process_datagram(&mut self, data: &[u8], peer: SocketAddr) {
        self.datagrams_recv += 1;

        let len = data.len().min(self.pkt_buf.len());
        self.pkt_buf[..len].copy_from_slice(&data[..len]);

        // Parse QUIC header — scope ensures the header's borrow of pkt_buf
        // ends before we reuse pkt_buf for recv().
        let (dcid, is_initial) = {
            let hdr =
                match quiche::Header::from_slice(&mut self.pkt_buf[..len], quiche::MAX_CONN_ID_LEN)
                {
                    Ok(h) => h,
                    Err(_) => return, // Malformed — drop silently (RFC 9000 §17).
                };
            (hdr.dcid.to_vec(), hdr.ty == quiche::Type::Initial)
        };

        // Route by DCID.
        if let Some(&idx) = self.dcid_to_idx.get(&dcid) {
            self.feed_existing(idx, len, peer);
        } else if is_initial {
            self.accept_new(&dcid, peer);
        }
        // Non-Initial from unknown DCID → drop (stateless reset or garbage).
    }

    /// Feeds a packet to an existing connection.
    fn feed_existing(&mut self, idx: usize, len: usize, peer: SocketAddr) {
        // Disjoint field borrows: pkt_buf, events, and connections are
        // separate fields of Self, so the borrow checker allows holding
        // all three mutable references simultaneously.
        let pkt_buf = &mut self.pkt_buf;
        let events = &mut self.events;
        let local_addr = self.local_addr;
        let reactor_id = self.reactor_id;

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
                crate::stream_bridge::process_readable(&mut entry.conn, reactor_id, idx, events);
            }
            Err(quiche::Error::Done) => {}
            Err(_) => {
                // Connection-level error — mark for cleanup.
            }
        }
    }

    /// Accepts a new QUIC connection from an Initial packet.
    fn accept_new(&mut self, dcid: &[u8], peer: SocketAddr) {
        if self.active_count >= self.config.max_connections {
            return; // At capacity — drop.
        }

        let scid = quiche::ConnectionId::from_ref(dcid);

        let conn = match quiche::accept(
            &scid,
            Some(&scid),
            self.local_addr,
            peer,
            &mut self.quiche_config,
        ) {
            Ok(c) => c,
            Err(_) => return,
        };

        // Insert into routing table using the server's source ID.
        let key: Vec<u8> = conn.source_id().to_vec();
        let idx = self.connections.len();
        self.connections.push(Some(ConnEntry {
            conn,
            peer_addr: peer,
            established: false,
        }));
        self.dcid_to_idx.insert(key, idx);
        self.active_count += 1;

        tracing::debug!(
            reactor_id = self.reactor_id,
            idx,
            peer = %peer,
            "accepted new QUIC connection"
        );
    }

    /// Flushes outgoing packets for all connections.
    fn flush_sends(&mut self, ctx: &mut TickContext) {
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
        let mut to_remove = Vec::new();
        for (idx, entry_opt) in self.connections.iter_mut().enumerate() {
            if let Some(entry) = entry_opt {
                if entry.conn.is_closed() {
                    to_remove.push(idx);
                }
            }
        }

        for idx in to_remove {
            if let Some(entry) = self.connections[idx].take() {
                let key: Vec<u8> = entry.conn.source_id().to_vec();
                self.dcid_to_idx.remove(&key);
                self.active_count = self.active_count.saturating_sub(1);

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
}

impl IoHandler for QuicEngine {
    fn on_datagram(&mut self, data: &[u8], peer: SocketAddr) {
        if self.is_shutting_down() {
            return;
        }
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

    #[test]
    fn engine_constructs() {
        let mut qconfig = QuicConfig::default();
        qconfig.max_connections = 256;
        // Build a minimal quiche config without cert/key (test only).
        let mut qconfig_quiche = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
        qconfig_quiche.set_application_protos(&[b"h3-29"]).unwrap();

        let engine = QuicEngine::new(qconfig, qconfig_quiche, 0, "127.0.0.1:0".parse().unwrap());

        assert_eq!(engine.reactor_id, 0);
        assert_eq!(engine.active_count, 0);
        assert_eq!(engine.datagrams_recv, 0);
    }

    #[test]
    fn on_datagram_drops_malformed() {
        let qconfig = QuicConfig::default();
        let mut qconfig_quiche = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
        qconfig_quiche.set_application_protos(&[b"h3-29"]).unwrap();

        let mut engine =
            QuicEngine::new(qconfig, qconfig_quiche, 0, "127.0.0.1:0".parse().unwrap());

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

        let mut engine =
            QuicEngine::new(qconfig, qconfig_quiche, 0, "127.0.0.1:0".parse().unwrap());

        // on_tick should not panic with zero connections.
        // We can't easily construct a TickContext in tests, so just verify
        // cleanup_closed works.
        engine.cleanup_closed();
        assert_eq!(engine.active_count, 0);
    }
}

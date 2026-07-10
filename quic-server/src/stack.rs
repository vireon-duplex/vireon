//! [`VireonStack`] — the full 8-layer composition root.
//!
//! Composes the upper half (Application → Session → Stream → Message)
//! with the lower half (Runtime + Transport engine), bridging them
//! via the io-layer `IoHandler` trait.
//!
//! ```text
//!                     VireonStack
//!                         │
//!     ┌───────────────────┼───────────────────┐
//!     │                   │                   │
//!     ▼                   ▼                   ▼
//!  ApplicationLayer   RuntimeLayer     QuicEngine
//!  (owns Session →    (Shard, Timer,   (transport)
//!   Stream →           BufferPool)
//!   Message)
//!     │                                       │
//!     │              ConnectionRegistry        │
//!     └──────── cross-layer conn state ────────┘
//! ```
//!
//! The reactor (io-layer) drives the event loop and calls
//! `IoHandler::on_datagram` / `IoHandler::on_tick` on this struct.

use std::net::SocketAddr;

use buffer_pool::Buffer;
use frame::codec::{FrameHeader, FrameRef};
use io::handler::{IoHandler, TickContext};
use quic_engine::{QuicEngine, QuicEvent};

use crate::connection::{ConnState, ConnectionRegistry};
use crate::error::ServerError;
use crate::layers::{ApplicationLayer, RuntimeLayer};

/// The full multi-layer stack — owns the application chain, the
/// runtime infrastructure, the QUIC engine, and the cross-layer
/// connection registry.
///
/// Not `Send` — `QuicEngine` holds raw pointers and `ConnectionShard`
/// uses a non-`Send` strategy box. Must stay on the reactor thread.
pub struct VireonStack {
    // ── Upper half (logical protocol) ──────────────────────────
    /// Application layer — owns Session → Stream → Message.
    application: ApplicationLayer,

    // ── Lower half (physical I/O) ──────────────────────────────
    /// Runtime layer — ConnectionShard + TimerWheel + BufferPool.
    runtime: RuntimeLayer,
    /// Transport layer — QUIC engine (decodes datagrams, emits events).
    engine: QuicEngine,
    /// Cross-layer connection registry (conn_idx → conn_id + session_id).
    connections: ConnectionRegistry,

    // ── Reusable scratch ────────────────────────────────────────
    /// Pre-allocated scratch for `reap_idle` (avoids per-tick Vec alloc).
    reap_buf: Vec<(usize, SocketAddr)>,
}

impl VireonStack {
    /// Creates a new stack, constructing all layer components.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] if the runtime layer (ConnectionShard,
    /// TimerWheel) cannot be constructed.
    pub fn new(engine: QuicEngine, reactor_id: u32) -> Result<Self, ServerError> {
        Ok(Self {
            application: ApplicationLayer::new(),
            runtime: RuntimeLayer::new(reactor_id)?,
            engine,
            connections: ConnectionRegistry::new(),
            reap_buf: Vec::with_capacity(16),
        })
    }

    // ── Event processing ──────────────────────────────────────────

    /// Processes a batch of [`QuicEvent`]s through the layer stack.
    pub fn process_events(&mut self, events: Vec<QuicEvent>) {
        for event in events {
            match event {
                QuicEvent::ConnectionEstablished {
                    conn_idx,
                    peer_addr,
                } => {
                    self.on_connection_established(conn_idx, peer_addr);
                }
                QuicEvent::ConnectionClosed {
                    conn_idx,
                    peer_addr,
                    reason,
                } => {
                    self.on_connection_closed(conn_idx, peer_addr, reason.as_deref());
                }
                QuicEvent::FrameReceived {
                    conn_idx,
                    header,
                    payload,
                } => {
                    self.on_frame_received(conn_idx, header, payload);
                }
                QuicEvent::StreamFinished {
                    conn_idx,
                    stream_id,
                } => {
                    self.on_stream_finished(conn_idx, stream_id);
                }
            }
        }
    }

    // ── Connection lifecycle ──────────────────────────────────────

    fn on_connection_established(&mut self, conn_idx: usize, peer: SocketAddr) {
        // [runtime-layer] Create connection + start idle timer.
        let Some(conn_id) = self.runtime.on_connect(conn_idx, peer) else {
            return;
        };

        // [application → session → stream] Top-down lifecycle.
        let Some(session_id) = self.application.on_connect(conn_idx, peer) else {
            // Session table full — rollback runtime connection.
            self.runtime.on_close(conn_idx, conn_id);
            return;
        };

        tracing::info!(
            conn_idx,
            conn_id = conn_id.raw(),
            session_id = ?session_id,
            shard_occupancy = self.runtime.active_count(),
            session_occupancy = self.application.session_occupancy(),
            %peer,
            "session created"
        );

        // Track cross-layer state.
        self.connections.insert(
            conn_idx,
            ConnState {
                conn_id,
                session_id,
                peer_addr: peer,
            },
        );
    }

    fn on_connection_closed(
        &mut self,
        conn_idx: usize,
        peer_addr: SocketAddr,
        reason: Option<&str>,
    ) {
        let Some(cs) = self.connections.remove(conn_idx) else {
            return;
        };

        // [application → session → stream] Top-down teardown.
        self.application.on_close(conn_idx, cs.session_id, reason);

        // [runtime-layer] Cancel timer + destroy connection.
        self.runtime.on_close(conn_idx, cs.conn_id);

        tracing::info!(
            conn_idx,
            conn_id = cs.conn_id.raw(),
            session_id = ?cs.session_id,
            %peer_addr,
            reason = reason.unwrap_or("none"),
            "session destroyed"
        );
    }

    // ── Frame dispatch ────────────────────────────────────────────

    fn on_frame_received(&mut self, conn_idx: usize, header: FrameHeader, payload: Buffer) {
        // Look up cross-layer connection state (ConnState is Copy).
        let Some(cs) = self.connections.get(conn_idx) else {
            tracing::warn!(conn_idx, "frame for unknown session");
            return;
        };

        // [runtime-layer] Update stats + reset timer.
        self.runtime.on_frame(conn_idx, cs.conn_id, payload.len());

        // Construct a zero-copy FrameRef borrowing from the pool buffer.
        // `payload` (Buffer) lives on the stack for the duration of the
        // layer-chain call, then drops → recycled to pool automatically.
        let frame_ref = FrameRef {
            stream_id: header.stream_id,
            seq: header.seq,
            msg_type: header.msg_type,
            flags: header.flags,
            payload: &payload[..],
        };

        // [application → session → stream → message] Top-down processing.
        self.application
            .on_frame(conn_idx, cs.session_id, &frame_ref);
    }

    fn on_stream_finished(&mut self, conn_idx: usize, stream_id: u64) {
        tracing::debug!(conn_idx, stream_id, "QUIC stream FIN");
    }

    // ── Idle-timeout reaping ──────────────────────────────────────

    /// Drains expired idle timers and closes timed-out connections.
    fn reap_idle(&mut self) {
        // Reuse pre-allocated scratch buffer.
        self.reap_buf.clear();

        let expired = self.runtime.drain_expired();
        for conn_idx in expired {
            if let Some(cs) = self.connections.get(conn_idx) {
                self.reap_buf.push((conn_idx, cs.peer_addr));
            }
        }

        if !self.reap_buf.is_empty() {
            tracing::debug!(
                idle_closed = self.reap_buf.len(),
                conn_idxs = ?self.reap_buf.iter().map(|(c, _)| *c).collect::<Vec<_>>(),
                "idle-timeout connections reaped"
            );
        }

        // Index-based iteration: `reap_buf[i]` copies the tuple out
        // (both usize and SocketAddr are Copy), so the shared borrow ends
        // before we call `on_connection_closed` which takes &mut self.
        let n = self.reap_buf.len();
        for i in 0..n {
            let (conn_idx, peer_addr) = self.reap_buf[i];
            self.on_connection_closed(conn_idx, peer_addr, Some("idle timeout"));
        }
    }

    // ── Introspection ─────────────────────────────────────────────

    /// Returns the number of active sessions (connections tracked).
    #[must_use]
    #[allow(dead_code)]
    pub fn session_count(&self) -> usize {
        self.connections.len()
    }

    /// Returns the runtime-layer shard's active connection count.
    #[must_use]
    #[allow(dead_code)]
    pub fn shard_active(&self) -> usize {
        self.runtime.active_count()
    }
}

// ── IoHandler implementation ───────────────────────────────────────

impl IoHandler for VireonStack {
    fn on_datagram(&mut self, data: &[u8], peer: SocketAddr) {
        // [transport-layer] Feed raw datagram to QUIC engine.
        self.engine.on_datagram(data, peer);
    }

    fn on_tick(&mut self, ctx: &mut TickContext) {
        // [transport-layer] Flush outgoing packets + cleanup.
        self.engine.on_tick(ctx);

        // [transport-layer] Drain structured events.
        let events = self.engine.drain_events();

        // Process events through the full layer stack.
        if !events.is_empty() {
            self.process_events(events);
        }

        // [runtime-layer] Reap idle-timed-out connections.
        self.reap_idle();
    }
}

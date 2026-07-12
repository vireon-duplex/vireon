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
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use buffer_pool::Buffer;
use frame::codec::{self, FrameHeader, FrameRef};
use io::handler::{IoHandler, TickContext};
use quic_engine::{QuicEngine, QuicEvent};

use crate::connection::{ConnState, ConnectionRegistry};
use crate::error::ServerError;
use crate::layers::{ApplicationLayer, FrameContext, OutgoingFrame, RuntimeLayer};

/// Drain timeout — how long to wait for connections to close cleanly
/// before forcing reactor exit. 2 seconds covers 3 × PTO (Probe Timeout)
/// for typical RTTs and retransmitted CONNECTION_CLOSE.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

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
    /// Runtime layer — ConnectionShard + TimerWheel + BufferPool +
    /// SendQueue + TaskQueue + CqeProcessor + Reactor facade.
    runtime: RuntimeLayer,
    /// Transport layer — QUIC engine (decodes datagrams, emits events).
    engine: QuicEngine,
    /// Cross-layer connection registry (conn_idx → conn_id + session_id).
    connections: ConnectionRegistry,

    // ── Reusable scratch ────────────────────────────────────────
    /// Pre-allocated scratch for `reap_idle` (avoids per-tick Vec alloc).
    reap_buf: Vec<(usize, SocketAddr)>,
    /// Pre-allocated scratch for outgoing frame encoding (avoids per-send alloc).
    send_scratch: Vec<u8>,

    // ── Observability ────────────────────────────────────────────
    /// Tick counter — used to throttle periodic stats logging.
    tick_count: u64,
    /// Throttle for memory-pressure warnings (logs at most every 5 s).
    pressure_throttle: PressureThrottle,

    // ── Memory-pressure eviction ────────────────────────────────
    /// Last time an eviction pass ran — used to enforce cooldown.
    eviction_cooldown: Option<Instant>,
    /// Eviction thresholds and fractions.
    eviction_config: crate::eviction::EvictionConfig,

    // ── Benchmark mode ───────────────────────────────────────────
    /// When true, echo every received frame back on the same stream.
    echo: bool,

    // ── Graceful shutdown ────────────────────────────────────────
    /// Set by the signal handler to request graceful shutdown.
    shutdown_requested: Arc<std::sync::atomic::AtomicBool>,
    /// Reactor stop handle — set by VireonStack after drain completes.
    reactor_stop: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Current shutdown phase.
    shutdown_state: ShutdownState,
}

/// Cooldown for memory-pressure log messages.
const PRESSURE_LOG_INTERVAL: Duration = Duration::from_secs(5);

/// Returns `true` on the first call or after `PRESSURE_LOG_INTERVAL`
/// has elapsed since the last `true` return.
struct PressureThrottle {
    last: Option<Instant>,
}

impl PressureThrottle {
    const fn new() -> Self {
        Self { last: None }
    }

    fn tick(&mut self) -> bool {
        let now = Instant::now();
        match self.last {
            None => {
                self.last = Some(now);
                true
            }
            Some(t) if now.saturating_duration_since(t) >= PRESSURE_LOG_INTERVAL => {
                self.last = Some(now);
                true
            }
            _ => false,
        }
    }
}

/// Graceful shutdown state machine.
///
/// ```text
/// Running → Draining { deadline } → Done
/// ```
///
/// Transitions are driven by `on_tick` checking `shutdown_requested`.
enum ShutdownState {
    /// Normal operation.
    Running,
    /// Connections closed; waiting for drain timeout or all-clear.
    Draining { deadline: Instant },
    /// Drain complete — reactor stop signaled.
    Done,
}

impl VireonStack {
    /// Creates a new stack, constructing all layer components.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] if the runtime layer (ConnectionShard,
    /// TimerWheel) cannot be constructed.
    pub fn new(
        engine: QuicEngine,
        reactor_id: u32,
        echo: bool,
        shutdown_requested: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Self, ServerError> {
        Ok(Self {
            application: ApplicationLayer::new()?,
            runtime: RuntimeLayer::new(reactor_id)?,
            engine,
            connections: ConnectionRegistry::new(),
            reap_buf: Vec::with_capacity(16),
            send_scratch: Vec::with_capacity(4096),
            tick_count: 0,
            pressure_throttle: PressureThrottle::new(),
            eviction_cooldown: None,
            eviction_config: crate::eviction::EvictionConfig::default(),
            echo,
            shutdown_requested,
            reactor_stop: None,
            shutdown_state: ShutdownState::Running,
        })
    }

    /// Wires the reactor's stop handle so VireonStack can signal reactor
    /// exit after the drain period completes. Called by `Worker::run`
    /// before entering the event loop.
    pub fn set_reactor_stop(&mut self, stop: Arc<std::sync::atomic::AtomicBool>) {
        self.reactor_stop = Some(stop);
    }

    // ── Event processing ──────────────────────────────────────────

    /// Processes a batch of [`QuicEvent`]s through the layer stack.
    pub fn process_events(&mut self, events: Vec<QuicEvent>) {
        // Drive the stream scheduler once per batch (not per-frame).
        self.application.on_batch_tick();

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
                    quic_stream_id,
                    header,
                    payload,
                } => {
                    self.on_frame_received(conn_idx, quic_stream_id, header, payload);
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
                bytes_recv: 0,
                last_active_ns: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64,
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

    fn on_frame_received(
        &mut self,
        conn_idx: usize,
        quic_stream_id: u64,
        header: FrameHeader,
        payload: Buffer,
    ) {
        // Look up cross-layer connection state (ConnState is Copy).
        let Some(cs) = self.connections.get(conn_idx) else {
            tracing::warn!(conn_idx, "frame for unknown session");
            return;
        };

        // [runtime-layer] Update stats + reset timer.
        self.runtime.on_frame(conn_idx, cs.conn_id, payload.len());

        // Track per-connection activity for memory-pressure eviction.
        self.connections.record_activity(
            conn_idx,
            payload.len() as u64,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
        );

        // [benchmark] Echo mode: bypass the full application layer chain
        // (session → stream → message → application services) and echo
        // the raw frame back immediately. This isolates transport-layer
        // performance from application-layer overhead.
        if self.echo {
            self.dispatch_outgoing(OutgoingFrame {
                conn_idx,
                quic_stream_id,
                header,
                payload: payload.to_vec(),
            });
            return;
        }

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

        // Build the dispatch context.
        let ctx = FrameContext {
            session_id: cs.session_id,
            conn_idx,
            quic_stream_id,
        };

        // [application → session → stream → message] Top-down processing.
        self.application.on_frame(&ctx, &frame_ref);

        // [application-layer → transport] Drain + send outgoing frames.
        let outbox = self.application.drain_outbox();
        for out in outbox {
            self.dispatch_outgoing(out);
        }
    }

    /// Encodes an outgoing frame and sends it via the QUIC engine.
    ///
    /// Reuses `send_scratch` to avoid per-send allocation. Send failures
    /// (flow-control blocked, unknown connection) are logged at debug —
    /// full retry / deferred-send is a future task.
    fn dispatch_outgoing(&mut self, out: OutgoingFrame) {
        let wire_size = codec::HEADER_SIZE + out.payload.len();
        self.send_scratch.clear();
        self.send_scratch.resize(wire_size, 0);
        match codec::encode_into_slice(&mut self.send_scratch, out.header, &out.payload) {
            Ok(n) => {
                if let Err(e) = self.engine.send_stream(
                    out.conn_idx,
                    out.quic_stream_id,
                    &self.send_scratch[..n],
                    false,
                ) {
                    tracing::debug!(
                        conn_idx = out.conn_idx,
                        quic_stream_id = out.quic_stream_id,
                        error = ?e,
                        "send_stream failed"
                    );
                }
            }
            Err(e) => tracing::warn!(error = ?e, "encode outgoing frame failed"),
        }
    }

    fn on_stream_finished(&mut self, conn_idx: usize, stream_id: u64) {
        tracing::trace!(conn_idx, stream_id, "QUIC stream FIN");
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

    // ── Memory-pressure eviction ──────────────────────────────────

    /// Selects and closes the heaviest connections to relieve memory pressure.
    ///
    /// Collects metadata snapshots from [`ConnectionRegistry`], delegates
    /// victim selection to [`eviction::select_victims`], then closes each
    /// victim via [`QuicEngine::close_connection`]. The actual
    /// CONNECTION_CLOSE frames are flushed on the subsequent `engine.on_tick`.
    fn evict_connections(&mut self, pressure: buffer_pool::PressureLevel, now_ns: u64) {
        // 1. Collect connection metadata (borrows registry immutably).
        let infos: Vec<crate::eviction::ConnEvictInfo> = self
            .connections
            .iter()
            .map(|(&idx, cs)| crate::eviction::ConnEvictInfo {
                conn_idx: idx,
                bytes_recv: cs.bytes_recv,
                last_active_ns: cs.last_active_ns,
            })
            .collect();

        // 2. Pure victim selection.
        let victims =
            crate::eviction::select_victims(&infos, pressure, now_ns, &self.eviction_config);
        if victims.is_empty() {
            return;
        }

        // 3. Close each victim — the CONNECTION_CLOSE frame is queued
        //    inside quiche and sent on the next flush_sends.
        let reason: &[u8] = match pressure {
            buffer_pool::PressureLevel::Critical => b"memory pressure critical",
            _ => b"memory pressure high",
        };
        for &conn_idx in &victims {
            self.engine
                .close_connection(conn_idx, crate::eviction::EVICTION_ERROR_CODE, reason);
        }

        tracing::warn!(
            reactor_id = self.engine.reactor_id,
            pressure = ?pressure,
            evicted = victims.len(),
            conn_idxs = ?victims,
            remaining = self.connections.len() - victims.len(),
            "evicted connections due to memory pressure"
        );
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
        // During shutdown, existing connections still receive packets
        // (needed for CONNECTION_CLOSE acks). Only new Initial packets
        // are rejected inside process_datagram.
        self.engine.on_datagram(data, peer);
    }

    fn on_tick(&mut self, ctx: &mut TickContext) {
        self.tick_count = self.tick_count.wrapping_add(1);

        // ── Periodic observability (every 1000 ticks ≈ ~1s) ────────
        if self.tick_count % 1000 == 0 && !self.connections.is_empty() {
            let sq_stats = self.runtime.send_queue_stats();
            let cqe_snap = self.runtime.cqe_stats();
            tracing::debug!(
                tick = self.tick_count,
                conns = self.connections.len(),
                sessions = self.application.session_occupancy(),
                sq_depth = sq_stats.queue_depth,
                sq_inflight = sq_stats.inflight_count,
                cqe_total = cqe_snap.cqes_total,
                "[observability] periodic stats"
            );
        }

        // ── Memory-pressure monitoring ─────────────────────────────
        // Check every tick (cheap: 2 atomic loads). Log only when
        // elevated and throttled to avoid spam.
        let pressure = self.engine.pressure_level();
        if pressure >= buffer_pool::PressureLevel::High && self.pressure_throttle.tick() {
            let rss = buffer_pool::maintenance::pressure::resident_memory().unwrap_or(0);
            tracing::warn!(
                reactor_id = self.engine.reactor_id,
                pressure = ?pressure,
                active_conns = self.engine.active_connections(),
                outstanding_bytes = self.engine.pool_bytes_outstanding(),
                rss_bytes = rss,
                "memory pressure elevated"
            );
        }

        // ── Memory-pressure eviction ──────────────────────────────
        // Close the heaviest / idle connections to free buffer capacity
        // before OOM kills the process. Only in production mode (not
        // echo benchmark) and only during normal operation (not drain).
        if pressure >= buffer_pool::PressureLevel::High
            && matches!(self.shutdown_state, ShutdownState::Running)
            && !self.echo
            && self.connections.len() > self.eviction_config.min_survivors
        {
            let can_evict = match self.eviction_cooldown {
                None => true,
                Some(t) => {
                    Instant::now().saturating_duration_since(t) >= self.eviction_config.cooldown
                }
            };
            if can_evict {
                let evict_ns = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                self.evict_connections(pressure, evict_ns);
                self.eviction_cooldown = Some(Instant::now());
            }
        }

        // ── Shutdown phase transition: Running → Draining ─────────
        if self.shutdown_requested.load(Ordering::Acquire)
            && matches!(self.shutdown_state, ShutdownState::Running)
        {
            let active = self.engine.active_connections();
            self.engine.begin_shutdown();
            self.shutdown_state = ShutdownState::Draining {
                deadline: Instant::now() + DRAIN_TIMEOUT,
            };
            tracing::info!(
                reactor_id = self.engine.reactor_id,
                active_connections = active,
                drain_timeout = ?DRAIN_TIMEOUT,
                "shutdown drain started — closing all connections"
            );
        }

        // [transport-layer] Flush outgoing packets + cleanup closed.
        // During drain, this sends CONNECTION_CLOSE frames.
        self.engine.on_tick(ctx);

        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        // [runtime-layer] Drive task queue and other runtime services.
        self.runtime.on_tick(now_ns);

        // [transport-layer] Drain structured events.
        let events = self.engine.drain_events();

        // Process events through the full layer stack.
        if !events.is_empty() {
            self.process_events(events);
        }

        // [transport-layer] Flush outgoing packets queued by process_events
        // (echo responses, flow-control credit, MAX_STREAM_DATA).
        //
        // Without this second flush, data queued in process_events sits in
        // quiche's send buffer until the NEXT tick — a one-tick delay that
        // deadlocks the client: it's blocked on flow control waiting for
        // MAX_STREAM_DATA that the server has generated but not sent yet.
        self.engine.flush_sends(ctx);

        // ── Shutdown drain check ──────────────────────────────────
        match self.shutdown_state {
            ShutdownState::Running => {
                // Normal operation: reap idle-timed-out connections.
                self.reap_idle();
            }
            ShutdownState::Draining { deadline } => {
                let all_closed = self.engine.active_connections() == 0;
                let timed_out = Instant::now() >= deadline;
                if all_closed || timed_out {
                    let remaining = self.engine.active_connections();
                    tracing::info!(
                        reactor_id = self.engine.reactor_id,
                        all_closed,
                        timed_out,
                        remaining_connections = remaining,
                        "drain complete — stopping reactor"
                    );
                    self.shutdown_state = ShutdownState::Done;
                    if let Some(stop) = &self.reactor_stop {
                        stop.store(true, Ordering::Release);
                    }
                }
            }
            ShutdownState::Done => {
                // Keep flushing sends (CONNECTION_CLOSE retransmissions).
            }
        }
    }
}

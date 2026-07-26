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

use bytes::BytesMut;
use rustc_hash::FxHashMap;

use bytes::Bytes;
use frame::codec::{self, FrameHeader, FrameRef};
use frame::header::MessageType;
use io::handler::{IoHandler, TickContext};
use quic_engine::{QuicEngine, QuicEvent, QuicSendError};

use crate::cluster::ClusterWorkerHandle;
use crate::connection::{ConnState, ConnectionRegistry};
use crate::error::ServerError;
use crate::layers::{ApplicationLayer, FrameContext, OutgoingFrame, RuntimeLayer};

/// Drain timeout — how long to wait for connections to close cleanly
/// before forcing reactor exit. 2 seconds covers 3 × PTO (Probe Timeout)
/// for typical RTTs and retransmitted CONNECTION_CLOSE.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

// ── Per-stream pending-send buffer ──────────────────────────────────
//
// When `engine.send_stream` returns a partial write or `FlowBlocked`,
// the unwritten bytes are buffered PER STREAM and retried on subsequent
// ticks. The per-stream keying is CRITICAL for correctness: QUIC's
// `stream_send` always APPENDS to the stream buffer. If frame N
// partial-writes and its tail is buffered, frame N+1 sent directly
// would land at stream offset [N_partial_end] — BEFORE the buffered
// tail of N. When the tail is later flushed it appends AFTER N+1,
// producing stream byte order [N_partial, N+1, N_tail] and corrupting
// the subscriber's frame decoder (CrcMismatch cascade → silent loss).
//
// The fix mirrors the client transport's `pending: HashMap<u64, BytesMut>`:
// before sending a new frame, check if the stream already has pending
// bytes. If so, APPEND the new frame to the pending buffer — preserving
// byte order — instead of calling `send_stream` directly.
//
// Caps (load-shed of last resort; eviction corrupts the evicted stream):
//
//   * `PENDING_MAX_STREAMS`  — max distinct streams with pending data
//   * `PENDING_MAX_ATTEMPTS` — per-stream FlowBlocked retry budget
//   * `PENDING_BYTE_CAP`     — total bytes bound (prevents OOM under
//                              chronic subscriber starvation)
const PENDING_MAX_STREAMS: usize = 65_536;
const PENDING_MAX_ATTEMPTS: u8 = 255;
const PENDING_BYTE_CAP: usize = 1024 * 1024 * 1024; // 1 GiB

/// Per-stream pending bytes awaiting (re)send.
///
/// Keyed by `(conn_idx, quic_stream_id)` in [`VireonStack::pending_sends`]
/// so that all buffered bytes for one stream are contiguous and retried
/// in FIFO order. Owns wire-encoded bytes (header + payload + CRC).
#[derive(Debug)]
struct PendingSend {
    data: BytesMut,
    /// Consecutive `FlowBlocked` results since last progress.
    /// Reset to 0 whenever `send_stream` accepts ≥1 byte.
    blocked_attempts: u8,
}

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
    /// Thread-safe metrics store (`None` when `--metrics-port` is not set).
    /// Incremented on the hot path to expose counters at `/metrics`.
    pub metrics: Option<Arc<crate::metrics::ServerMetrics>>,
    /// Cross-layer connection registry (conn_idx → conn_id + session_id).
    connections: ConnectionRegistry,

    // ── Reusable scratch ────────────────────────────────────────
    /// Pre-allocated scratch for `reap_idle` (avoids per-tick Vec alloc).
    reap_buf: Vec<(usize, SocketAddr)>,
    /// Pre-allocated scratch for outgoing frame encoding (avoids per-send alloc).
    send_scratch: Vec<u8>,
    /// Pre-allocated scratch for eviction metadata (avoids per-pass Vec alloc).
    evict_scratch: Vec<crate::eviction::ConnEvictInfo>,

    // ── Observability ────────────────────────────────────────────
    /// Tick counter — used to throttle periodic stats logging.
    tick_count: u64,
    /// Throttle for memory-pressure warnings (logs at most every 5 s).
    pressure_throttle: PressureThrottle,
    /// ── per-frame error throttles ────────────────────────────────
    /// Encode failures on persistently malformed outgoing frames and
    /// retry-queue evictions under sustained backpressure can both fire
    /// per-tick. Each yields one log line per N events so a stuck
    /// stream doesn't drown the rest of the log.
    encode_fail_throttle: crate::log_throttle::RateCounter,
    pending_evict_throttle: crate::log_throttle::RateCounter,

    // ── Memory-pressure eviction ────────────────────────────────
    /// Last time an eviction pass ran — used to enforce cooldown.
    eviction_cooldown: Option<Instant>,
    /// Eviction thresholds and fractions.
    eviction_config: crate::eviction::EvictionConfig,

    // ── Benchmark mode ───────────────────────────────────────────
    /// When true, echo every received frame back on the same stream.
    echo: bool,

    // ── Per-stream pending-send buffer ───────────────────────────
    /// Per-`(conn_idx, quic_stream_id)` buffered bytes from partial
    /// `send_stream` writes or `FlowBlocked`. Keyed by stream so that
    /// new frames for a stream with pending data APPEND to the buffer
    /// (preserving byte order) instead of being sent directly (which
    /// would corrupt the stream — see `PENDING_BYTE_CAP` doc above).
    pending_sends: FxHashMap<(usize, u64), PendingSend>,
    /// Total bytes currently held in `pending_sends` (for cap enforcement).
    pending_bytes: usize,
    /// Frames dropped after exhausting `PENDING_MAX_ATTEMPTS` or hitting
    /// a cap. Surfaced in the periodic `[distribution]` log.
    pending_drops: u64,

    // ── Rate-limiter metric sync ────────────────────────────────
    /// Last engine handshake-rejection count observed by the metrics
    /// sync pass. The per-tick delta (`engine.handshake_rejections -
    /// last_rl_rejections`) is added to
    /// `ServerMetrics::rate_limit_rejections_total` so the Prometheus
    /// counter reflects engine state without coupling the two crates.
    last_rl_rejections: u64,

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
        acl_table: Option<topic_acl::AclTable>,
        wal_root: Option<&std::path::Path>,
        wal_retention: Option<wal_store::RetentionPolicy>,
        cluster: Option<(&cluster_coordinator::ClusterConfig, ClusterWorkerHandle)>,
        worker_inbox: Option<std::sync::mpsc::Receiver<crate::layers::InterWorkerPublish>>,
        worker_outboxes: Vec<std::sync::mpsc::SyncSender<crate::layers::InterWorkerPublish>>,
        shutdown_requested: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Self, ServerError> {
        let mut application = ApplicationLayer::new()?;
        if let Some(table) = acl_table {
            application.set_acl(table);
        }
        if let Some(policy) = wal_retention {
            application.set_wal_retention(policy);
        }
        if let Some(root) = wal_root {
            // Each worker partitions under `<root>/worker_<reactor_id>/`
            // so concurrent reactors don't fight over the same files.
            // Cross-worker WAL sharing is a P5/P6 concern.
            let worker_dir = root.join(format!("worker_{reactor_id}"));
            match wal_store::FileStore::open(&worker_dir) {
                Ok(store) => application.set_wal(store),
                Err(e) => {
                    return Err(ServerError::EngineInit(format!(
                        "wal_store open `{}`: {e:?}",
                        worker_dir.display()
                    )));
                }
            }
        }
        if let Some((cfg, handle)) = cluster {
            application.set_cluster(cfg, handle.outgoing_tx, handle.incoming_rx);
        }
        if let Some(inbox) = worker_inbox {
            application.set_worker_channels(reactor_id as usize, inbox, worker_outboxes);
        }
        Ok(Self {
            application,
            runtime: RuntimeLayer::new(reactor_id)?,
            engine,
            metrics: None,
            connections: ConnectionRegistry::new(),
            reap_buf: Vec::with_capacity(16),
            send_scratch: Vec::with_capacity(4096),
            evict_scratch: Vec::with_capacity(32),
            tick_count: 0,
            pressure_throttle: PressureThrottle::new(),
            encode_fail_throttle: crate::log_throttle::RateCounter::new(256),
            pending_evict_throttle: crate::log_throttle::RateCounter::new(128),
            eviction_cooldown: None,
            eviction_config: crate::eviction::EvictionConfig::default(),
            echo,
            pending_sends: FxHashMap::with_capacity_and_hasher(64, Default::default()),
            pending_bytes: 0,
            pending_drops: 0,
            last_rl_rejections: 0,
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

        // Drain + send all outgoing frames accumulated during the batch.
        // Shared with the no-events tick path via `flush_outbox`.
        self.flush_outbox();
    }

    /// Drains the application outbox and sends every frame via the
    /// encode-and-send path. Frames on streams bound to a send policy
    /// are consumed by the policy table and emerge from
    /// `drain_ready_egress`; unbound frames go direct.
    ///
    /// Called from both `process_events` (after inbound events) and
    /// the no-events tick path (to flush cross-worker / cluster
    /// deliveries that arrived while this worker had no QUIC traffic).
    fn flush_outbox(&mut self) {
        let outbox = self.application.drain_outbox();
        let mut direct_send: Vec<OutgoingFrame> = Vec::with_capacity(outbox.len());
        for out in outbox {
            match self.application.enqueue_egress(out) {
                None => {} // consumed by policy
                Some(out) => direct_send.push(out),
            }
        }
        for out in direct_send {
            self.encode_and_send(out.conn_idx, out.quic_stream_id, out.header, &out.payload);
            if let Some(m) = &self.metrics {
                m.inc(&m.deliveries_total);
            }
        }
        let mut policy_ready: Vec<OutgoingFrame> = Vec::new();
        self.application.drain_ready_egress(&mut policy_ready);
        for out in policy_ready {
            self.encode_and_send(out.conn_idx, out.quic_stream_id, out.header, &out.payload);
            if let Some(m) = &self.metrics {
                m.inc(&m.deliveries_total);
            }
        }
    }

    // ── Connection lifecycle ──────────────────────────────────────

    fn on_connection_established(&mut self, conn_idx: usize, peer: SocketAddr) {
        // [runtime-layer] Create connection + start idle timer.
        let Some(conn_id) = self.runtime.on_connect(conn_idx, peer) else {
            return;
        };

        // Metrics: count the connection + bump the active gauge.
        if let Some(m) = &self.metrics {
            m.inc(&m.connections_total);
            m.active_connections.fetch_add(1, Ordering::Relaxed);
        }

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

        // Track cross-layer state. The dispatcher independently tracks
        // the per-connection principal (resolved from the Handshake
        // frame) in its own map; the registry here stores only the
        // connection / session identifiers and eviction bookkeeping.
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

        // Metrics: decrement the active-connections gauge.
        if let Some(m) = &self.metrics {
            m.active_connections.fetch_sub(1, Ordering::Relaxed);
        }

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
        payload: Bytes,
    ) {
        // Look up cross-layer connection state (ConnState is Copy).
        let Some(cs) = self.connections.get(conn_idx) else {
            // Normal teardown race: the connection was removed from the
            // registry but QUIC stream events for it are still draining.
            // Not an error — just drop the frame.
            tracing::debug!(conn_idx, "frame for unknown session (teardown race)");
            return;
        };

        // [runtime-layer] Update stats + reset timer.
        self.runtime.on_frame(conn_idx, cs.conn_id, payload.len());

        // Metrics: count frames + bytes; tag publishes separately.
        if let Some(m) = &self.metrics {
            m.add(&m.bytes_recv_total, payload.len() as u64);
            if header.msg_type == MessageType::Publish {
                m.inc(&m.publishes_total);
            }
        }

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
            self.encode_and_send(conn_idx, quic_stream_id, header, &payload[..]);
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

        // Outbox is drained once per batch in `process_events`, not here —
        // this allows coalescing responses from multiple frames into fewer
        // flush cycles.
    }

    /// Encodes header + payload into `send_scratch` and sends via the
    /// QUIC engine.
    ///
    /// Uses `set_len` instead of `resize` to avoid zeroing bytes that
    /// `encode_into_slice` will immediately overwrite. On partial write
    /// or [`QuicSendError::FlowBlocked`] the unwritten bytes are buffered
    /// in `pending_sends` (per-stream) and retried on subsequent ticks;
    /// other errors are logged at debug.
    ///
    /// **Byte-order preservation:** if `pending_sends` already has an
    /// entry for this `(conn_idx, quic_stream_id)`, the new frame's
    /// bytes are APPENDED to that entry instead of being sent directly.
    /// Sending directly would call `quiche::stream_send`, which appends
    /// to the QUIC stream buffer — landing the new frame BEFORE the
    /// buffered tail of the previous frame and corrupting the stream.
    fn encode_and_send(
        &mut self,
        conn_idx: usize,
        quic_stream_id: u64,
        header: codec::FrameHeader,
        payload: &[u8],
    ) {
        let wire_size = codec::HEADER_SIZE + payload.len() + codec::CRC_SIZE;
        if self.send_scratch.capacity() < wire_size {
            self.send_scratch.reserve_exact(wire_size);
        }
        // SAFETY: capacity >= wire_size. `encode_into_slice` writes all
        // `wire_size` bytes on success. On error we `clear()` so no
        // uninitialized memory persists beyond this call.
        unsafe { self.send_scratch.set_len(wire_size) };
        let n = match codec::encode_into_slice(&mut self.send_scratch, header, payload) {
            Ok(n) => n,
            Err(e) => {
                self.send_scratch.clear();
                if self.encode_fail_throttle.tick() {
                    tracing::warn!(
                        error = ?e,
                        total = self.encode_fail_throttle.count(),
                        "encode outgoing frame failed (logging every 256th)"
                    );
                }
                return;
            }
        };

        // If there's already pending data for this stream, APPEND the
        // new frame to it — do NOT call send_stream directly. quiche's
        // stream_send always appends to the stream buffer, so sending
        // the new frame now would place its bytes BEFORE the buffered
        // tail of the prior frame, corrupting the byte stream.
        let key = (conn_idx, quic_stream_id);
        if let Some(p) = self.pending_sends.get_mut(&key) {
            let prev_len = p.data.len();
            p.data.extend_from_slice(&self.send_scratch[..n]);
            self.pending_bytes += p.data.len() - prev_len;
            self.enforce_pending_caps_on_insert(conn_idx, quic_stream_id);
            return;
        }

        // No pending data — send directly.
        let outcome = self.engine.send_stream(
            conn_idx,
            quic_stream_id,
            &self.send_scratch[..n],
            false,
        );
        match outcome {
            Ok(written) => {
                if let Some(m) = &self.metrics {
                    m.add(&m.bytes_sent_total, written as u64);
                }
                if written < n {
                    // Partial-write tail: quiche accepted `written`
                    // bytes but flow-control stopped it before the
                    // full frame. Buffer the tail per-stream so the
                    // next frame for this stream appends to it instead
                    // of landing at the wrong offset.
                    let tail = self.send_scratch[written..n].to_vec();
                    self.insert_pending(conn_idx, quic_stream_id, BytesMut::from_iter(tail));
                }
            }
            Err(QuicSendError::FlowBlocked) => {
                // Per-stream outbound buffer full: peer hasn't acked
                // enough to free credit. Buffer the whole frame for
                // retry on a subsequent tick so the peer eventually
                // receives the data and recharges flow-control credit.
                let data = self.send_scratch[..n].to_vec();
                self.insert_pending(conn_idx, quic_stream_id, BytesMut::from_iter(data));
            }
            Err(e) => {
                tracing::debug!(
                    conn_idx,
                    quic_stream_id,
                    error = ?e,
                    "send_stream failed"
                );
            }
        }
    }

    fn on_stream_finished(&mut self, conn_idx: usize, stream_id: u64) {
        tracing::trace!(conn_idx, stream_id, "QUIC stream FIN");
    }

    // ── Per-stream pending-send buffer ───────────────────────────

    /// Inserts a new `PendingSend` for `(conn_idx, stream_id)`,
    /// respecting the byte cap. The stream cap (`PENDING_MAX_STREAMS`)
    /// is bounded by the number of active connections × streams and is
    /// not expected to hit in practice; the byte cap is the primary
    /// defense against OOM under chronic subscriber starvation.
    fn insert_pending(&mut self, conn_idx: usize, quic_stream_id: u64, data: BytesMut) {
        self.enforce_pending_caps(conn_idx, quic_stream_id, data.len());
        self.pending_bytes += data.len();
        self.pending_sends.insert(
            (conn_idx, quic_stream_id),
            PendingSend {
                data,
                blocked_attempts: 0,
            },
        );
    }

    /// Same as `insert_pending` but for the APPEND path — the entry
    /// already exists. Caps are enforced by evicting OTHER streams if
    /// needed; the current entry is preserved so we don't corrupt it
    /// mid-append.
    fn enforce_pending_caps_on_insert(&mut self, conn_idx: usize, quic_stream_id: u64) {
        let key = (conn_idx, quic_stream_id);
        while self.pending_bytes > PENDING_BYTE_CAP
            || self.pending_sends.len() > PENDING_MAX_STREAMS
        {
            // Evict any OTHER entry. Pick by iteration — under chronic
            // starvation all streams are equally stuck, so the choice
            // is arbitrary. Evicting the current entry mid-append would
            // corrupt its already-partially-written stream.
            let Some((victim_key, victim_len)) = self
                .pending_sends
                .iter()
                .find(|(k, _)| **k != key)
                .map(|(k, v)| (*k, v.data.len()))
            else {
                break; // no other entry to evict
            };
            self.pending_sends.remove(&victim_key);
            self.pending_bytes -= victim_len;
            self.pending_drops += 1;
            if self.pending_evict_throttle.tick() {
                tracing::warn!(
                    conn_idx = victim_key.0,
                    quic_stream_id = victim_key.1,
                    evicted_bytes = victim_len,
                    queue_len = self.pending_sends.len(),
                    queue_bytes = self.pending_bytes,
                    total = self.pending_evict_throttle.count(),
                    "[pending] evicted entry to satisfy caps — subscriber stream may corrupt \
                     (logging every 128th)"
                );
            }
        }
    }

    /// Evicts other-stream entries until inserting `incoming_len` bytes
    /// would fit under both caps. Called by `insert_pending`.
    fn enforce_pending_caps(
        &mut self,
        conn_idx: usize,
        quic_stream_id: u64,
        incoming_len: usize,
    ) {
        let key = (conn_idx, quic_stream_id);
        while self.pending_bytes + incoming_len > PENDING_BYTE_CAP
            || self.pending_sends.len() + 1 > PENDING_MAX_STREAMS
        {
            // Evict any OTHER entry to make room. Same rationale as
            // `enforce_pending_caps_on_insert`.
            let Some((victim_key, victim_len)) = self
                .pending_sends
                .iter()
                .find(|(k, _)| **k != key)
                .map(|(k, v)| (*k, v.data.len()))
            else {
                break;
            };
            self.pending_sends.remove(&victim_key);
            self.pending_bytes -= victim_len;
            self.pending_drops += 1;
            if self.pending_evict_throttle.tick() {
                tracing::warn!(
                    conn_idx = victim_key.0,
                    quic_stream_id = victim_key.1,
                    evicted_bytes = victim_len,
                    queue_len = self.pending_sends.len(),
                    queue_bytes = self.pending_bytes,
                    total = self.pending_evict_throttle.count(),
                    "[pending] evicted entry to satisfy caps — subscriber stream may corrupt \
                     (logging every 128th)"
                );
            }
        }
    }

    /// Drains the pending-send buffer, re-attempting each stream's
    /// buffered bytes.
    ///
    /// Called once per `on_tick` BEFORE `flush_sends` so retried sends
    /// get picked up in the same tick's flush. Per-stream entries are
    /// retried in place (no reorder). Entries that succeed fully are
    /// dropped; partial writes update the buffered tail in place;
    /// entries that exhaust `PENDING_MAX_ATTEMPTS` (or hit a
    /// non-retryable error) are dropped and counted in `pending_drops`.
    fn drain_pending_sends(&mut self) {
        if self.pending_sends.is_empty() {
            return;
        }
        // Snapshot keys to release the borrow on `pending_sends` before
        // we call `engine.send_stream` (which takes &mut self).
        let keys: Vec<(usize, u64)> = self.pending_sends.keys().copied().collect();
        let mut succeeded = 0u32;
        let mut still_blocked = 0u32;
        let mut dropped = 0u32;

        for key in keys {
            let Some(p) = self.pending_sends.get_mut(&key) else {
                continue;
            };
            // Take ownership of the buffer so we can call send_stream.
            let mut buf = std::mem::take(&mut p.data);
            // buf is now empty in map; we'll either re-populate or remove below.
            let prev_len = buf.len();
            match self.engine.send_stream(key.0, key.1, &buf, false) {
                Ok(written) => {
                    if let Some(m) = &self.metrics {
                        m.add(&m.bytes_sent_total, written as u64);
                    }
                    self.pending_bytes -= prev_len;
                    if written < buf.len() {
                        // Partial write — buffer the tail, reset attempts.
                        let tail = buf.split_off(written);
                        self.pending_bytes += tail.len();
                        let p = self.pending_sends.get_mut(&key).expect("just get_mut'd above");
                        p.data = tail;
                        p.blocked_attempts = 0;
                        still_blocked += 1;
                    } else {
                        // Full send — drop the entry.
                        self.pending_sends.remove(&key);
                        succeeded += 1;
                    }
                }
                Err(QuicSendError::FlowBlocked) => {
                    // Restore the buffer; bump attempts.
                    self.pending_bytes -= prev_len;
                    self.pending_bytes += buf.len(); // same length, but be explicit
                    let p = self.pending_sends.get_mut(&key).expect("just get_mut'd above");
                    p.data = buf;
                    p.blocked_attempts = p.blocked_attempts.saturating_add(1);
                    if p.blocked_attempts >= PENDING_MAX_ATTEMPTS {
                        self.pending_bytes -= p.data.len();
                        self.pending_sends.remove(&key);
                        dropped += 1;
                    } else {
                        still_blocked += 1;
                    }
                }
                Err(e) => {
                    // Unknown connection or quiche error — drop and log.
                    self.pending_bytes -= prev_len;
                    self.pending_sends.remove(&key);
                    tracing::warn!(
                        conn_idx = key.0,
                        quic_stream_id = key.1,
                        bytes = prev_len,
                        error = ?e,
                        "[pending] dropping buffered frame — non-retryable error"
                    );
                    dropped += 1;
                }
            }
        }

        self.pending_drops += u64::from(dropped);
        if succeeded > 0 || still_blocked > 0 || dropped > 0 {
            tracing::trace!(
                succeeded,
                still_blocked,
                dropped,
                queue_len = self.pending_sends.len(),
                queue_bytes = self.pending_bytes,
                "pending sends drain"
            );
        }
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
        // 1. Collect connection metadata into reusable scratch.
        self.evict_scratch.clear();
        self.evict_scratch
            .extend(
                self.connections
                    .iter()
                    .map(|(&idx, cs)| crate::eviction::ConnEvictInfo {
                        conn_idx: idx,
                        bytes_recv: cs.bytes_recv,
                        last_active_ns: cs.last_active_ns,
                    }),
            );

        // 2. Pure victim selection.
        let victims = crate::eviction::select_victims(
            &self.evict_scratch,
            pressure,
            now_ns,
            &self.eviction_config,
        );
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
    fn on_datagram(&mut self, data: &mut [u8], peer: SocketAddr) {
        // [transport-layer] Feed raw datagram to QUIC engine.
        // During shutdown, existing connections still receive packets
        // (needed for CONNECTION_CLOSE acks). Only new Initial packets
        // are rejected inside process_datagram.
        self.engine.on_datagram(data, peer);
    }

    fn on_tick(&mut self, ctx: &mut TickContext) {
        self.tick_count = self.tick_count.wrapping_add(1);

        // ── Sync engine handshake-rejection counter to metrics ─────
        // The engine tracks rejections per-worker (no Arc<AtomicU64>
        // in the hot path); we mirror the delta to the shared metrics
        // store once per tick so Prometheus scrapes see up-to-date
        // counts. Cheap (one relaxed load + one relaxed fetch_add).
        if let Some(m) = self.metrics.as_ref() {
            let current = self.engine.handshake_rejections;
            let delta = current.wrapping_sub(self.last_rl_rejections);
            if delta > 0 {
                m.add(&m.rate_limit_rejections_total, delta);
                self.last_rl_rejections = current;
            }
        }

        // ── Retry flow-control-blocked sends BEFORE the engine tick ─
        // Draining first lets successfully-retried frames ride the
        // same tick's `engine.on_tick` → `flush_sends`, so a recovered
        // stream re-enters the dirty bitset and emits packets without
        // waiting an extra tick.
        self.drain_pending_sends();

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

        // ── Per-worker distribution stats (every 5000 ticks ≈ ~5s) ─
        // Reports active connection count + forwarded packet metrics so
        // operators can verify load balance across workers. Non-zero
        // forwarded_out / forwarded_in indicates migration activity or
        // kernel-hash misrouting (expected without eBPF steering).
        // Also surfaces stream_send FlowBlocked counts to quantify the
        // flow-control deadlock pattern (see encode_and_send drops).
        if self.tick_count % 30_000 == 0 {
            let reactor_id = self.engine.reactor_id;
            let active = self.engine.active_connections();
            let blocked = self.engine.stream_send_blocked;
            let send_bytes = self.engine.stream_send_bytes;
            let pending_len = self.pending_sends.len();
            let pending_bytes = self.pending_bytes;
            let pending_drops = self.pending_drops;
            if let Some(mesh) = self.engine.forward_mesh() {
                tracing::info!(
                    reactor_id,
                    active_conns = active,
                    forwarded_out = mesh.forwarded_out,
                    forwarded_in = mesh.forwarded_in,
                    forward_drops = mesh.forward_drops,
                    stream_send_blocked = blocked,
                    stream_send_bytes = send_bytes,
                    pending_streams = pending_len,
                    pending_bytes,
                    pending_drops,
                    "[distribution] worker stats"
                );
            } else {
                tracing::info!(
                    reactor_id,
                    active_conns = active,
                    stream_send_blocked = blocked,
                    stream_send_bytes = send_bytes,
                    pending_streams = pending_len,
                    pending_bytes,
                    pending_drops,
                    "[distribution] worker stats (single-worker mode)"
                );
            }
        }

        // ── Memory-pressure monitoring ─────────────────────────────
        // Two independent pressure signals:
        //   1. Pool-internal: outstanding pool bytes vs pool capacity
        //   2. cgroup-level:  real process RSS vs cgroup memory limit
        // Take the higher (more urgent) of the two — a cgroup OOM kills
        // the process regardless of how healthy the pool thinks it is.
        let pool_pressure = self.engine.pressure_level();
        let cgroup_pressure = buffer_pool::maintenance::cgroup::cgroup_pressure_level();
        let pressure = pool_pressure.max(cgroup_pressure);

        if pressure >= buffer_pool::PressureLevel::High && self.pressure_throttle.tick() {
            let rss = buffer_pool::maintenance::pressure::resident_memory().unwrap_or(0);
            let cgroup_usage = buffer_pool::maintenance::cgroup::cgroup_memory_usage().unwrap_or(0);
            let cgroup_limit = buffer_pool::maintenance::cgroup::cgroup_memory_limit().unwrap_or(0);
            tracing::warn!(
                reactor_id = self.engine.reactor_id,
                pressure = ?pressure,
                pool_pressure = ?pool_pressure,
                cgroup_pressure = ?cgroup_pressure,
                active_conns = self.engine.active_connections(),
                outstanding_bytes = self.engine.pool_bytes_outstanding(),
                rss_bytes = rss,
                cgroup_usage_bytes = cgroup_usage,
                cgroup_limit_bytes = cgroup_limit,
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
        } else {
            // Even with no inbound events, we must drain cross-worker
            // and cluster inboxes each tick — otherwise publishes
            // forwarded from other workers pile up undelivered when
            // this worker has no incoming QUIC traffic to trigger
            // `process_events`.
            self.application.on_batch_tick();
            self.flush_outbox();
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

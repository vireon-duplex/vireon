//! Runtime layer — per-core runtime infrastructure.
//!
//! Owns the shared-nothing runtime primitives:
//!
//! - `ConnectionShard` — connection ownership and lookup
//! - `TimerWheel` — idle-timeout reaping
//! - `BufferPool` — NUMA-aware scratch buffers
//! - `SendQueue` — outbound packet queue (metrics + backpressure)
//! - `TaskQueue` — reactor-local deferred task execution
//! - `CqeProcessor` — CQE batch dispatch (metrics + lifecycle events)
//! - `RuntimeReactor` — orchestrator facade (config + stats)
//!
//! Timer handles are tracked internally so the upper layers never
//! need to manage timer lifecycle directly.

use rustc_hash::FxHashMap;
use std::net::SocketAddr;
use std::time::Duration;

use buffer_pool::BufferPool;
use connection_shard::{ConnectionId, ConnectionShard, ConnectionShardBuilder, ConnectionSpec};
use timer_wheel::{TaskId, TimerHandle, TimerWheel, TimerWheelBuilder};

use cqe_processor::{CqeProcessor, CqeProcessorBuilder};
use send_queue::{SendQueue, SendQueueBuilder};
use task_queue::{TaskContext, TaskQueue, TaskQueueBuilder};
use vireon_reactor::{Reactor as RuntimeReactor, ReactorBuilder};

use crate::error::ServerError;

/// Idle timeout — connections with no traffic for this duration are closed.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Runtime layer — owns ConnectionShard, TimerWheel, BufferPool,
/// SendQueue, TaskQueue, CqeProcessor, and the runtime-layer Reactor facade.
pub struct RuntimeLayer {
    /// Reactor ID for this instance.
    reactor_id: u32,
    /// Connection ownership store (runtime-layer).
    shard: ConnectionShard,
    /// Hierarchical timing wheel (runtime-layer).
    timer: TimerWheel,
    /// NUMA-aware buffer pool (runtime-layer).
    #[allow(dead_code)]
    pool: BufferPool,
    /// Per-connection idle timer handles.
    timer_handles: FxHashMap<usize, TimerHandle>,
    /// Outbound packet queue (runtime-layer) — metrics + backpressure.
    pub send_queue: SendQueue,
    /// Reactor-local task execution (runtime-layer).
    pub task_queue: TaskQueue,
    /// CQE batch dispatch (runtime-layer) — metrics + lifecycle events.
    pub cqe_processor: CqeProcessor,
    /// Runtime-layer Reactor facade — config + stats orchestrator.
    pub reactor_facade: RuntimeReactor,
}

impl RuntimeLayer {
    /// Creates a new runtime layer bound to `reactor_id`.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] if ConnectionShard or TimerWheel
    /// cannot be constructed.
    pub fn new(reactor_id: u32) -> Result<Self, ServerError> {
        let shard = ConnectionShardBuilder::new()
            .reactor_id(reactor_id)
            .build()
            .map_err(|e| ServerError::EngineInit(format!("connection-shard: {e:?}")))?;

        let timer = TimerWheelBuilder::new()
            .shards(1)
            .cpu_pin(reactor_id)
            .spawn()
            .map_err(|e| ServerError::EngineInit(format!("timer-wheel: {e:?}")))?;

        let send_queue = SendQueueBuilder::new()
            .capacity(4096)
            .build()
            .map_err(|e| ServerError::EngineInit(format!("send-queue: {e:?}")))?;

        let task_queue = TaskQueueBuilder::new()
            .capacity(1024)
            .build()
            .map_err(|e| ServerError::EngineInit(format!("task-queue: {e:?}")))?;

        let cqe_processor = CqeProcessorBuilder::new()
            .batch_max_cqes(256)
            .reactor_id(reactor_id)
            .build()
            .map_err(|e| ServerError::EngineInit(format!("cqe-processor: {e:?}")))?;

        let reactor_facade = ReactorBuilder::new()
            .cpu_id(reactor_id)
            .build()
            .map_err(|e| ServerError::EngineInit(format!("runtime-reactor: {e:?}")))?;

        Ok(Self {
            reactor_id,
            shard,
            timer,
            pool: BufferPool::default(),
            timer_handles: FxHashMap::default(),
            send_queue,
            task_queue,
            cqe_processor,
            reactor_facade,
        })
    }

    // ── Connection lifecycle ───────────────────────────────────────

    /// Creates a connection in the shard and starts the idle timer.
    ///
    /// Returns `None` if the shard is full. On success, returns the
    /// runtime-layer `ConnectionId`.
    pub fn on_connect(&mut self, conn_idx: usize, peer: SocketAddr) -> Option<ConnectionId> {
        let spec = ConnectionSpec::new(conn_idx as u64, peer);
        let conn_id = match self.shard.create(spec) {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!(conn_idx, error = ?e, "connection-shard create failed");
                return None;
            }
        };
        let _ = self.shard.activate(conn_id);

        // Start idle timer (task_id = conn_idx).
        if let Ok(handle) = self.timer.start(conn_idx as TaskId, IDLE_TIMEOUT) {
            self.timer_handles.insert(conn_idx, handle);
        }

        Some(conn_id)
    }

    /// Updates recv stats and resets the idle timer.
    pub fn on_frame(&mut self, conn_idx: usize, conn_id: ConnectionId, frame_len: usize) {
        // Compute time once — reused for both touch and timer.
        let now_ns = connection_shard::shard::monotonic_now_ns();
        let _ = self.shard.touch_recv(conn_id, frame_len, now_ns);

        // Reset idle timer (cancel old + start new).
        if let Some(old) = self.timer_handles.remove(&conn_idx) {
            let _ = self.timer.cancel(old);
        }
        if let Ok(handle) = self.timer.start(conn_idx as TaskId, IDLE_TIMEOUT) {
            self.timer_handles.insert(conn_idx, handle);
        }
    }

    /// Cancels the idle timer and destroys the connection.
    pub fn on_close(&mut self, conn_idx: usize, conn_id: ConnectionId) {
        if let Some(handle) = self.timer_handles.remove(&conn_idx) {
            let _ = self.timer.cancel(handle);
        }
        let _ = self.shard.destroy(conn_id);
    }

    // ── Timer reaping ──────────────────────────────────────────────

    /// Drains expired idle timers and returns the list of conn_idx
    /// values that should be reaped.
    pub fn drain_expired(&self) -> Vec<usize> {
        self.timer
            .drain_expired()
            .into_iter()
            .map(|t| t as usize)
            .collect()
    }

    // ── Task ticking ───────────────────────────────────────────────

    /// Drives the task queue and drains pending CQE processor events
    /// for one reactor tick.
    pub fn on_tick(&mut self, now_ns: u64) {
        // [task-queue] Execute ready deferred tasks.
        let mut ctx = TaskContext::new(now_ns, self.reactor_id);
        let _res = self.task_queue.tick(&mut ctx, now_ns);

        // [cqe-processor] Drain any pending lifecycle events that may
        // have been enqueued by dispatch_one calls. These are usually
        // empty in the current architecture (io-layer handles CQEs
        // directly), but draining ensures correctness if synthetic
        // events are ever injected.
        let _recycles = self.cqe_processor.drain_pending_recycles();
        let _sendq_events = self.cqe_processor.drain_pending_sendq_events();
        let _timers = self.cqe_processor.drain_pending_timer_retires();
        let _tasks = self.cqe_processor.drain_pending_tasks();
    }

    // ── Introspection ──────────────────────────────────────────────

    /// Returns the number of active connections in the shard.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.shard.active_count()
    }

    /// Returns a snapshot of send-queue stats for observability.
    #[must_use]
    pub fn send_queue_stats(&self) -> send_queue::StatsSnapshot {
        self.send_queue.stats()
    }

    /// Returns the current send-queue depth.
    #[must_use]
    #[allow(dead_code)]
    pub fn send_queue_depth(&self) -> usize {
        self.send_queue.depth()
    }

    /// Returns a snapshot of CQE processor stats.
    #[must_use]
    pub fn cqe_stats(&self) -> cqe_processor::StatsSnapshot {
        self.cqe_processor.stats()
    }

    /// Returns a snapshot of the runtime-reactor stats.
    #[must_use]
    #[allow(dead_code)]
    pub fn reactor_stats(&self) -> vireon_reactor::StatsSnapshot {
        self.reactor_facade.stats().snapshot()
    }
}

impl Drop for RuntimeLayer {
    fn drop(&mut self) {
        // Shut down timer worker thread.
        self.timer.shutdown();
    }
}

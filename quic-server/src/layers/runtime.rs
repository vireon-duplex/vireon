//! Runtime layer — per-core runtime infrastructure.
//!
//! Owns the three shared-nothing runtime primitives:
//!
//! - `ConnectionShard` — connection ownership and lookup
//! - `TimerWheel` — idle-timeout reaping
//! - `BufferPool` — NUMA-aware scratch buffers
//!
//! Timer handles are tracked internally so the upper layers never
//! need to manage timer lifecycle directly.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use buffer_pool::BufferPool;
use connection_shard::{ConnectionId, ConnectionShard, ConnectionShardBuilder, ConnectionSpec};
use timer_wheel::{TaskId, TimerHandle, TimerWheel, TimerWheelBuilder};

use crate::error::ServerError;

/// Idle timeout — connections with no traffic for this duration are closed.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Runtime layer — owns ConnectionShard, TimerWheel, and BufferPool.
pub struct RuntimeLayer {
    /// Connection ownership store (runtime-layer).
    shard: ConnectionShard,
    /// Hierarchical timing wheel (runtime-layer).
    timer: TimerWheel,
    /// NUMA-aware buffer pool (runtime-layer).
    pool: BufferPool,
    /// Per-connection idle timer handles.
    timer_handles: HashMap<usize, TimerHandle>,
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
            .spawn()
            .map_err(|e| ServerError::EngineInit(format!("timer-wheel: {e:?}")))?;

        Ok(Self {
            shard,
            timer,
            pool: BufferPool::default(),
            timer_handles: HashMap::new(),
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

    /// Updates recv stats, resets the idle timer, and acquires a
    /// scratch buffer from the pool.
    pub fn on_frame(&mut self, conn_idx: usize, conn_id: ConnectionId, frame_len: usize) {
        // Update connection recv stats.
        let _ = self.shard.touch_recv(conn_id, frame_len);

        // Reset idle timer (cancel old + start new).
        if let Some(old) = self.timer_handles.remove(&conn_idx) {
            let _ = self.timer.cancel(old);
        }
        if let Ok(handle) = self.timer.start(conn_idx as TaskId, IDLE_TIMEOUT) {
            self.timer_handles.insert(conn_idx, handle);
        }

        // Acquire scratch buffer.
        if let Err(e) = self.pool.get(frame_len).as_ref() {
            tracing::warn!(conn_idx, error = ?e, "buffer pool exhausted");
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

    // ── Introspection ──────────────────────────────────────────────

    /// Returns the number of active connections in the shard.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.shard.active_count()
    }
}

impl Drop for RuntimeLayer {
    fn drop(&mut self) {
        // Shut down timer worker thread.
        self.timer.shutdown();
    }
}

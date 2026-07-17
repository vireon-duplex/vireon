//! Maintenance worker (Layer 19).
//!
//! Previously a background thread; now a **no-op stub** since the pool is
//! single-threaded (`Rc`-based). The reactor should call [`run_tick`]
//! inline during its event loop if maintenance is needed.
//!
//! [`MaintenanceWorker`] is kept as a type for API compatibility but does
//! not spawn a thread.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::buffer::pool::BufferPool;
use crate::maintenance::pressure::{classify_pressure, resident_memory};
use crate::maintenance::shrink::maybe_shrink;
use crate::maintenance::tuning::analyze;

/// Maintenance handle for a [`BufferPool`].
///
/// No longer spawns a background thread — the pool is single-threaded.
/// Dropping this is a no-op.
pub struct MaintenanceWorker {
    _stop: Arc<AtomicBool>,
}

impl MaintenanceWorker {
    /// Returns a no-op maintenance handle.
    ///
    /// The caller should call [`run_tick`] manually from the reactor's
    /// event loop if periodic maintenance is desired.
    pub fn start(_pool: BufferPool) -> std::io::Result<Self> {
        Ok(Self {
            _stop: Arc::new(AtomicBool::new(false)),
        })
    }

    /// No-op (kept for API compatibility).
    pub fn stop(&mut self) {}

    /// Always `false` (no background thread to stop).
    pub fn is_stopped(&self) -> bool {
        false
    }
}

/// Runs one maintenance tick.
///
/// Exposed so the reactor can call it inline during its event loop.
pub fn run_tick(pool: &BufferPool) {
    let stats = pool.stats();
    let rss = resident_memory().unwrap_or(0);
    let level = classify_pressure(rss, pool.config().max_memory);

    let shrank = maybe_shrink(pool, level, pool.config().shrink);

    let report = analyze(&stats, pool.config().batch);

    tracing::debug!(
        target: "buffer_pool::maintenance",
        outstanding = stats.counters.outstanding,
        arena_bytes = stats.arena_bytes,
        cached_bytes = stats.cached_bytes,
        hit_rate = %format!("{:.3}", report.hit_rate),
        miss_rate = %format!("{:.3}", report.miss_rate),
        pressure = ?level,
        shrank,
        recommended_batch = report.recommended_batch,
        "maintenance tick"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_tick_does_not_panic_on_fresh_pool() {
        let pool = BufferPool::default();
        run_tick(&pool);
    }

    #[test]
    fn run_tick_after_traffic() {
        let pool = BufferPool::default();
        let buffers: Vec<_> = (0..8).map(|_| pool.get(256).unwrap()).collect();
        drop(buffers);
        run_tick(&pool);
    }

    #[test]
    fn worker_starts_and_drops_cleanly() {
        let pool = BufferPool::default();
        let worker = MaintenanceWorker::start(pool).expect("start");
        drop(worker);
    }
}

//! Background maintenance worker (Layer 19).
//!
//! [`MaintenanceWorker`] spawns a named `std::thread` that periodically reads
//! stats, checks memory pressure, shrinks caches, and logs tuning
//! recommendations. The worker is stopped via an [`AtomicBool`] flag and joined
//! on [`Drop`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::buffer::pool::BufferPool;
use crate::maintenance::pressure::{classify_pressure, resident_memory};
use crate::maintenance::shrink::maybe_shrink;
use crate::maintenance::tuning::analyze;

/// Background maintenance thread for a [`BufferPool`].
///
/// Construct via [`BufferPool::start_maintenance`]. The worker runs until
/// [`stop`](MaintenanceWorker::stop) is called or it is dropped.
pub struct MaintenanceWorker {
    handle: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl MaintenanceWorker {
    /// Spawns the maintenance thread for `pool`.
    ///
    /// The thread ticks every `pool.config().maintenance_interval_ms`
    /// milliseconds.
    ///
    /// Returns [`std::io::Error`] if the OS could not create the thread.
    pub fn start(pool: BufferPool) -> std::io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let interval_ms = pool.config().maintenance_interval_ms;
        let handle = thread::Builder::new()
            .name("bp-maintenance".into())
            .spawn(move || {
                worker_loop(pool, stop_clone, interval_ms);
            })?;
        Ok(Self {
            handle: Some(handle),
            stop,
        })
    }

    /// Signals the worker to stop and waits for the thread to finish.
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    /// `true` after [`stop`](MaintenanceWorker::stop) has been called.
    pub fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }
}

impl Drop for MaintenanceWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The worker's main loop.
fn worker_loop(pool: BufferPool, stop: Arc<AtomicBool>, interval_ms: u64) {
    let interval = Duration::from_millis(interval_ms);
    while !stop.load(Ordering::Relaxed) {
        run_tick(&pool);
        thread::sleep(interval);
    }
}

/// Runs one maintenance tick.
///
/// Exposed so tests can exercise the logic without spawning a thread.
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
        pool.flush();
        run_tick(&pool);
    }

    #[test]
    fn worker_starts_and_stops() {
        let pool = BufferPool::default();
        let mut worker = MaintenanceWorker::start(pool).expect("spawn");
        assert!(!worker.is_stopped());
        worker.stop();
        assert!(worker.is_stopped());
    }

    #[test]
    fn worker_drops_cleanly() {
        let pool = BufferPool::default();
        let worker = MaintenanceWorker::start(pool).expect("spawn");
        drop(worker);
        // If we reach here, Drop joined the thread without deadlock.
    }
}

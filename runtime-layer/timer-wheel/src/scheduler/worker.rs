//! Worker event loop (Layer 8).
//!
//! A [`Worker`] owns one [`Shard`] and drives its event loop on a dedicated,
//! named `std::thread`. Shutdown is signalled via a shared [`AtomicBool`]; on
//! `Drop` the worker stops and joins, mirroring the maintenance-worker
//! pattern in `buffer-pool`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::error::TimerError;
use crate::scheduler::shard::Shard;

/// Runs one shard's event loop on a dedicated thread.
pub struct Worker {
    handle: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl Worker {
    /// Spawns a worker thread that drives `shard` until [`shutdown`](Self::shutdown)
    /// (or drop) is called.
    ///
    /// `idle_interval` bounds the sleep between ticks (and therefore
    /// command-ingestion latency). When `cpu_pin` is `Some(core)`, the worker
    /// thread is pinned to that core via `sched_setaffinity` before entering
    /// the run loop — this co-locates the timer worker with its owning
    /// reactor on multi-socket NUMA systems.
    pub fn spawn(
        name: String,
        mut shard: Shard,
        idle_interval: Duration,
        stop: Arc<AtomicBool>,
        cpu_pin: Option<u32>,
    ) -> Result<Self, TimerError> {
        let stop_for_thread = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name(name)
            .spawn(move || {
                if let Some(core) = cpu_pin {
                    // Best-effort: log on failure but don't abort the worker.
                    if let Err(e) = pin_to_core(core) {
                        tracing::warn!(
                            core,
                            error = %e,
                            "failed to pin timer-wheel worker; continuing unpinned"
                        );
                    }
                }
                run_loop(&mut shard, idle_interval, &stop_for_thread)
            })
            .map_err(|e| {
                TimerError::InvalidConfig(format!("failed to spawn worker thread: {e}"))
            })?;
        Ok(Self {
            handle: Some(handle),
            stop,
        })
    }

    /// Signals the worker to stop and joins the thread.
    ///
    /// Shutdown latency is bounded by the worker's current `thread::sleep`
    /// (at most `idle_interval`).
    pub fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    /// Whether the worker thread has been signalled to stop.
    #[inline]
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_loop(shard: &mut Shard, idle_interval: Duration, stop: &AtomicBool) {
    while !stop.load(Ordering::Relaxed) {
        shard.tick();
        let park = shard.park_duration(idle_interval);
        if park > Duration::ZERO {
            thread::sleep(park);
        }
    }
}

/// Pins the calling thread to a single CPU core via `sched_setaffinity(2)`.
///
/// Returns a string describing the errno on failure. Best-effort: callers
/// log but do not abort.
fn pin_to_core(core: u32) -> Result<(), String> {
    // SAFETY: `cpu_set_t` is a POD bitset; `CPU_ZERO`/`CPU_SET` initialise it
    // in place. `sched_setaffinity(0, ..)` targets the calling thread.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe { libc::CPU_ZERO(&mut set) };
    unsafe { libc::CPU_SET(core as usize, &mut set) };
    let rc =
        unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) };
    if rc == 0 {
        Ok(())
    } else {
        let errno = unsafe { *libc::__errno_location() };
        Err(format!(
            "sched_setaffinity(cpu={core}) failed (errno={errno})"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TimerWheelConfig;
    use crate::scheduler::clock::Clock;
    use crate::scheduler::shard::TimerCmd;

    fn valid_cfg() -> TimerWheelConfig {
        TimerWheelConfig {
            bits_per_level: 6,
            levels: 3,
            tick_ns: 1_000_000,
            capacity_per_shard: 256,
            shards: 1,
            cmd_queue_capacity: 64,
            idle_interval_ns: 5_000_000,
        }
    }

    #[test]
    fn worker_starts_and_stops_cleanly() {
        let cfg = valid_cfg();
        let clock = Clock::shared(cfg.tick_ns);
        let (shard, _channels) = Shard::new(
            0,
            cfg.bits_per_level,
            cfg.levels,
            cfg.capacity_per_shard,
            cfg.cmd_queue_capacity.next_power_of_two(),
            cfg.cmd_queue_capacity.next_power_of_two(),
            clock,
        )
        .expect("shard");
        let stop = Arc::new(AtomicBool::new(false));
        let mut worker =
            Worker::spawn("tw-test".into(), shard, Duration::from_millis(5), stop, None).expect("spawn");
        std::thread::sleep(Duration::from_millis(10));
        worker.shutdown();
        assert!(worker.is_stopped());
        assert!(worker.handle.is_none());
    }

    #[test]
    fn drop_impl_shuts_down() {
        let cfg = valid_cfg();
        let clock = Clock::shared(cfg.tick_ns);
        let (shard, _channels) = Shard::new(
            0,
            cfg.bits_per_level,
            cfg.levels,
            cfg.capacity_per_shard,
            cfg.cmd_queue_capacity.next_power_of_two(),
            cfg.cmd_queue_capacity.next_power_of_two(),
            clock,
        )
        .expect("shard");
        let stop = Arc::new(AtomicBool::new(false));
        {
            let _worker = Worker::spawn("tw-drop".into(), shard, Duration::from_millis(5), stop, None)
                .expect("spawn");
            std::thread::sleep(Duration::from_millis(8));
            // worker dropped here
        }
        // if Drop deadlocked, the test would time out in CI.
    }

    #[test]
    fn worker_fires_a_timer_end_to_end() {
        let cfg = valid_cfg();
        let clock = Clock::shared(cfg.tick_ns);
        let (shard, channels) = Shard::new(
            0,
            cfg.bits_per_level,
            cfg.levels,
            cfg.capacity_per_shard,
            cfg.cmd_queue_capacity.next_power_of_two(),
            cfg.cmd_queue_capacity.next_power_of_two(),
            Arc::clone(&clock),
        )
        .expect("shard");
        let now = clock.now_ticks();
        channels.outstanding.fetch_add(1, Ordering::Relaxed);
        let _ = channels.cmd_tx.push(TimerCmd::Start {
            task_id: 5,
            deadline_ticks: now + 3,
        });
        let stop = Arc::new(AtomicBool::new(false));
        let mut worker =
            Worker::spawn("tw-fire".into(), shard, Duration::from_millis(2), stop, None).expect("spawn");
        // wait long enough for the 3-tick (3 ms) timer to fire
        std::thread::sleep(Duration::from_millis(40));
        let mut fired = Vec::new();
        channels.expired_rx.drain_into(&mut fired);
        worker.shutdown();
        assert_eq!(fired, vec![5]);
    }
}

//! `TimerWheel` facade (Layer 9).
//!
//! [`TimerWheel`] hides the sharding, lock-free ingestion, and worker-thread
//! machinery behind four synchronous operations: [`start`](TimerWheel::start),
//! [`cancel`](TimerWheel::cancel), [`drain_expired`](TimerWheel::drain_expired),
//! and [`shutdown`](TimerWheel::shutdown).

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::concurrent::sharder::Sharder;
use crate::config::TimerWheelConfig;
use crate::error::{ShardId, TimerError};
use crate::scheduler::clock::Clock;
use crate::scheduler::shard::{Shard, ShardChannels, TaskId, TimerCmd, TimerHandler};
use crate::scheduler::worker::Worker;

/// Opaque handle returned by [`TimerWheel::start`] for later cancellation.
#[derive(Clone, Copy, Debug)]
pub struct TimerHandle {
    shard: ShardId,
    task_id: TaskId,
}

impl TimerHandle {
    /// The shard owning this timer.
    #[inline]
    #[must_use]
    pub const fn shard(self) -> ShardId {
        self.shard
    }

    /// The application-supplied task id.
    #[inline]
    #[must_use]
    pub const fn task_id(self) -> TaskId {
        self.task_id
    }
}

/// Top-level timer-wheel handle. Owns the worker threads and shard channels.
pub struct TimerWheel {
    shards: Vec<ShardChannels>,
    sharder: Sharder,
    workers: Vec<Worker>,
    clock: Arc<Clock>,
    config: TimerWheelConfig,
}

impl TimerWheel {
    /// Returns a builder with defaults (call `.spawn()` to start workers).
    #[must_use]
    pub fn builder() -> TimerWheelBuilder {
        TimerWheelBuilder::default()
    }

    /// Number of shards.
    #[inline]
    #[must_use]
    pub fn shards(&self) -> NonZeroU32 {
        self.sharder.shards()
    }

    /// Approximate total number of outstanding timers across all shards.
    ///
    /// Sum of the per-shard `outstanding` counters at this instant. Useful
    /// for observability and leak detection; the value is a snapshot and
    /// may move the moment it is read.
    #[inline]
    #[must_use]
    pub fn outstanding(&self) -> u64 {
        self.shards
            .iter()
            .map(|ch| ch.outstanding.load(Ordering::Relaxed))
            .sum()
    }

    /// Schedules `task_id` to fire after `duration`.
    ///
    /// The handle returned is suitable for [`cancel`](Self::cancel); it does
    /// not need to be kept if cancellation is unnecessary.
    pub fn start(&self, task_id: TaskId, duration: Duration) -> Result<TimerHandle, TimerError> {
        let ticks = duration_to_ticks(duration, self.config.tick_ns)?;
        let shard = self.sharder.route(task_id);
        let channels = &self.shards[shard as usize];

        // Synchronous capacity gate so the worker's slab allocate cannot fail.
        reserve_outstanding(&channels.outstanding, channels.capacity, shard)?;

        let deadline_ticks = self.clock.now_ticks().saturating_add(ticks);
        match channels.cmd_tx.push(TimerCmd::Start {
            task_id,
            deadline_ticks,
        }) {
            Ok(()) => Ok(TimerHandle { shard, task_id }),
            Err(_) => {
                // Roll back the reservation.
                channels.outstanding.fetch_sub(1, Ordering::Relaxed);
                Err(TimerError::ShardFull {
                    shard,
                    capacity: channels.capacity,
                })
            }
        }
    }

    /// Best-effort cancellation. A cancel issued before the worker has
    /// processed the corresponding `start` is still honoured.
    pub fn cancel(&self, handle: TimerHandle) -> Result<(), TimerError> {
        let channels = &self.shards[handle.shard as usize];
        match channels.cmd_tx.push(TimerCmd::Cancel {
            task_id: handle.task_id,
        }) {
            Ok(()) => Ok(()),
            Err(_) => Err(TimerError::ShardFull {
                shard: handle.shard,
                capacity: channels.capacity,
            }),
        }
    }

    /// Drains every fired `task_id` buffered across all shards.
    ///
    /// Order is not guaranteed across shards; within a shard, roughly
    /// deadline-ordered.
    #[must_use]
    pub fn drain_expired(&self) -> Vec<TaskId> {
        let mut out = Vec::new();
        for channels in &self.shards {
            channels.expired_rx.drain_into(&mut out);
        }
        out
    }

    /// Stops every worker and joins the threads. Idempotent.
    pub fn shutdown(&mut self) {
        for worker in &mut self.workers {
            worker.shutdown();
        }
    }

    /// A reference to the shared monotonic clock.
    #[must_use]
    pub fn clock(&self) -> &Clock {
        &self.clock
    }
}

impl Drop for TimerWheel {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Fluent builder for [`TimerWheel`].
#[derive(Default)]
pub struct TimerWheelBuilder {
    config: TimerWheelConfig,
    handler: Option<Arc<dyn TimerHandler>>,
    /// CPU core to pin each worker thread to (None = no pinning).
    cpu_pin: Option<u32>,
}

impl TimerWheelBuilder {
    /// Creates a builder populated with [`TimerWheelConfig::default`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Bits per wheel level (default 6 → 64 slots/level).
    #[must_use]
    pub fn bits_per_level(mut self, bits: u32) -> Self {
        self.config.bits_per_level = bits;
        self
    }

    /// Number of hierarchical levels (default 4).
    #[must_use]
    pub fn levels(mut self, levels: u32) -> Self {
        self.config.levels = levels;
        self
    }

    /// Nanoseconds per tick (default 1 ms).
    #[must_use]
    pub fn tick_duration(mut self, tick_ns: u64) -> Self {
        self.config.tick_ns = tick_ns;
        self
    }

    /// Slab capacity (max outstanding timers) per shard (default 65 536).
    #[must_use]
    pub fn capacity_per_shard(mut self, capacity: usize) -> Self {
        self.config.capacity_per_shard = capacity;
        self
    }

    /// Number of worker shards (default `available_parallelism()`).
    #[must_use]
    pub fn shards(mut self, shards: u32) -> Self {
        self.config.shards = shards;
        self
    }

    /// Per-shard command-queue capacity (rounded up to a power of two).
    #[must_use]
    pub fn cmd_queue_capacity(mut self, capacity: usize) -> Self {
        self.config.cmd_queue_capacity = capacity;
        self
    }

    /// Worker park duration in nanoseconds when the wheel is idle.
    #[must_use]
    pub fn idle_interval_ns(mut self, ns: u64) -> Self {
        self.config.idle_interval_ns = ns;
        self
    }

    /// Installs an inline fire handler invoked on each worker thread.
    #[must_use]
    pub fn handler(mut self, handler: Arc<dyn TimerHandler>) -> Self {
        self.handler = Some(handler);
        self
    }

    /// Pins each worker thread to `core` via `sched_setaffinity` before
    /// entering its run loop. Use to co-locate a timer worker with its
    /// owning reactor on the same core / NUMA node.
    ///
    /// When the wheel has multiple shards, all workers pin to the same
    /// core — typical usage is `shards(1)` per reactor.
    #[must_use]
    pub fn cpu_pin(mut self, core: u32) -> Self {
        self.cpu_pin = Some(core);
        self
    }

    /// Validates the configuration and constructs the wheel, spawning one
    /// worker thread per shard.
    pub fn spawn(self) -> Result<TimerWheel, TimerError> {
        let mut config = self.config;
        config.round_cmd_queue_up();
        config.validate()?;
        let shards_count = config.shards;
        let sharder = Sharder::new(
            NonZeroU32::new(shards_count)
                .ok_or_else(|| TimerError::InvalidConfig("shards must be > 0".into()))?,
        );
        let clock = Clock::shared(config.tick_ns);
        let expired_cap = config.cmd_queue_capacity; // generous default

        let mut channels_vec = Vec::with_capacity(shards_count as usize);
        let mut workers = Vec::with_capacity(shards_count as usize);
        for sid in 0..shards_count {
            let (mut shard, channels) = Shard::new(
                sid,
                config.bits_per_level,
                config.levels,
                config.capacity_per_shard,
                config.cmd_queue_capacity,
                expired_cap,
                Arc::clone(&clock),
            )
            .ok_or_else(|| TimerError::InvalidConfig("shard capacity too large".into()))?;
            if let Some(handler) = self.handler.as_ref() {
                shard.set_handler(Arc::clone(handler));
            }
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let idle = Duration::from_nanos(config.idle_interval_ns);
            let worker = Worker::spawn(
                format!("timer-wheel-{sid}"),
                shard,
                idle,
                stop,
                self.cpu_pin,
            )?;
            channels_vec.push(channels);
            workers.push(worker);
        }

        Ok(TimerWheel {
            shards: channels_vec,
            sharder,
            workers,
            clock,
            config,
        })
    }
}

/// Converts a duration to a positive tick count, validating the range.
fn duration_to_ticks(duration: Duration, tick_ns: u64) -> Result<u64, TimerError> {
    let nanos = duration.as_nanos();
    if nanos == 0 {
        return Err(TimerError::InvalidDuration { nanos });
    }
    // ceil division so a sub-tick duration rounds up to one tick
    let ticks = nanos.div_ceil(u128::from(tick_ns)) as u64;
    Ok(ticks)
}

fn reserve_outstanding(
    counter: &AtomicU64,
    capacity: usize,
    shard: ShardId,
) -> Result<(), TimerError> {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        if current >= capacity as u64 {
            return Err(TimerError::WheelFull { shard, capacity });
        }
        match counter.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Ok(()),
            Err(actual) => current = actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Instant;

    #[test]
    fn duration_to_ticks_ceil_divides() {
        assert_eq!(
            duration_to_ticks(Duration::from_nanos(0), 1_000_000)
                .unwrap_err()
                .to_string(),
            contains_zero()
        );
        assert_eq!(
            duration_to_ticks(Duration::from_millis(1), 1_000_000).unwrap(),
            1
        );
        // 1500 us = 1.5 ms, rounds up to 2 ticks
        assert_eq!(
            duration_to_ticks(Duration::from_micros(1500), 1_000_000).unwrap(),
            2
        );
        assert_eq!(
            duration_to_ticks(Duration::from_millis(3), 1_000_000).unwrap(),
            3
        );
    }

    fn contains_zero() -> String {
        // helper because Display text is checked elsewhere
        String::from("duration 0 ns is outside the wheel's representable range")
    }

    #[test]
    fn end_to_end_fire_and_drain() {
        let mut wheel = TimerWheelBuilder::new()
            .shards(2)
            .tick_duration(1_000_000)
            .capacity_per_shard(64)
            .spawn()
            .expect("spawn");
        let _ = wheel.start(123, Duration::from_millis(5)).expect("start");
        // wait long enough for the worker to fire
        let deadline = Instant::now() + Duration::from_millis(80);
        let mut fired = Vec::new();
        while Instant::now() < deadline && fired.is_empty() {
            std::thread::sleep(Duration::from_millis(5));
            fired = wheel.drain_expired();
        }
        wheel.shutdown();
        assert_eq!(fired, vec![123]);
    }

    #[test]
    fn cancel_suppresses_fire() {
        let mut wheel = TimerWheelBuilder::new()
            .shards(1)
            .tick_duration(1_000_000)
            .spawn()
            .expect("spawn");
        let h = wheel.start(7, Duration::from_millis(40)).expect("start");
        wheel.cancel(h).expect("cancel");
        std::thread::sleep(Duration::from_millis(70));
        let fired = wheel.drain_expired();
        wheel.shutdown();
        assert!(fired.is_empty(), "cancelled timer fired: {fired:?}");
    }

    #[test]
    fn handler_is_invoked_on_fire() {
        static FIRED: AtomicUsize = AtomicUsize::new(0);
        struct Counting;
        impl TimerHandler for Counting {
            fn on_fire(&self, _id: TaskId) {
                FIRED.fetch_add(1, Ordering::SeqCst);
            }
        }
        FIRED.store(0, Ordering::SeqCst);
        let mut wheel = TimerWheelBuilder::new()
            .shards(1)
            .tick_duration(1_000_000)
            .handler(Arc::new(Counting))
            .spawn()
            .expect("spawn");
        let _ = wheel.start(1, Duration::from_millis(3)).expect("start");
        std::thread::sleep(Duration::from_millis(30));
        let _ = wheel.drain_expired();
        wheel.shutdown();
        assert!(FIRED.load(Ordering::SeqCst) >= 1);
    }

    #[test]
    fn capacity_gate_rejects_when_full() {
        let mut wheel = TimerWheelBuilder::new()
            .shards(1)
            .tick_duration(1_000_000)
            .capacity_per_shard(4)
            .spawn()
            .expect("spawn");
        // fill the slab (outstanding gate)
        for i in 1..=4u64 {
            let _ = wheel.start(i, Duration::from_secs(60)).expect("start");
        }
        // 5th must be rejected synchronously
        match wheel.start(99, Duration::from_secs(60)) {
            Err(TimerError::WheelFull { .. }) => {}
            other => panic!("expected WheelFull, got {other:?}"),
        }
        wheel.shutdown();
    }

    #[test]
    fn shutdown_is_idempotent() {
        let mut wheel = TimerWheelBuilder::new().shards(1).spawn().expect("spawn");
        wheel.shutdown();
        wheel.shutdown();
    }

    #[test]
    fn invalid_config_rejected_at_spawn() {
        let res = TimerWheelBuilder::new().bits_per_level(0).spawn();
        assert!(matches!(res, Err(TimerError::InvalidConfig(_))));
    }
}

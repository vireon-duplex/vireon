//! Shared helpers for all `timer-wheel` benchmark binaries.
//!
//! Each `[[bench]]` file starts with:
//!
//! ```rust,ignore
//! #![allow(clippy::expect_used, clippy::unwrap_used)]
//! #[path = "common.rs"]
//! mod common;
//! ```
//!
//! This keeps workload data, the PRNG, the wheel/shard builders, and the
//! seed routines in one place.

// Not every bench binary uses every constant/function in this module.
#![allow(dead_code)]
// Bench helper code uses expect/unwrap for brevity (allowed in benches).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use timer_wheel::scheduler::clock::Clock;
use timer_wheel::scheduler::shard::{Shard, ShardChannels, TimerCmd};
use timer_wheel::{TimerWheel, TimerWheelBuilder};

// ============================================================================
// Workload sizes
// ============================================================================

/// Bulk-operation sweep sizes (1k, 10k, 100k, 1M timers).
pub const BULK_SIZES: [usize; 4] = [1_000, 10_000, 100_000, 1_000_000];

/// Expire-drain sweep sizes (matches the user-facing 100/1k/100k request).
pub const EXPIRE_SIZES: [usize; 3] = [100, 1_000, 100_000];

/// Tick-cost sweep sizes (sparse — 1, 1k, 100k — to anchor the curve).
pub const TICK_SIZES: [usize; 3] = [1, 1_000, 100_000];

/// Realistic QUIC/Vireon timer deadlines (deadline, label).
pub const QUIC_DEADLINES: &[(Duration, &str)] = &[
    (Duration::from_millis(1), "ack_1ms"),
    (Duration::from_millis(10), "pto_10ms"),
    (Duration::from_millis(100), "idle_100ms"),
    (Duration::from_secs(30), "retransmit_30s"),
    (Duration::from_secs(300), "close_5m"),
];

/// Default per-shard slab capacity used by bench fixtures.
pub const DEFAULT_BENCH_CAPACITY: usize = 1 << 21; // 2M timers; enough for the 1M sweep.

// ============================================================================
// PRNG
// ============================================================================

/// Deterministic, allocation-free `u64` generator (XorShift64).
///
/// Seeded from a fixed constant so every bench run reproduces the same
/// workload — no hidden cross-correlation with `Instant`-based seeding.
#[derive(Clone, Copy)]
pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    /// Fresh PRNG with a fixed, non-zero seed.
    #[must_use]
    pub const fn new() -> Self {
        // Non-zero starting state (any odd 64-bit constant works).
        Self {
            state: 0x9E37_79B9_7F4A_7C15,
        }
    }

    /// Returns the next pseudo-random `u64`.
    #[inline]
    #[must_use]
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Returns a pseudo-random `usize` in `[0, bound)`.
    #[inline]
    #[must_use]
    pub fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            return 0;
        }
        (self.next_u64() as usize) % bound
    }
}

impl Default for XorShift64 {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Wheel / shard builders
// ============================================================================

/// Builds a single shard sized to hold at least `capacity` timers, with no
/// worker thread spawned. The caller drives `Shard::tick()` manually.
///
/// `ShardChannels` is returned alongside so the bench can push commands
/// through the real lock-free MPSC path (mirrors what the facade does).
///
/// The command queue is sized to `capacity` (rounded up to a power of two) so
/// bulk benches can push `capacity` commands before any `tick()` without
/// overflowing the bounded MPSC.
#[must_use]
pub fn isolated_shard(capacity: usize) -> (Shard, ShardChannels, Arc<Clock>) {
    let clock = Clock::shared(1_000_000); // 1 ms tick
    let queue_cap = capacity.max(4096).next_power_of_two();
    let (shard, channels) = Shard::new(
        0,
        6, // bits_per_level
        4, // levels
        capacity,
        queue_cap, // cmd queue sized to capacity
        queue_cap, // expired queue likewise
        Arc::clone(&clock),
    )
    .expect("shard build");
    (shard, channels, clock)
}

/// Builds a full facade with `shards` worker threads, each sized to
/// `capacity_per_shard`. Used for facade-level (end-to-end) benches.
///
/// The command queue is sized generously (`1 << 15` = 32 768 per shard) so
/// the bench's tight start-loop cannot overflow the MPSC before the worker
/// drains it.
#[must_use]
pub fn live_wheel(shards: u32, capacity_per_shard: usize) -> TimerWheel {
    TimerWheelBuilder::new()
        .shards(shards)
        .tick_duration(1_000_000)
        .capacity_per_shard(capacity_per_shard)
        .cmd_queue_capacity(1 << 15)
        .spawn()
        .expect("wheel spawn")
}

// ============================================================================
// Seeders (setup-only; never inside the measured routine)
// ============================================================================

/// Pushes `n` `Start` commands into a shard via the real MPSC path and
/// advances the wheel by one tick so the worker-side state absorbs them.
/// Returns the slot indices the worker assigned (only valid until the
/// timers fire or are freed).
pub fn seed_shard(shard: &mut Shard, channels: &ShardChannels, n: usize, deadline_ticks: u64) {
    channels
        .outstanding
        .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
    for i in 1..=n as u64 {
        channels
            .cmd_tx
            .push(TimerCmd::Start {
                task_id: i,
                deadline_ticks,
            })
            .expect("cmd queue full during seed");
    }
    // Drain the commands into the wheel state.
    shard.tick();
}

/// Schedules `n` timers via the facade; returns the handles in insertion
/// order so the bench can cancel them deterministically.
pub fn seed_timers(
    wheel: &TimerWheel,
    n: usize,
    duration: Duration,
) -> Vec<timer_wheel::TimerHandle> {
    let mut handles = Vec::with_capacity(n);
    for i in 1..=n as u64 {
        handles.push(wheel.start(i, duration).expect("start during seed"));
    }
    handles
}

/// Anti-dead-call-elimination helper — sinks a value into the optimiser's
/// "used" set so the call producing it cannot be hoisted out of a loop.
#[inline]
pub fn sink<T>(t: T) {
    black_box(t);
}

// ============================================================================
// Linux memory stats (best-effort)
// ============================================================================

/// Returns `(VmPeak_kb, VmRSS_kb)` from `/proc/self/status` on Linux.
///
/// Returns `None` on any platform without `/proc/self/status` or on read
/// failure — callers should treat the absence as "measurement unavailable"
/// rather than as an error.
#[must_use]
pub fn proc_vm_stats() -> Option<(u64, u64)> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let mut peak: Option<u64> = None;
    let mut rss: Option<u64> = None;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmPeak:") {
            peak = rest.trim().trim_end_matches(" kB").parse::<u64>().ok();
        } else if let Some(rest) = line.strip_prefix("VmRSS:") {
            rss = rest.trim().trim_end_matches(" kB").parse::<u64>().ok();
        }
    }
    Some((peak?, rss?))
}

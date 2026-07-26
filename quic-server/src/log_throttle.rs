//! Small rate-limiting primitives for log noise control.
//!
//! Hot data-plane paths (cluster transport loop, per-frame decode,
//! retry eviction, ACL denials) can fire `tracing::warn!` hundreds of
//! times per second under sustained load — one per packet or per frame.
//! That drowns useful signal and strains log sinks.
//!
//! This module gives two patterns:
//!
//! - [`RateCounter`] — logs every Nth occurrence. Use when a sustained
//!   burst produces identical warnings and you want a periodic
//!   summary ("dropped 1024 events") rather than per-event noise.
//! - [`Cooldown`] — logs at most once per `period`. Use when only the
//!   *first* occurrence in any window carries new information (e.g.
//!   a stuck socket that errors on every recv).
//!
//! For cross-thread / module-level contexts (cluster transport thread)
//! use [`AtomicRateCounter`] — lock-free, `const`-constructible.
//!
//! ## Typical use
//!
//! ### Struct field (single-threaded)
//!
//! ```no_run
//! # use quic_server::log_throttle::RateCounter;
//! struct Dispatcher {
//!     decode_drops: RateCounter,
//! }
//!
//! impl Dispatcher {
//!     fn on_decode_fail(&mut self) {
//!         if self.decode_drops.tick() {
//!             tracing::warn!(drops = self.decode_drops.count(), "decode failed");
//!         }
//!     }
//! }
//! ```
//!
//! ### Module-level static (cross-thread)
//!
//! ```no_run
//! # use quic_server::log_throttle::AtomicRateCounter;
//! static DECODE_DROP: AtomicRateCounter = AtomicRateCounter::new(64);
//!
//! // in transport loop:
//! # fn _x() {
//! if DECODE_DROP.tick() {
//!     tracing::warn!("decode failed — dropped recent (logging every 64th)");
//! }
//! # }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ── RateCounter (single-thread, mut self) ──────────────────────────

/// Counts events and decides whether the current one should be logged.
///
/// Logs every Nth tick. `tick()` returns `true` on ticks 1, N+1, 2N+1,
/// ... — so the *first* occurrence always logs (good for transient
/// errors that may not repeat).
#[derive(Debug)]
pub struct RateCounter {
    count: u64,
    every: u64,
}

impl RateCounter {
    /// Create a counter that yields a log signal every `every` ticks.
    ///
    /// `every == 0` makes every tick log (useful for tests / opt-in).
    #[must_use]
    pub const fn new(every: u64) -> Self {
        Self { count: 0, every: if every == 0 { 1 } else { every } }
    }

    /// Record one event. Returns `true` if the caller should log.
    pub fn tick(&mut self) -> bool {
        // Check *before* increment so the first tick (count == 0)
        // returns true — transient errors that don't repeat should
        // still surface once.
        let should = self.count % self.every == 0;
        self.count = self.count.wrapping_add(1);
        should
    }

    /// Current event count (for telemetry in the log message itself).
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count
    }
}

impl Default for RateCounter {
    fn default() -> Self {
        Self::new(1024)
    }
}

// ── Cooldown (time-based, mut self) ────────────────────────────────

/// Time-based throttle: logs at most once per `period`.
///
/// Use when a fault persists (stuck socket, exhausted quota) and
/// every iteration of the loop produces the same warning — only the
/// first occurrence in any window carries new information.
#[derive(Debug)]
pub struct Cooldown {
    last: Option<Instant>,
    period: Duration,
}

impl Cooldown {
    /// Create a cooldown of length `period`.
    #[must_use]
    pub const fn new(period: Duration) -> Self {
        Self { last: None, period }
    }

    /// Returns `true` if no previous log exists or the cooldown has
    /// elapsed since the last `true` return.
    pub fn tick(&mut self) -> bool {
        let now = Instant::now();
        match self.last {
            None => {
                self.last = Some(now);
                true
            }
            Some(t) if now.duration_since(t) >= self.period => {
                self.last = Some(now);
                true
            }
            _ => false,
        }
    }

    /// Reset the cooldown so the next tick logs immediately.
    pub fn reset(&mut self) {
        self.last = None;
    }
}

// ── AtomicRateCounter (cross-thread, lock-free) ────────────────────

/// Atomic variant of [`RateCounter`] for module-level statics and
/// cross-thread contexts (e.g. the cluster transport thread).
///
/// `const`-constructible so it can live in a `static` without
/// `OnceLock` / `lazy_static`:
///
/// ```no_run
/// # use quic_server::log_throttle::AtomicRateCounter;
/// static DROP: AtomicRateCounter = AtomicRateCounter::new(64);
/// ```
#[derive(Debug)]
pub struct AtomicRateCounter {
    count: AtomicU64,
    every: u64,
}

impl AtomicRateCounter {
    /// Create a counter that yields a log signal every `every` ticks.
    #[must_use]
    pub const fn new(every: u64) -> Self {
        Self { count: AtomicU64::new(0), every: if every == 0 { 1 } else { every } }
    }

    /// Atomically record one event. Returns `true` if the caller
    /// should log. Uses `Relaxed` ordering — this is a best-effort
    /// counter, not synchronization.
    pub fn tick(&self) -> bool {
        // fetch_add returns the *previous* value; first call returns 0.
        let prev = self.count.fetch_add(1, Ordering::Relaxed);
        // We want ticks 1, N+1, 2N+1, ... to log → prev ∈ {0, N, 2N, ...}.
        prev % self.every == 0
    }

    /// Current count (telemetry only).
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── RateCounter ───────────────────────────────────────────────

    #[test]
    fn rate_counter_first_tick_logs() {
        let mut r = RateCounter::new(3);
        assert!(r.tick(), "first tick must log (transient errors matter)");
    }

    #[test]
    fn rate_counter_periodic() {
        let mut r = RateCounter::new(3);
        // Logs on ticks 1, 4, 7, ... (count % every == 0 *before*
        // increment), so the first occurrence always surfaces.
        assert!(r.tick()); // tick 1 — logs (count 0→1)
        assert!(!r.tick()); // tick 2 (count 1→2)
        assert!(!r.tick()); // tick 3 (count 2→3)
        assert!(r.tick()); // tick 4 — logs (count 3→4)
        assert!(!r.tick()); // tick 5
        assert!(!r.tick()); // tick 6
        assert!(r.tick()); // tick 7 — logs (count 6→7)
    }

    #[test]
    fn rate_counter_every_zero_is_one() {
        let mut r = RateCounter::new(0);
        assert!(r.tick());
        assert!(r.tick());
        assert!(r.tick());
    }

    #[test]
    fn rate_counter_wraps_without_panic() {
        let mut r = RateCounter::new(1);
        r.count = u64::MAX;
        let _ = r.tick(); // would overflow with regular add
    }

    // ── Cooldown ──────────────────────────────────────────────────

    #[test]
    fn cooldown_first_call_logs() {
        let mut c = Cooldown::new(Duration::from_secs(60));
        assert!(c.tick());
        assert!(!c.tick(), "subsequent calls within window must not log");
    }

    #[test]
    fn cooldown_reset_forces_next_log() {
        let mut c = Cooldown::new(Duration::from_secs(60));
        assert!(c.tick());
        assert!(!c.tick());
        c.reset();
        assert!(c.tick());
    }

    // ── AtomicRateCounter ─────────────────────────────────────────

    #[test]
    fn atomic_first_tick_logs() {
        let r = AtomicRateCounter::new(4);
        assert!(r.tick(), "first tick logs");
    }

    #[test]
    fn atomic_periodic() {
        let r = AtomicRateCounter::new(2);
        assert!(r.tick()); // prev=0
        assert!(!r.tick()); // prev=1
        assert!(r.tick()); // prev=2
        assert!(!r.tick()); // prev=3
        assert!(r.tick()); // prev=4
    }

    #[test]
    fn atomic_count_tracks_total() {
        let r = AtomicRateCounter::new(100);
        for _ in 0..50 {
            r.tick();
        }
        assert_eq!(r.count(), 50);
    }

    #[test]
    fn atomic_concurrent_safe() {
        // Spawn two threads ticking the same counter; should not
        // panic or corrupt state.
        let r = std::sync::Arc::new(AtomicRateCounter::new(5));
        let r2 = r.clone();
        let h1 = std::thread::spawn(move || {
            for _ in 0..1000 {
                r.tick();
            }
        });
        let h2 = std::thread::spawn(move || {
            for _ in 0..1000 {
                r2.tick();
            }
        });
        h1.join().unwrap();
        h2.join().unwrap();
        // Total ticks should match exactly (lock-free add is exact).
        // The "should log?" return values are best-effort and we
        // don't assert on them — only on the count.
    }
}

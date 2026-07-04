//! Monotonic tick source (Layer 7).
//!
//! [`Clock`] converts `std::time::Instant` deltas into wheel ticks. All shards
//! in a wheel share one clock (via `Arc<Clock>`) so their tick counters stay
//! aligned and deadlines are portable across shards.

use std::sync::Arc;
use std::time::{Duration, Instant};

/// A monotonic time source that maps wall progress to discrete ticks.
#[derive(Debug)]
pub struct Clock {
    epoch: Instant,
    tick_ns: u64,
}

impl Clock {
    /// Creates a clock whose tick counter starts at zero "now".
    #[must_use]
    pub fn new(tick_ns: u64) -> Self {
        Self {
            epoch: Instant::now(),
            tick_ns,
        }
    }

    /// Wraps the clock in an `Arc` for sharing across shards.
    #[must_use]
    pub fn shared(tick_ns: u64) -> Arc<Self> {
        Arc::new(Self::new(tick_ns))
    }

    /// Nanoseconds per tick.
    #[inline]
    #[must_use]
    pub fn tick_ns(&self) -> u64 {
        self.tick_ns
    }

    /// Current tick count (monotonically non-decreasing).
    #[inline]
    #[must_use]
    pub fn now_ticks(&self) -> u64 {
        let elapsed_ns = self.epoch.elapsed().as_nanos();
        (elapsed_ns / u128::from(self.tick_ns)) as u64
    }

    /// Duration to sleep until `target_ticks` arrives (zero if already past).
    ///
    /// Saturates to `Duration::MAX`-like cap (one hour) so huge horizons never
    /// overflow the `Duration` representation. Callers that need a tighter cap
    /// (e.g. the worker's idle interval) clamp the result themselves.
    #[must_use]
    pub fn sleep_until(&self, target_ticks: u64) -> Duration {
        let now = self.now_ticks();
        if target_ticks <= now {
            return Duration::ZERO;
        }
        let delta_ticks = target_ticks - now;
        let delta_ns = u128::from(delta_ticks) * u128::from(self.tick_ns);
        // Cap at one hour per park; the worker re-evaluates after waking.
        const ONE_HOUR_NS: u128 = 3_600_000_000_000;
        let capped = delta_ns.min(ONE_HOUR_NS);
        Duration::from_nanos(capped as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_ticks_advances_with_real_time() {
        let clock = Clock::new(1_000); // 1 us tick
        let t0 = clock.now_ticks();
        std::thread::sleep(Duration::from_millis(2));
        let t1 = clock.now_ticks();
        assert!(t1 > t0, "ticks did not advance: {t0} -> {t1}");
    }

    #[test]
    fn sleep_until_past_target_is_zero() {
        let clock = Clock::new(1_000_000);
        let now = clock.now_ticks();
        assert_eq!(clock.sleep_until(now.saturating_sub(5)), Duration::ZERO);
        assert_eq!(clock.sleep_until(now), Duration::ZERO);
    }

    #[test]
    fn sleep_until_future_target_is_positive() {
        let clock = Clock::new(1_000_000); // 1 ms tick
        let now = clock.now_ticks();
        let d = clock.sleep_until(now + 10);
        // ~10 ms, allow scheduling slack
        assert!(d >= Duration::from_millis(5), "sleep too short: {d:?}");
        assert!(d <= Duration::from_millis(15), "sleep too long: {d:?}");
    }

    #[test]
    fn sleep_until_caps_at_one_hour_for_huge_horizons() {
        let clock = Clock::new(1_000_000);
        let d = clock.sleep_until(u64::MAX / 2);
        assert!(
            d <= Duration::from_secs(3601),
            "exceeded one-hour cap: {d:?}"
        );
    }
}

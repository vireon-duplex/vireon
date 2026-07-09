//! [`SchedulerHookStats`], [`StatsSnapshot`] — atomic counters for
//! scheduler hook observability.
//!
//! See `DESIGN.md` Part 12 (observability).
//!
//! # Design
//!
//! Counters are stored as `AtomicU64` with `Relaxed` ordering:
//! stats are advisory, never load-bearing, so we trade
//! synchronization cost for throughput. Snapshots are taken on
//! demand.

use std::sync::atomic::{AtomicU64, Ordering};

// ── SchedulerHookStats ─────────────────────────────────────────────

/// Atomic counters for the scheduler hooks system.
#[derive(Debug)]
pub struct SchedulerHookStats {
    /// Total hooks registered.
    pub hooks_registered: AtomicU64,
    /// Total hook invocations.
    pub hooks_invoked: AtomicU64,
    /// Total hook failures.
    pub hooks_failed: AtomicU64,
    /// Total hook panics.
    pub hook_panics: AtomicU64,
    /// Total time spent in hooks (nanoseconds).
    pub total_hook_time_ns: AtomicU64,
    /// Total ticks processed.
    pub tick_count: AtomicU64,
}

impl Default for SchedulerHookStats {
    fn default() -> Self {
        Self::new()
    }
}

impl SchedulerHookStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            hooks_registered: AtomicU64::new(0),
            hooks_invoked: AtomicU64::new(0),
            hooks_failed: AtomicU64::new(0),
            hook_panics: AtomicU64::new(0),
            total_hook_time_ns: AtomicU64::new(0),
            tick_count: AtomicU64::new(0),
        }
    }

    /// Record a hook registration.
    pub fn record_hook_registered(&self) {
        self.hooks_registered.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a hook invocation.
    pub fn record_hook_invoked(&self) {
        self.hooks_invoked.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a hook failure.
    pub fn record_hook_failed(&self) {
        self.hooks_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a hook panic.
    pub fn record_hook_panic(&self) {
        self.hook_panics.fetch_add(1, Ordering::Relaxed);
    }

    /// Record time spent in hooks.
    pub fn record_hook_time_ns(&self, ns: u64) {
        self.total_hook_time_ns.fetch_add(ns, Ordering::Relaxed);
    }

    /// Record a tick.
    pub fn record_tick(&self) {
        self.tick_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            hooks_registered: self.hooks_registered.load(Ordering::Relaxed),
            hooks_invoked: self.hooks_invoked.load(Ordering::Relaxed),
            hooks_failed: self.hooks_failed.load(Ordering::Relaxed),
            hook_panics: self.hook_panics.load(Ordering::Relaxed),
            total_hook_time_ns: self.total_hook_time_ns.load(Ordering::Relaxed),
            tick_count: self.tick_count.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ─────────────────────────────────────────────────

/// Point-in-time snapshot of scheduler hook counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total hooks registered.
    pub hooks_registered: u64,
    /// Total hook invocations.
    pub hooks_invoked: u64,
    /// Total hook failures.
    pub hooks_failed: u64,
    /// Total hook panics.
    pub hook_panics: u64,
    /// Total time spent in hooks (nanoseconds).
    pub total_hook_time_ns: u64,
    /// Total ticks processed.
    pub tick_count: u64,
}

impl StatsSnapshot {
    /// Failure rate in basis points (0..=10_000).
    /// Returns 0 when there are no invocations.
    #[must_use]
    pub const fn failure_rate_bps(&self) -> u16 {
        if self.hooks_invoked == 0 {
            return 0;
        }
        let total_failures = self.hooks_failed.saturating_add(self.hook_panics);
        let rate = (total_failures * 10_000) / self.hooks_invoked;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Average hook time in nanoseconds.
    /// Returns 0 when there are no invocations.
    #[must_use]
    pub const fn avg_hook_time_ns(&self) -> u64 {
        if self.hooks_invoked == 0 {
            return 0;
        }
        self.total_hook_time_ns / self.hooks_invoked
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = SchedulerHookStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_hook_registered() {
        let s = SchedulerHookStats::new();
        s.record_hook_registered();
        s.record_hook_registered();
        let snap = s.snapshot();
        assert_eq!(snap.hooks_registered, 2);
    }

    #[test]
    fn record_hook_invoked() {
        let s = SchedulerHookStats::new();
        s.record_hook_invoked();
        s.record_hook_invoked();
        let snap = s.snapshot();
        assert_eq!(snap.hooks_invoked, 2);
    }

    #[test]
    fn record_hook_failed() {
        let s = SchedulerHookStats::new();
        s.record_hook_failed();
        let snap = s.snapshot();
        assert_eq!(snap.hooks_failed, 1);
    }

    #[test]
    fn record_hook_panic() {
        let s = SchedulerHookStats::new();
        s.record_hook_panic();
        s.record_hook_panic();
        let snap = s.snapshot();
        assert_eq!(snap.hook_panics, 2);
    }

    #[test]
    fn record_hook_time_ns() {
        let s = SchedulerHookStats::new();
        s.record_hook_time_ns(1000);
        s.record_hook_time_ns(2000);
        let snap = s.snapshot();
        assert_eq!(snap.total_hook_time_ns, 3000);
    }

    #[test]
    fn record_tick() {
        let s = SchedulerHookStats::new();
        s.record_tick();
        let snap = s.snapshot();
        assert_eq!(snap.tick_count, 1);
    }

    #[test]
    fn failure_rate_with_no_invocations_is_zero() {
        let s = SchedulerHookStats::new();
        assert_eq!(s.snapshot().failure_rate_bps(), 0);
    }

    #[test]
    fn failure_rate_with_mixed_results() {
        let s = SchedulerHookStats::new();
        // 100 invocations, 5 failures, 1 panic
        for _ in 0..100 {
            s.record_hook_invoked();
        }
        for _ in 0..5 {
            s.record_hook_failed();
        }
        s.record_hook_panic();

        // (5 + 1) / 100 = 6% = 600 bps
        assert_eq!(s.snapshot().failure_rate_bps(), 600);
    }

    #[test]
    fn failure_rate_all_failures() {
        let s = SchedulerHookStats::new();
        for _ in 0..10 {
            s.record_hook_invoked();
            s.record_hook_failed();
        }
        // 10 / 10 = 100% = 10_000 bps
        assert_eq!(s.snapshot().failure_rate_bps(), 10_000);
    }

    #[test]
    fn avg_hook_time_with_no_invocations_is_zero() {
        let s = SchedulerHookStats::new();
        assert_eq!(s.snapshot().avg_hook_time_ns(), 0);
    }

    #[test]
    fn avg_hook_time_with_multiple_invocations() {
        let s = SchedulerHookStats::new();
        s.record_hook_invoked();
        s.record_hook_time_ns(1000);
        s.record_hook_invoked();
        s.record_hook_time_ns(2000);
        s.record_hook_invoked();
        s.record_hook_time_ns(3000);

        // (1000 + 2000 + 3000) / 3 = 2000
        assert_eq!(s.snapshot().avg_hook_time_ns(), 2000);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = SchedulerHookStats::new();
        s.record_hook_invoked();
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn snapshot_does_not_affect_counters() {
        let s = SchedulerHookStats::new();
        s.record_hook_registered();
        let snap1 = s.snapshot();
        let snap2 = s.snapshot();
        assert_eq!(snap1, snap2);
    }
}

//! [`ResourceQuotaStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for quota observability.
//!
//! See `DESIGN.md` Part 12 (observability).
//!
//! # Design
//!
//! Counters are stored as `AtomicU64` with `Relaxed` ordering:
//! stats are advisory, never load-bearing, so we trade
//! synchronization cost for throughput.

use std::sync::atomic::{AtomicU64, Ordering};

// ── ResourceQuotaStats ───────────────────────────────────────────

/// Atomic counters for the resource quotas system.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`ResourceQuotaStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct ResourceQuotaStats {
    /// Total quota checks performed (counter).
    pub checks_total: AtomicU64,
    /// Checks that passed (counter).
    pub checks_passed: AtomicU64,
    /// Checks that exceeded limits (counter).
    pub checks_exceeded: AtomicU64,
    /// Tenants currently tracked (gauge).
    pub tenants_tracked: AtomicU64,
    /// Sessions currently tracked across all tenants (gauge).
    pub sessions_tracked: AtomicU64,
    /// Total bandwidth across all tenants (bps, gauge).
    pub bandwidth_total_bps: AtomicU64,
}

impl Default for ResourceQuotaStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceQuotaStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            checks_total: AtomicU64::new(0),
            checks_passed: AtomicU64::new(0),
            checks_exceeded: AtomicU64::new(0),
            tenants_tracked: AtomicU64::new(0),
            sessions_tracked: AtomicU64::new(0),
            bandwidth_total_bps: AtomicU64::new(0),
        }
    }

    /// Record a quota check attempt.
    pub fn record_check(&self) {
        self.checks_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a quota check that passed.
    pub fn record_check_passed(&self) {
        self.checks_passed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a quota check that exceeded limits.
    pub fn record_check_exceeded(&self) {
        self.checks_exceeded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a tenant being tracked.
    pub fn record_tenant_tracked(&self) {
        self.tenants_tracked.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a tenant being removed.
    pub fn record_tenant_removed(&self) {
        self.tenants_tracked.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a session being tracked.
    pub fn record_session_tracked(&self) {
        self.sessions_tracked.fetch_add(1, Ordering::Relaxed);
    }

    /// Record bandwidth being used.
    pub fn record_bandwidth(&self, bps: u64) {
        self.bandwidth_total_bps.fetch_add(bps, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields. This is acceptable
    /// for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            checks_total: self.checks_total.load(Ordering::Relaxed),
            checks_passed: self.checks_passed.load(Ordering::Relaxed),
            checks_exceeded: self.checks_exceeded.load(Ordering::Relaxed),
            tenants_tracked: self.tenants_tracked.load(Ordering::Relaxed),
            sessions_tracked: self.sessions_tracked.load(Ordering::Relaxed),
            bandwidth_total_bps: self.bandwidth_total_bps.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ───────────────────────────────────────────────

/// Point-in-time snapshot of quota counters.
///
/// All rates are precomputed as basis-points (bps, 1/100 of a
/// percent) so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total quota checks performed.
    pub checks_total: u64,
    /// Checks that passed.
    pub checks_passed: u64,
    /// Checks that exceeded limits.
    pub checks_exceeded: u64,
    /// Tenants currently tracked.
    pub tenants_tracked: u64,
    /// Sessions currently tracked.
    pub sessions_tracked: u64,
    /// Total bandwidth across all tenants (bps).
    pub bandwidth_total_bps: u64,
}

impl StatsSnapshot {
    /// Exceed rate in basis points (0..=10_000).
    /// Returns 0 when there are no checks yet.
    #[must_use]
    pub const fn exceed_rate_bps(&self) -> u16 {
        if self.checks_total == 0 {
            return 0;
        }
        let rate = (self.checks_exceeded * 10_000) / self.checks_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = ResourceQuotaStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_check_increments_total() {
        let s = ResourceQuotaStats::new();
        s.record_check();
        s.record_check();
        let snap = s.snapshot();
        assert_eq!(snap.checks_total, 2);
    }

    #[test]
    fn record_check_passed_increments_passed() {
        let s = ResourceQuotaStats::new();
        s.record_check_passed();
        s.record_check_passed();
        let snap = s.snapshot();
        assert_eq!(snap.checks_passed, 2);
    }

    #[test]
    fn record_check_exceeded_increments_exceeded() {
        let s = ResourceQuotaStats::new();
        s.record_check_exceeded();
        s.record_check_exceeded();
        let snap = s.snapshot();
        assert_eq!(snap.checks_exceeded, 2);
    }

    #[test]
    fn record_tenant_tracked_increments_counter() {
        let s = ResourceQuotaStats::new();
        s.record_tenant_tracked();
        s.record_tenant_tracked();
        let snap = s.snapshot();
        assert_eq!(snap.tenants_tracked, 2);
    }

    #[test]
    fn record_tenant_removed_decrements_counter() {
        let s = ResourceQuotaStats::new();
        s.record_tenant_tracked();
        s.record_tenant_tracked();
        s.record_tenant_removed();
        let snap = s.snapshot();
        assert_eq!(snap.tenants_tracked, 1);
    }

    #[test]
    fn record_session_tracked_increments_counter() {
        let s = ResourceQuotaStats::new();
        s.record_session_tracked();
        let snap = s.snapshot();
        assert_eq!(snap.sessions_tracked, 1);
    }

    #[test]
    fn record_bandwidth_increments_counter() {
        let s = ResourceQuotaStats::new();
        s.record_bandwidth(1_000_000_000);
        s.record_bandwidth(2_000_000_000);
        let snap = s.snapshot();
        assert_eq!(snap.bandwidth_total_bps, 3_000_000_000);
    }

    #[test]
    fn exceed_rate_with_no_checks_is_zero() {
        let s = ResourceQuotaStats::new();
        assert_eq!(s.snapshot().exceed_rate_bps(), 0);
    }

    #[test]
    fn exceed_rate_with_mixed_results() {
        let s = ResourceQuotaStats::new();
        s.checks_total.store(1000, Ordering::Relaxed);
        s.checks_exceeded.store(50, Ordering::Relaxed);
        // 50/1000 = 5% = 500 bps.
        assert_eq!(s.snapshot().exceed_rate_bps(), 500);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = ResourceQuotaStats::new();
        s.record_check();
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }
}

//! [`MiddlewareStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for observability.
//!
//! See `DESIGN.md` Part 12 (observability).
//!
//! # Design
//!
//! Counters are stored as `AtomicU64` with `Relaxed` ordering:
//! stats are advisory, never load-bearing, so we trade
//! synchronization cost for throughput. Snapshots are taken on
//! demand and the snapshot is what gets exported to metrics.

use std::sync::atomic::{AtomicU64, Ordering};

// ── MiddlewareStats ─────────────────────────────────────────────

/// Atomic counters for the middleware pipeline system.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`MiddlewareStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct MiddlewareStats {
    /// Total pipeline executions (counter).
    pub executions_total: AtomicU64,
    /// Successful executions (counter).
    pub executions_succeeded: AtomicU64,
    /// Rejected executions (counter, short-circuit).
    pub executions_rejected: AtomicU64,
    /// Chain aborts (counter, mid-chain abort).
    pub chain_aborts: AtomicU64,
    /// Total time spent in pipeline (ns).
    pub total_time_ns: AtomicU64,
}

impl Default for MiddlewareStats {
    fn default() -> Self {
        Self::new()
    }
}

impl MiddlewareStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            executions_total: AtomicU64::new(0),
            executions_succeeded: AtomicU64::new(0),
            executions_rejected: AtomicU64::new(0),
            chain_aborts: AtomicU64::new(0),
            total_time_ns: AtomicU64::new(0),
        }
    }

    /// Record a pipeline execution start.
    pub fn record_execution(&self) {
        self.executions_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful execution.
    pub fn record_success(&self) {
        self.executions_succeeded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a rejected execution (short-circuit).
    pub fn record_rejected(&self) {
        self.executions_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a chain abort (mid-chain).
    pub fn record_abort(&self) {
        self.chain_aborts.fetch_add(1, Ordering::Relaxed);
    }

    /// Record execution time (ns).
    pub fn record_time(&self, ns: u64) {
        self.total_time_ns.fetch_add(ns, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields. This is acceptable
    /// for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            executions_total: self.executions_total.load(Ordering::Relaxed),
            executions_succeeded: self.executions_succeeded.load(Ordering::Relaxed),
            executions_rejected: self.executions_rejected.load(Ordering::Relaxed),
            chain_aborts: self.chain_aborts.load(Ordering::Relaxed),
            total_time_ns: self.total_time_ns.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ─────────────────────────────────────────────

/// Point-in-time snapshot of middleware counters.
///
/// All rates are precomputed as basis-points (bps, 1/100 of a
/// percent) so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total executions.
    pub executions_total: u64,
    /// Successful executions.
    pub executions_succeeded: u64,
    /// Rejected executions.
    pub executions_rejected: u64,
    /// Chain aborts.
    pub chain_aborts: u64,
    /// Total execution time (ns).
    pub total_time_ns: u64,
}

impl StatsSnapshot {
    /// Rejection rate in basis points (0..=10_000).
    /// Returns 0 when there are no executions.
    #[must_use]
    pub const fn rejection_rate_bps(&self) -> u16 {
        if self.executions_total == 0 {
            return 0;
        }
        let rate = (self.executions_rejected * 10_000) / self.executions_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Average execution time in ns.
    /// Returns 0 when no executions have occurred.
    #[must_use]
    pub const fn avg_time_ns(&self) -> u64 {
        if self.executions_total == 0 {
            return 0;
        }
        self.total_time_ns / self.executions_total
    }

    /// Success rate in basis points (0..=10_000).
    /// Returns 10_000 (100%) when no executions yet.
    #[must_use]
    pub const fn success_rate_bps(&self) -> u16 {
        if self.executions_total == 0 {
            return 10_000;
        }
        let rate = (self.executions_succeeded * 10_000) / self.executions_total;
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
        let s = MiddlewareStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_execution_increments_total() {
        let s = MiddlewareStats::new();
        s.record_execution();
        s.record_execution();
        let snap = s.snapshot();
        assert_eq!(snap.executions_total, 2);
    }

    #[test]
    fn record_success_increments_succeeded() {
        let s = MiddlewareStats::new();
        s.record_success();
        s.record_success();
        s.record_success();
        let snap = s.snapshot();
        assert_eq!(snap.executions_succeeded, 3);
    }

    #[test]
    fn record_rejected_increments_rejected() {
        let s = MiddlewareStats::new();
        s.record_rejected();
        let snap = s.snapshot();
        assert_eq!(snap.executions_rejected, 1);
    }

    #[test]
    fn record_abort_increments_aborts() {
        let s = MiddlewareStats::new();
        s.record_abort();
        s.record_abort();
        let snap = s.snapshot();
        assert_eq!(snap.chain_aborts, 2);
    }

    #[test]
    fn record_time_accumulates() {
        let s = MiddlewareStats::new();
        s.record_time(1000);
        s.record_time(2000);
        assert_eq!(s.snapshot().total_time_ns, 3000);
    }

    #[test]
    fn rejection_rate_is_zero_with_no_executions() {
        let s = MiddlewareStats::new();
        assert_eq!(s.snapshot().rejection_rate_bps(), 0);
    }

    #[test]
    fn rejection_rate_calculates_correctly() {
        let mut snap = StatsSnapshot::default();
        snap.executions_total = 100;
        snap.executions_rejected = 15; // 15% = 1500 bps.
        assert_eq!(snap.rejection_rate_bps(), 1_500);
    }

    #[test]
    fn rejection_rate_clamps_at_max() {
        let mut snap = StatsSnapshot::default();
        snap.executions_total = 1;
        snap.executions_rejected = 200_000; // Would overflow u16.
        assert_eq!(snap.rejection_rate_bps(), u16::MAX);
    }

    #[test]
    fn avg_time_is_zero_with_no_executions() {
        let s = MiddlewareStats::new();
        assert_eq!(s.snapshot().avg_time_ns(), 0);
    }

    #[test]
    fn avg_time_calculates_correctly() {
        let mut snap = StatsSnapshot::default();
        snap.executions_total = 4;
        snap.total_time_ns = 8000; // 8000 / 4 = 2000.
        assert_eq!(snap.avg_time_ns(), 2000);
    }

    #[test]
    fn success_rate_is_100_pct_with_no_executions() {
        let s = MiddlewareStats::new();
        assert_eq!(s.snapshot().success_rate_bps(), 10_000);
    }

    #[test]
    fn success_rate_calculates_correctly() {
        let mut snap = StatsSnapshot::default();
        snap.executions_total = 100;
        snap.executions_succeeded = 95; // 95% = 9500 bps.
        assert_eq!(snap.success_rate_bps(), 9_500);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = MiddlewareStats::new();
        s.record_execution();
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn snapshot_is_copy() {
        let s = MiddlewareStats::new();
        let snap1 = s.snapshot();
        let snap2 = snap1;
        assert_eq!(snap1.executions_total, snap2.executions_total);
    }
}

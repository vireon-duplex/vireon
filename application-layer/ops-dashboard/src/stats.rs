//! [`OpsDashboardStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for the ops-dashboard.
//!
//! See `DESIGN.md` Part 12 (observability).
//!
//! # Design
//!
//! Counters are stored as `AtomicU64` with `Relaxed` ordering:
//! stats are advisory, never load-bearing, so we trade
//! synchronization cost for throughput. Snapshots are taken on
//! demand and the snapshot is what gets exported to metrics.
//!
//! # Mapping to metrics
//!
//! Each counter maps 1:1 to a Prometheus counter or gauge in
//! Phase 2+:
//!
//! | Counter | Prometheus type | Name |
//! |---------|-----------------|------|
//! | `queries_total` | counter | `ops_dashboard_queries_total` |
//! | `queries_succeeded` | counter | `ops_dashboard_queries_succeeded_total` |
//! | `queries_failed` | counter | `ops_dashboard_queries_failed_total` |
//! | `queries_timed_out` | counter | `ops_dashboard_queries_timed_out_total` |
//! | `widgets_rendered` | counter | `ops_dashboard_widgets_rendered_total` |
//! | `data_points_fetched` | counter | `ops_dashboard_data_points_fetched_total` |

use std::sync::atomic::{AtomicU64, Ordering};

// ── OpsDashboardStats ───────────────────────────────────────────

/// Atomic counters for the ops-dashboard.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`OpsDashboardStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct OpsDashboardStats {
    /// Total queries attempted since startup (counter).
    pub queries_total: AtomicU64,
    /// Queries that succeeded (counter).
    pub queries_succeeded: AtomicU64,
    /// Queries that failed (counter).
    pub queries_failed: AtomicU64,
    /// Queries that timed out (counter).
    pub queries_timed_out: AtomicU64,
    /// Widgets rendered since startup (counter).
    pub widgets_rendered: AtomicU64,
    /// Data points fetched since startup (counter).
    pub data_points_fetched: AtomicU64,
}

impl Default for OpsDashboardStats {
    fn default() -> Self {
        Self::new()
    }
}

impl OpsDashboardStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            queries_total: AtomicU64::new(0),
            queries_succeeded: AtomicU64::new(0),
            queries_failed: AtomicU64::new(0),
            queries_timed_out: AtomicU64::new(0),
            widgets_rendered: AtomicU64::new(0),
            data_points_fetched: AtomicU64::new(0),
        }
    }

    /// Record a query attempt.
    pub fn record_query(&self) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful query.
    pub fn record_query_success(&self) {
        self.queries_succeeded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed query.
    pub fn record_query_failure(&self) {
        self.queries_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a timed-out query.
    pub fn record_query_timeout(&self) {
        self.queries_timed_out.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a widget being rendered.
    pub fn record_widget_render(&self) {
        self.widgets_rendered.fetch_add(1, Ordering::Relaxed);
    }

    /// Record data points being fetched.
    pub fn record_data_points(&self, count: u64) {
        self.data_points_fetched.fetch_add(count, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields. This is acceptable
    /// for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            queries_total: self.queries_total.load(Ordering::Relaxed),
            queries_succeeded: self.queries_succeeded.load(Ordering::Relaxed),
            queries_failed: self.queries_failed.load(Ordering::Relaxed),
            queries_timed_out: self.queries_timed_out.load(Ordering::Relaxed),
            widgets_rendered: self.widgets_rendered.load(Ordering::Relaxed),
            data_points_fetched: self.data_points_fetched.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ───────────────────────────────────────────────

/// Point-in-time snapshot of ops-dashboard counters.
///
/// All rates are precomputed as basis-points (bps, 1/100 of a
/// percent) so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total queries attempted since startup.
    pub queries_total: u64,
    /// Queries that succeeded.
    pub queries_succeeded: u64,
    /// Queries that failed.
    pub queries_failed: u64,
    /// Queries that timed out.
    pub queries_timed_out: u64,
    /// Widgets rendered since startup.
    pub widgets_rendered: u64,
    /// Data points fetched since startup.
    pub data_points_fetched: u64,
}

impl StatsSnapshot {
    /// Query success rate in basis points (0..=10_000).
    ///
    /// Returns 10_000 (100%) when there are no queries yet, since
    /// an idle system is considered healthy.
    #[must_use]
    pub const fn query_success_rate_bps(&self) -> u16 {
        if self.queries_total == 0 {
            return 10_000;
        }
        let rate = (self.queries_succeeded.saturating_mul(10_000)) / self.queries_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Query failure rate in basis points (0..=10_000).
    ///
    /// Returns 0 when there are no queries yet.
    #[must_use]
    pub const fn query_failure_rate_bps(&self) -> u16 {
        if self.queries_total == 0 {
            return 0;
        }
        let rate = (self.queries_failed.saturating_mul(10_000)) / self.queries_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Query timeout rate in basis points (0..=10_000).
    ///
    /// Returns 0 when there are no queries yet.
    #[must_use]
    pub const fn query_timeout_rate_bps(&self) -> u16 {
        if self.queries_total == 0 {
            return 0;
        }
        let rate = (self.queries_timed_out.saturating_mul(10_000)) / self.queries_total;
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
        let s = OpsDashboardStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_query_increments_counter() {
        let s = OpsDashboardStats::new();
        s.record_query();
        s.record_query();
        assert_eq!(s.snapshot().queries_total, 2);
    }

    #[test]
    fn record_query_success_and_failure() {
        let s = OpsDashboardStats::new();
        s.record_query();
        s.record_query();
        s.record_query();
        s.record_query_success();
        s.record_query_failure();
        let snap = s.snapshot();
        assert_eq!(snap.queries_total, 3);
        assert_eq!(snap.queries_succeeded, 1);
        assert_eq!(snap.queries_failed, 1);
    }

    #[test]
    fn record_query_timeout() {
        let s = OpsDashboardStats::new();
        s.record_query();
        s.record_query_timeout();
        assert_eq!(s.snapshot().queries_timed_out, 1);
    }

    #[test]
    fn record_widget_render() {
        let s = OpsDashboardStats::new();
        s.record_widget_render();
        s.record_widget_render();
        s.record_widget_render();
        assert_eq!(s.snapshot().widgets_rendered, 3);
    }

    #[test]
    fn record_data_points() {
        let s = OpsDashboardStats::new();
        s.record_data_points(100);
        s.record_data_points(250);
        assert_eq!(s.snapshot().data_points_fetched, 350);
    }

    #[test]
    fn success_rate_with_no_queries_is_max() {
        let snap = StatsSnapshot::default();
        assert_eq!(snap.query_success_rate_bps(), 10_000);
    }

    #[test]
    fn success_rate_all_succeeded() {
        let s = OpsDashboardStats::new();
        for _ in 0..100 {
            s.record_query();
            s.record_query_success();
        }
        assert_eq!(s.snapshot().query_success_rate_bps(), 10_000);
    }

    #[test]
    fn success_rate_mixed_results() {
        // 97 succeeded out of 100 = 97% = 9700 bps.
        let s = OpsDashboardStats::new();
        for _ in 0..97 {
            s.record_query();
            s.record_query_success();
        }
        for _ in 0..3 {
            s.record_query();
            s.record_query_failure();
        }
        // 97 * 10000 / 100 = 9700
        assert_eq!(s.snapshot().query_success_rate_bps(), 9700);
    }

    #[test]
    fn success_rate_truncates_not_rounds() {
        // 2 succeeded out of 3 = 6666 bps (truncated, not 6667).
        let s = OpsDashboardStats::new();
        s.record_query();
        s.record_query();
        s.record_query();
        s.record_query_success();
        s.record_query_success();
        assert_eq!(s.snapshot().query_success_rate_bps(), 6666);
    }

    #[test]
    fn failure_rate_with_no_queries_is_zero() {
        let snap = StatsSnapshot::default();
        assert_eq!(snap.query_failure_rate_bps(), 0);
    }

    #[test]
    fn failure_rate_mixed_results() {
        let s = OpsDashboardStats::new();
        for _ in 0..95 {
            s.record_query();
            s.record_query_success();
        }
        for _ in 0..5 {
            s.record_query();
            s.record_query_failure();
        }
        // 5 * 10000 / 100 = 500
        assert_eq!(s.snapshot().query_failure_rate_bps(), 500);
    }

    #[test]
    fn timeout_rate_with_no_queries_is_zero() {
        let snap = StatsSnapshot::default();
        assert_eq!(snap.query_timeout_rate_bps(), 0);
    }

    #[test]
    fn timeout_rate_mixed_results() {
        let s = OpsDashboardStats::new();
        for _ in 0..99 {
            s.record_query();
            s.record_query_success();
        }
        s.record_query();
        s.record_query_timeout();
        // 1 * 10000 / 100 = 100
        assert_eq!(s.snapshot().query_timeout_rate_bps(), 100);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = OpsDashboardStats::new();
        s.record_query();
        s.record_query_success();
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }
}

//! [`ObservabilityStats`], [`StatsSnapshot`] — atomic counters and a
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
//!
//! # Mapping to metrics
//!
//! Each counter maps 1:1 to a Prometheus counter or gauge in
//! Phase 2+:
//!
//! | Counter | Prometheus type | Name |
//! |---------|-----------------|------|
//! | `metrics_recorded` | counter | `observability_metrics_recorded_total` |
//! | `spans_opened` | counter | `observability_spans_opened_total` |
//! | `spans_closed` | counter | `observability_spans_closed_total` |
//! | `logs_emitted` | counter | `observability_logs_emitted_total` |
//! | `exports_total` | counter | `observability_exports_total` |
//! | `exports_failed` | counter | `observability_exports_failed_total` |

use std::sync::atomic::{AtomicU64, Ordering};

// ── ObservabilityStats ───────────────────────────────────────────

/// Atomic counters for the observability SDK.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`ObservabilityStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct ObservabilityStats {
    /// Metrics recorded since startup (counter).
    pub metrics_recorded: AtomicU64,
    /// Spans opened since startup (counter).
    pub spans_opened: AtomicU64,
    /// Spans closed since startup (counter).
    pub spans_closed: AtomicU64,
    /// Log records emitted since startup (counter).
    pub logs_emitted: AtomicU64,
    /// Export operations attempted (counter).
    pub exports_total: AtomicU64,
    /// Export operations that failed (counter).
    pub exports_failed: AtomicU64,
}

impl Default for ObservabilityStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ObservabilityStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            metrics_recorded: AtomicU64::new(0),
            spans_opened: AtomicU64::new(0),
            spans_closed: AtomicU64::new(0),
            logs_emitted: AtomicU64::new(0),
            exports_total: AtomicU64::new(0),
            exports_failed: AtomicU64::new(0),
        }
    }

    /// Record a metric being recorded.
    pub fn record_metric(&self) {
        self.metrics_recorded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a span being opened.
    pub fn record_span_open(&self) {
        self.spans_opened.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a span being closed.
    pub fn record_span_close(&self) {
        self.spans_closed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a log being emitted.
    pub fn record_log(&self) {
        self.logs_emitted.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an export attempt.
    pub fn record_export(&self) {
        self.exports_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an export failure.
    pub fn record_export_failure(&self) {
        self.exports_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields. This is acceptable
    /// for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            metrics_recorded: self.metrics_recorded.load(Ordering::Relaxed),
            spans_opened: self.spans_opened.load(Ordering::Relaxed),
            spans_closed: self.spans_closed.load(Ordering::Relaxed),
            logs_emitted: self.logs_emitted.load(Ordering::Relaxed),
            exports_total: self.exports_total.load(Ordering::Relaxed),
            exports_failed: self.exports_failed.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ───────────────────────────────────────────────

/// Point-in-time snapshot of observability counters.
///
/// All rates are precomputed as basis-points (bps, 1/100 of a
/// percent) so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Metrics recorded since startup.
    pub metrics_recorded: u64,
    /// Spans opened since startup.
    pub spans_opened: u64,
    /// Spans closed since startup.
    pub spans_closed: u64,
    /// Log records emitted since startup.
    pub logs_emitted: u64,
    /// Export operations attempted.
    pub exports_total: u64,
    /// Export operations that failed.
    pub exports_failed: u64,
}

impl StatsSnapshot {
    /// Export failure rate in basis points (0..=10_000).
    /// Returns 0 when there are no exports yet.
    #[must_use]
    pub const fn export_failure_rate_bps(&self) -> u16 {
        if self.exports_total == 0 {
            return 0;
        }
        let rate = (self.exports_failed * 10_000) / self.exports_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Number of spans currently active (opened - closed).
    #[must_use]
    #[allow(clippy::implicit_saturating_sub)]
    pub const fn active_spans(&self) -> u64 {
        // Can't use saturating_sub in const fn before 1.76.
        if self.spans_opened >= self.spans_closed {
            self.spans_opened - self.spans_closed
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = ObservabilityStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_metric_increments_counter() {
        let s = ObservabilityStats::new();
        s.record_metric();
        s.record_metric();
        let snap = s.snapshot();
        assert_eq!(snap.metrics_recorded, 2);
    }

    #[test]
    fn record_span_operations() {
        let s = ObservabilityStats::new();
        s.record_span_open();
        s.record_span_open();
        s.record_span_close();
        let snap = s.snapshot();
        assert_eq!(snap.spans_opened, 2);
        assert_eq!(snap.spans_closed, 1);
        assert_eq!(snap.active_spans(), 1);
    }

    #[test]
    fn record_log_increments_counter() {
        let s = ObservabilityStats::new();
        s.record_log();
        s.record_log();
        s.record_log();
        assert_eq!(s.snapshot().logs_emitted, 3);
    }

    #[test]
    fn record_export_operations() {
        let s = ObservabilityStats::new();
        s.record_export();
        s.record_export();
        s.record_export_failure();
        let snap = s.snapshot();
        assert_eq!(snap.exports_total, 2);
        assert_eq!(snap.exports_failed, 1);
    }

    #[test]
    fn export_failure_rate_with_no_exports_is_zero() {
        let s = ObservabilityStats::new();
        assert_eq!(s.snapshot().export_failure_rate_bps(), 0);
    }

    #[test]
    fn export_failure_rate_with_mixed_results() {
        let s = ObservabilityStats::new();
        s.exports_total.store(100, Ordering::Relaxed);
        s.exports_failed.store(5, Ordering::Relaxed);
        // 5/100 = 5% = 500 bps.
        assert_eq!(s.snapshot().export_failure_rate_bps(), 500);
    }

    #[test]
    fn active_spans_with_only_opened() {
        let mut snap = StatsSnapshot::default();
        snap.spans_opened = 10;
        assert_eq!(snap.active_spans(), 10);
    }

    #[test]
    fn active_spans_with_more_closed_than_opened() {
        let mut snap = StatsSnapshot::default();
        snap.spans_opened = 5;
        snap.spans_closed = 10;
        assert_eq!(snap.active_spans(), 0);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = ObservabilityStats::new();
        s.record_metric();
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }
}

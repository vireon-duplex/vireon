//! [`MetricsCollector`] — skeleton for metrics collection.
//!
//! See `DESIGN.md` Part 10 (collector).
//!
//! # Phase 1
//!
//! This module ships as a typed skeleton. Functional collection lands
//! in Phase 1.

use crate::config::OpsDashboardConfig;
use crate::error::OpsDashboardError;
use crate::health::{HealthReport, HealthState};
use crate::query::{QueryAggregation, QueryRange};
use crate::stats::OpsDashboardStats;

/// Metrics collector (skeleton).
///
/// In Phase 1, this will collect metrics from the observability SDK,
/// execute ranged queries with aggregation, and return data points.
/// Currently returns placeholder errors.
#[derive(Debug)]
pub struct MetricsCollector {
    #[allow(dead_code)]
    cfg: OpsDashboardConfig,
    stats: OpsDashboardStats,
}

impl MetricsCollector {
    /// Create a new metrics collector.
    #[must_use]
    pub fn new(cfg: OpsDashboardConfig, stats: OpsDashboardStats) -> Self {
        Self { cfg, stats }
    }

    /// Collect metrics for the current instant.
    ///
    /// # Errors
    /// Returns [`OpsDashboardError::CollectorUnavailable`] unconditionally
    /// in this skeleton.
    ///
    /// // TODO(phase-1): Implement actual metric collection.
    pub fn collect(&self) -> Result<u64, OpsDashboardError> {
        // TODO(phase-1): Implement actual metric collection.
        Err(OpsDashboardError::CollectorUnavailable)
    }

    /// Query metrics over a time range with aggregation.
    ///
    /// # Errors
    /// Returns [`OpsDashboardError::QueryFailed`] unconditionally
    /// in this skeleton.
    ///
    /// // TODO(phase-1): Implement actual ranged queries.
    pub fn query_range(
        &self,
        _range: QueryRange,
        _aggregation: QueryAggregation,
    ) -> Result<u64, OpsDashboardError> {
        // TODO(phase-1): Implement actual ranged queries.
        Err(OpsDashboardError::QueryFailed("not implemented"))
    }

    /// Get the current health state.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        let snapshot = self.stats.snapshot();
        HealthReport::derive(&snapshot)
    }

    /// Get the current health state as a simple enum.
    #[must_use]
    pub fn health_state(&self) -> HealthState {
        self.health().state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_collector_created() {
        let cfg = OpsDashboardConfig::new();
        let stats = OpsDashboardStats::new();
        let collector = MetricsCollector::new(cfg, stats);
        let health = collector.health();
        assert_eq!(health.state, HealthState::Healthy);
    }

    #[test]
    fn collect_returns_collector_unavailable() {
        let collector = MetricsCollector::new(OpsDashboardConfig::new(), OpsDashboardStats::new());
        let result = collector.collect();
        assert!(matches!(
            result,
            Err(OpsDashboardError::CollectorUnavailable)
        ));
    }

    #[test]
    fn query_range_returns_query_failed() {
        let collector = MetricsCollector::new(OpsDashboardConfig::new(), OpsDashboardStats::new());
        let range = QueryRange::new(1_000, 5_000);
        let result = collector.query_range(range, QueryAggregation::Avg);
        assert!(matches!(result, Err(OpsDashboardError::QueryFailed(_))));
    }

    #[test]
    fn health_reflects_stats() {
        let stats = OpsDashboardStats::new();
        // Record some queries with a few failures.
        for _ in 0..100 {
            stats.record_query();
            stats.record_query_success();
        }
        for _ in 0..3 {
            stats.record_query();
            stats.record_query_failure();
        }
        // 100 succeeded out of 103 = 97.08% = 9708 bps.
        // ≥ 9500 → Degraded.
        let collector = MetricsCollector::new(OpsDashboardConfig::new(), stats);
        let health = collector.health();
        assert_eq!(health.state, HealthState::Degraded);
    }
}

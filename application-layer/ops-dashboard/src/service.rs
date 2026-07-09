//! [`OpsDashboardService`] — skeleton for the dashboard rendering
//! service.
//!
//! See `DESIGN.md` Part 9 (service).
//!
//! # Phase 1
//!
//! This module ships as a typed skeleton. Functional rendering lands
//! in Phase 1.

use crate::config::OpsDashboardConfig;
use crate::error::OpsDashboardError;
use crate::health::{HealthReport, HealthState};
use crate::stats::OpsDashboardStats;
use crate::widget::DashboardWidgetId;

/// Dashboard rendering service (skeleton).
///
/// In Phase 1, this will render widgets, manage refresh cycles,
/// and coordinate with the metrics collector. Currently returns
/// placeholder errors.
#[derive(Debug)]
pub struct OpsDashboardService {
    #[allow(dead_code)]
    cfg: OpsDashboardConfig,
    stats: OpsDashboardStats,
}

impl OpsDashboardService {
    /// Create a new dashboard service.
    #[must_use]
    pub fn new(cfg: OpsDashboardConfig, stats: OpsDashboardStats) -> Self {
        Self { cfg, stats }
    }

    /// Render a widget by its ID.
    ///
    /// # Errors
    /// Returns [`OpsDashboardError::ShuttingDown`] unconditionally
    /// in this skeleton.
    ///
    /// // TODO(phase-1): Implement actual widget rendering.
    pub fn render_widget(&self, _widget_id: DashboardWidgetId) -> Result<(), OpsDashboardError> {
        // TODO(phase-1): Implement actual widget rendering.
        Err(OpsDashboardError::ShuttingDown)
    }

    /// Refresh a widget's data.
    ///
    /// # Errors
    /// Returns [`OpsDashboardError::ShuttingDown`] unconditionally
    /// in this skeleton.
    ///
    /// // TODO(phase-1): Implement actual widget refresh.
    pub fn refresh_widget(&self, _widget_id: DashboardWidgetId) -> Result<(), OpsDashboardError> {
        // TODO(phase-1): Implement actual widget refresh.
        Err(OpsDashboardError::ShuttingDown)
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
    fn new_service_created() {
        let cfg = OpsDashboardConfig::new();
        let stats = OpsDashboardStats::new();
        let svc = OpsDashboardService::new(cfg, stats);
        let health = svc.health();
        assert_eq!(health.state, HealthState::Healthy);
    }

    #[test]
    fn render_widget_returns_shutting_down() {
        let svc = OpsDashboardService::new(OpsDashboardConfig::new(), OpsDashboardStats::new());
        let result = svc.render_widget(DashboardWidgetId::new(1));
        assert!(matches!(result, Err(OpsDashboardError::ShuttingDown)));
    }

    #[test]
    fn refresh_widget_returns_shutting_down() {
        let svc = OpsDashboardService::new(OpsDashboardConfig::new(), OpsDashboardStats::new());
        let result = svc.refresh_widget(DashboardWidgetId::new(1));
        assert!(matches!(result, Err(OpsDashboardError::ShuttingDown)));
    }

    #[test]
    fn health_reflects_stats() {
        let stats = OpsDashboardStats::new();
        // Record some failing queries to degrade health.
        for _ in 0..100 {
            stats.record_query();
        }
        for _ in 0..15 {
            stats.record_query_failure();
        }
        let svc = OpsDashboardService::new(OpsDashboardConfig::new(), stats);
        let health = svc.health();
        // 85% success < 90% → Critical.
        assert_eq!(health.state, HealthState::Critical);
    }
}

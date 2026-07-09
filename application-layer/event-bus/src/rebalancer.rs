//! [`PartitionRebalancer`] — partition rebalancing facade (typed skeleton).
//!
//! This is a **Phase 0 skeleton**. The full implementation lands in
//! Phase 1+.
//!
//! See `DESIGN.md` Part 11 (rebalancer).

use crate::config::EventBusConfig;
use crate::consumer::ConsumerGroupId;
use crate::error::EventError;
use crate::health::HealthReport;
use crate::partition::PartitionId;
use crate::stats::EventBusStats;
use std::fmt;

/// Partition rebalancing coordinator.
///
/// In Phase 0, this is a typed skeleton. Phase 1+ will include:
/// - Rebalance planning
/// - Partition assignment
/// - Consumer notification
/// - Rebalance execution
pub struct PartitionRebalancer {
    /// Rebalancer configuration.
    #[allow(dead_code)]
    cfg: EventBusConfig,
    /// Rebalancer statistics.
    stats: EventBusStats,
}

impl fmt::Debug for PartitionRebalancer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionRebalancer")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .finish()
    }
}

impl PartitionRebalancer {
    /// Create a new partition rebalancer with the given config.
    #[must_use]
    pub fn new(cfg: EventBusConfig) -> Self {
        Self {
            cfg,
            stats: EventBusStats::new(),
        }
    }

    /// Plan a rebalance for a consumer group.
    ///
    /// # Errors
    /// Returns [`EventError::ShuttingDown`] if the rebalancer is shutting down.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn plan(
        &self,
        _group: ConsumerGroupId,
        _partitions: &[PartitionId],
    ) -> Result<(), EventError> {
        Err(EventError::ShuttingDown)
    }

    /// Execute a rebalance for a consumer group.
    ///
    /// # Errors
    /// Returns [`EventError::ShuttingDown`] if the rebalancer is shutting down.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn execute(&self, _group: ConsumerGroupId) -> Result<(), EventError> {
        Err(EventError::ShuttingDown)
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebalancer_creation() {
        let cfg = EventBusConfig::new();
        let rebalancer = PartitionRebalancer::new(cfg);
        assert_eq!(rebalancer.stats.snapshot().rebalance_count, 0);
    }

    #[test]
    fn plan_returns_shutting_down() {
        let rebalancer = PartitionRebalancer::new(EventBusConfig::new());
        let result = rebalancer.plan(ConsumerGroupId::new(1), &[PartitionId::new(0)]);
        assert!(matches!(result, Err(EventError::ShuttingDown)));
    }

    #[test]
    fn execute_returns_shutting_down() {
        let rebalancer = PartitionRebalancer::new(EventBusConfig::new());
        let result = rebalancer.execute(ConsumerGroupId::new(1));
        assert!(matches!(result, Err(EventError::ShuttingDown)));
    }

    #[test]
    fn health_report_initially_healthy() {
        let rebalancer = PartitionRebalancer::new(EventBusConfig::new());
        let health = rebalancer.health();
        assert!(health.is_healthy());
    }
}

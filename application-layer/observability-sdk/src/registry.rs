//! [`MetricsRegistry`] — skeleton for metrics registry functionality.
//!
//! See `DESIGN.md` Part 10 (registry).
//!
//! # Phase 1
//!
//! This module ships as a typed skeleton. Functional registry lands in
//! Phase 1.

use crate::MetricKey;
use crate::config::ObservabilityConfig;
use crate::error::ObservabilityError;
use crate::metric::MetricType;
use crate::stats::ObservabilityStats;

/// Metrics registry (skeleton).
///
/// In Phase 1, this will manage metric registration and recording.
/// Currently returns placeholder errors.
#[derive(Debug)]
pub struct MetricsRegistry {
    #[allow(dead_code)]
    cfg: ObservabilityConfig,
    #[allow(dead_code)]
    stats: ObservabilityStats,
}

impl MetricsRegistry {
    /// Create a new metrics registry.
    pub fn new(cfg: ObservabilityConfig, stats: ObservabilityStats) -> Self {
        Self { cfg, stats }
    }

    /// Register a new metric.
    ///
    /// # Errors
    /// Returns [`ObservabilityError::ShuttingDown`] unconditionally
    /// in this skeleton.
    pub fn register(
        &mut self,
        _key: MetricKey,
        _mtype: MetricType,
    ) -> Result<(), ObservabilityError> {
        Err(ObservabilityError::ShuttingDown)
    }

    /// Record a metric value.
    ///
    /// # Errors
    /// Returns [`ObservabilityError::ShuttingDown`] unconditionally
    /// in this skeleton.
    pub fn record(&mut self, _key: MetricKey, _value: i64) -> Result<(), ObservabilityError> {
        Err(ObservabilityError::ShuttingDown)
    }

    /// Get the current health state.
    #[must_use]
    pub fn health(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_registry_created() {
        let cfg = ObservabilityConfig::new();
        let stats = ObservabilityStats::new();
        let registry = MetricsRegistry::new(cfg, stats);
        assert!(registry.health());
    }

    #[test]
    fn register_returns_shutting_down() {
        let mut registry =
            MetricsRegistry::new(ObservabilityConfig::new(), ObservabilityStats::new());
        let key = MetricKey::new("test").unwrap();
        assert!(matches!(
            registry.register(key, MetricType::Counter),
            Err(ObservabilityError::ShuttingDown)
        ));
    }

    #[test]
    fn record_returns_shutting_down() {
        let mut registry =
            MetricsRegistry::new(ObservabilityConfig::new(), ObservabilityStats::new());
        let key = MetricKey::new("test").unwrap();
        assert!(matches!(
            registry.record(key, 42),
            Err(ObservabilityError::ShuttingDown)
        ));
    }
}

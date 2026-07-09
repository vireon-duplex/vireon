//! [`MetricsExporter`] — skeleton for metrics export functionality.
//!
//! See `DESIGN.md` Part 9 (exporter).
//!
//! # Phase 1
//!
//! This module ships as a typed skeleton. Functional export lands in
//! Phase 1.

use crate::config::ObservabilityConfig;
use crate::error::ObservabilityError;
use crate::stats::ObservabilityStats;

/// Metrics exporter (skeleton).
///
/// In Phase 1, this will batch and export metrics to a backend.
/// Currently returns placeholder errors.
#[derive(Debug)]
pub struct MetricsExporter {
    #[allow(dead_code)]
    cfg: ObservabilityConfig,
    #[allow(dead_code)]
    stats: ObservabilityStats,
}

impl MetricsExporter {
    /// Create a new metrics exporter.
    pub fn new(cfg: ObservabilityConfig, stats: ObservabilityStats) -> Self {
        Self { cfg, stats }
    }

    /// Export metrics to the backend.
    ///
    /// # Errors
    /// Returns [`ObservabilityError::ShuttingDown`] unconditionally
    /// in this skeleton.
    pub fn export(&mut self) -> Result<u64, ObservabilityError> {
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
    fn new_exporter_created() {
        let cfg = ObservabilityConfig::new();
        let stats = ObservabilityStats::new();
        let exporter = MetricsExporter::new(cfg, stats);
        assert!(exporter.health());
    }

    #[test]
    fn export_returns_shutting_down() {
        let mut exporter =
            MetricsExporter::new(ObservabilityConfig::new(), ObservabilityStats::new());
        assert!(matches!(
            exporter.export(),
            Err(ObservabilityError::ShuttingDown)
        ));
    }
}

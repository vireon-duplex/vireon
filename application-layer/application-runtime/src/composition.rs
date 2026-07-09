//! [`CompositionRoot`] — skeleton for the composition root that
//! ties together all application-layer subsystems.
//!
//! See `DESIGN.md` Part 10 (composition root).
//!
//! # Phase 1
//!
//! This module ships as a typed skeleton. Functional composition,
//! dependency resolution, and health aggregation land in Phase 1.

use crate::config::AppRuntimeConfig;
use crate::error::AppRuntimeError;
use crate::health::{HealthReport, HealthState};
use crate::stats::AppRuntimeStats;

/// Composition root for the application runtime (skeleton).
///
/// In Phase 1, this will wire together all 20 application-layer
/// crates into a cohesive runtime. Currently returns placeholder
/// errors.
#[derive(Debug)]
pub struct CompositionRoot {
    #[allow(dead_code)]
    cfg: AppRuntimeConfig,
    stats: AppRuntimeStats,
}

impl CompositionRoot {
    /// Create a new composition root.
    #[must_use]
    pub fn new(cfg: AppRuntimeConfig, stats: AppRuntimeStats) -> Self {
        Self { cfg, stats }
    }

    /// Compose all subsystems.
    ///
    /// # Errors
    /// Returns [`AppRuntimeError::CompositionFailed`] unconditionally
    /// in this skeleton.
    ///
    /// // TODO(phase-1): Implement actual subsystem composition.
    pub fn compose(&self) -> Result<(), AppRuntimeError> {
        // TODO(phase-1): Implement actual subsystem composition.
        Err(AppRuntimeError::CompositionFailed("not implemented"))
    }

    /// Resolve a dependency by name.
    ///
    /// # Errors
    /// Returns [`AppRuntimeError::CompositionFailed`] unconditionally
    /// in this skeleton.
    ///
    /// // TODO(phase-1): Implement actual dependency resolution.
    pub fn resolve(&self, _name: &'static str) -> Result<(), AppRuntimeError> {
        // TODO(phase-1): Implement actual dependency resolution.
        Err(AppRuntimeError::CompositionFailed("not implemented"))
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
    fn new_composition_root_created() {
        let cfg = AppRuntimeConfig::new();
        let stats = AppRuntimeStats::new();
        let root = CompositionRoot::new(cfg, stats);
        let health = root.health();
        assert_eq!(health.state, HealthState::Healthy);
    }

    #[test]
    fn compose_returns_composition_failed() {
        let root = CompositionRoot::new(AppRuntimeConfig::new(), AppRuntimeStats::new());
        let result = root.compose();
        assert!(matches!(result, Err(AppRuntimeError::CompositionFailed(_))));
    }

    #[test]
    fn resolve_returns_composition_failed() {
        let root = CompositionRoot::new(AppRuntimeConfig::new(), AppRuntimeStats::new());
        let result = root.resolve("auth");
        assert!(matches!(result, Err(AppRuntimeError::CompositionFailed(_))));
    }

    #[test]
    fn health_reflects_stats() {
        let stats = AppRuntimeStats::new();
        for _ in 0..100 {
            stats.record_startup();
        }
        for _ in 0..5 {
            stats.record_module_failure();
        }
        let root = CompositionRoot::new(AppRuntimeConfig::new(), stats);
        let health = root.health();
        // 95% success >= 90% but < 99% -> Degraded.
        assert_eq!(health.state, HealthState::Degraded);
    }
}

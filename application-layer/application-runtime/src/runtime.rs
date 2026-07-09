//! [`AppRuntime`] — skeleton for the application runtime facade.
//!
//! See `DESIGN.md` Part 9 (runtime facade).
//!
//! # Phase 1
//!
//! This module ships as a typed skeleton. Functional startup,
//! shutdown, and health monitoring land in Phase 1.

use crate::config::AppRuntimeConfig;
use crate::error::AppRuntimeError;
use crate::health::{HealthReport, HealthState};
use crate::runtime_id::RuntimeId;
use crate::stats::AppRuntimeStats;

/// Application runtime facade (skeleton).
///
/// In Phase 1, this will orchestrate module startup, shutdown,
/// health monitoring, and composition root management. Currently
/// returns placeholder errors.
#[derive(Debug)]
pub struct AppRuntime {
    #[allow(dead_code)]
    cfg: AppRuntimeConfig,
    #[allow(dead_code)]
    id: RuntimeId,
    stats: AppRuntimeStats,
}

impl AppRuntime {
    /// Create a new application runtime.
    #[must_use]
    pub fn new(cfg: AppRuntimeConfig, id: RuntimeId, stats: AppRuntimeStats) -> Self {
        Self { cfg, id, stats }
    }

    /// Start the application runtime.
    ///
    /// # Errors
    /// Returns [`AppRuntimeError::ShuttingDown`] unconditionally
    /// in this skeleton.
    ///
    /// // TODO(phase-1): Implement actual runtime startup.
    pub fn start(&self) -> Result<(), AppRuntimeError> {
        // TODO(phase-1): Implement actual runtime startup.
        Err(AppRuntimeError::ShuttingDown)
    }

    /// Stop the application runtime.
    ///
    /// # Errors
    /// Returns [`AppRuntimeError::NotRunning`] unconditionally
    /// in this skeleton.
    ///
    /// // TODO(phase-1): Implement actual runtime shutdown.
    pub fn stop(&self) -> Result<(), AppRuntimeError> {
        // TODO(phase-1): Implement actual runtime shutdown.
        Err(AppRuntimeError::NotRunning)
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
    fn new_runtime_created() {
        let cfg = AppRuntimeConfig::new();
        let id = RuntimeId::new(1);
        let stats = AppRuntimeStats::new();
        let rt = AppRuntime::new(cfg, id, stats);
        let health = rt.health();
        assert_eq!(health.state, HealthState::Healthy);
    }

    #[test]
    fn start_returns_shutting_down() {
        let rt = AppRuntime::new(
            AppRuntimeConfig::new(),
            RuntimeId::new(1),
            AppRuntimeStats::new(),
        );
        let result = rt.start();
        assert!(matches!(result, Err(AppRuntimeError::ShuttingDown)));
    }

    #[test]
    fn stop_returns_not_running() {
        let rt = AppRuntime::new(
            AppRuntimeConfig::new(),
            RuntimeId::new(1),
            AppRuntimeStats::new(),
        );
        let result = rt.stop();
        assert!(matches!(result, Err(AppRuntimeError::NotRunning)));
    }

    #[test]
    fn health_reflects_stats() {
        let stats = AppRuntimeStats::new();
        // Record some startups with failures to degrade health.
        for _ in 0..100 {
            stats.record_startup();
        }
        for _ in 0..15 {
            stats.record_module_failure();
        }
        let rt = AppRuntime::new(AppRuntimeConfig::new(), RuntimeId::new(1), stats);
        let health = rt.health();
        // 85% success < 90% -> Critical.
        assert_eq!(health.state, HealthState::Critical);
    }
}

//! [`FeatureFlagManager`] — typed skeleton for flag registry and
//! evaluation.
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a
//! stub error so that callers can code against the API contract
//! today. Phase 1 fills in the real logic.

use crate::config::FeatureFlagsConfig;
use crate::error::FlagError;
use crate::eval::EvaluationContext;
use crate::flag::{FlagKey, FlagState, FlagValue};
use crate::health::HealthReport;
use crate::stats::FeatureFlagsStats;

/// Feature flag manager facade.
///
/// Composes config, stats, and (in Phase 1+) a flag registry
/// and evaluation cache. Phase 0 ships a typed skeleton;
/// Phase 1+ populates real logic.
#[derive(Debug)]
pub struct FeatureFlagManager {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: FeatureFlagsConfig,
    /// Atomic observability counters.
    stats: FeatureFlagsStats,
}

impl FeatureFlagManager {
    /// Construct a manager from validated configuration.
    ///
    /// # Errors
    /// Returns `FlagError::InvalidValue` if the config fails validation.
    pub fn new(cfg: FeatureFlagsConfig) -> Result<Self, FlagError> {
        cfg.validate()?;
        Ok(Self {
            cfg,
            stats: FeatureFlagsStats::new(),
        })
    }

    /// Evaluate a flag for the given context.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn evaluate(&self, _key: FlagKey, _ctx: EvaluationContext) -> Result<FlagValue, FlagError> {
        // TODO(phase-1): registry lookup + rule evaluation + cache.
        Err(FlagError::ShuttingDown)
    }

    /// Toggle a flag's enabled state.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn toggle(&mut self, _key: FlagKey, _enabled: bool) -> Result<(), FlagError> {
        // TODO(phase-1): registry update + cache invalidation.
        Err(FlagError::ShuttingDown)
    }

    /// Register a new flag.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn register(&mut self, _key: FlagKey, _state: FlagState) -> Result<(), FlagError> {
        // TODO(phase-1): registry insert.
        Err(FlagError::ShuttingDown)
    }

    /// Snapshot the manager's health.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    /// Reference to the underlying stats (for testing / metrics).
    #[must_use]
    pub fn stats(&self) -> &FeatureFlagsStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;

    #[test]
    fn manager_constructs_from_valid_config() {
        let cfg = Preset::Default.to_config();
        let manager = FeatureFlagManager::new(cfg).unwrap();
        let h = manager.health();
        assert!(h.is_healthy());
    }

    #[test]
    fn manager_rejects_invalid_config() {
        let mut cfg = Preset::Default.to_config();
        cfg.max_flags = 5; // too small
        assert!(FeatureFlagManager::new(cfg).is_err());
    }

    #[test]
    fn evaluate_phase0_stub() {
        let cfg = Preset::Default.to_config();
        let manager = FeatureFlagManager::new(cfg).unwrap();
        let key = FlagKey::new("test").unwrap();
        let ctx = EvaluationContext::minimal(1, 100);
        assert!(manager.evaluate(key, ctx).is_err());
    }

    #[test]
    fn toggle_phase0_stub() {
        let cfg = Preset::Default.to_config();
        let mut manager = FeatureFlagManager::new(cfg).unwrap();
        let key = FlagKey::new("test").unwrap();
        assert!(manager.toggle(key, true).is_err());
    }

    #[test]
    fn register_phase0_stub() {
        let cfg = Preset::Default.to_config();
        let mut manager = FeatureFlagManager::new(cfg).unwrap();
        let key = FlagKey::new("test").unwrap();
        let state = FlagState::new(true, FlagValue::bool(true), false, 0);
        assert!(manager.register(key, state).is_err());
    }
}

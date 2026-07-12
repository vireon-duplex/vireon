//! [`FeatureFlagManager`] — flag registry and evaluation.
//!
//! Provides an in-process feature flag registry backed by a
//! `HashMap<FlagKey, FlagState>`. Single-threaded (reactor-local).

use std::collections::HashMap;

use crate::config::FeatureFlagsConfig;
use crate::error::FlagError;
use crate::eval::EvaluationContext;
use crate::flag::{FlagKey, FlagState, FlagValue};
use crate::health::HealthReport;
use crate::stats::FeatureFlagsStats;

/// Feature flag manager facade.
///
/// Owns the flag registry and evaluation stats. All methods take
/// `&self` or `&mut self` (single-threaded, no locking).
#[derive(Debug)]
pub struct FeatureFlagManager {
    #[allow(dead_code)]
    cfg: FeatureFlagsConfig,
    stats: FeatureFlagsStats,
    flags: HashMap<FlagKey, FlagState>,
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
            flags: HashMap::new(),
        })
    }

    /// Evaluate a flag for the given context.
    ///
    /// Returns the flag's configured `FlagValue` if enabled, or
    /// `FlagValue::Bool(false)` if the flag exists but is disabled.
    ///
    /// # Errors
    /// Returns [`FlagError::NotFound`] if the flag was never registered.
    pub fn evaluate(&self, key: FlagKey, _ctx: EvaluationContext) -> Result<FlagValue, FlagError> {
        let state = self
            .flags
            .get(&key)
            .ok_or_else(|| FlagError::NotFound(key.as_str().to_string()))?;

        let value = if state.is_enabled() {
            state.value()
        } else {
            FlagValue::Bool(false)
        };

        self.stats.record_evaluation(state.is_enabled());
        Ok(value)
    }

    /// Evaluate a flag and return a simple `bool`.
    ///
    /// Convenience for `evaluate(key, ctx)?.as_bool().unwrap_or(false)`.
    /// Returns `false` if the flag is not found (fail-closed for bool).
    #[must_use]
    pub fn is_enabled(&self, key: FlagKey) -> bool {
        match self.flags.get(&key) {
            Some(state) => {
                let enabled = state.is_enabled();
                self.stats.record_evaluation(enabled);
                enabled
            }
            None => false,
        }
    }

    /// Toggle a flag's enabled state.
    ///
    /// # Errors
    /// Returns [`FlagError::Frozen`] if the flag is frozen.
    /// Returns [`FlagError::NotFound`] if the flag was never registered.
    pub fn toggle(&mut self, key: FlagKey, enabled: bool) -> Result<(), FlagError> {
        let state = self
            .flags
            .get_mut(&key)
            .ok_or_else(|| FlagError::NotFound(key.as_str().to_string()))?;

        if state.is_frozen() {
            return Err(FlagError::Frozen(key));
        }

        state.enabled = enabled;
        Ok(())
    }

    /// Register a new flag.
    ///
    /// If the flag already exists, its state is replaced.
    pub fn register(&mut self, key: FlagKey, state: FlagState) -> Result<(), FlagError> {
        self.flags.insert(key, state);
        Ok(())
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

    /// Number of registered flags.
    #[must_use]
    pub fn len(&self) -> usize {
        self.flags.len()
    }

    /// Whether the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.flags.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;

    fn fresh() -> FeatureFlagManager {
        let cfg = Preset::Default.to_config();
        FeatureFlagManager::new(cfg).unwrap()
    }

    fn enabled_state() -> FlagState {
        FlagState::new(true, FlagValue::bool(true), false, 0)
    }

    fn disabled_state() -> FlagState {
        FlagState::new(false, FlagValue::bool(false), false, 0)
    }

    #[test]
    fn manager_constructs_from_valid_config() {
        let manager = fresh();
        assert!(manager.health().is_healthy());
    }

    #[test]
    fn manager_rejects_invalid_config() {
        let mut cfg = Preset::Default.to_config();
        cfg.max_flags = 5; // too small
        assert!(FeatureFlagManager::new(cfg).is_err());
    }

    #[test]
    fn register_and_evaluate_enabled() {
        let mut manager = fresh();
        let key = FlagKey::new("ai.enabled").unwrap();
        manager.register(key, enabled_state()).unwrap();

        let ctx = EvaluationContext::minimal(1, 100);
        let val = manager.evaluate(key, ctx).unwrap();
        assert_eq!(val, FlagValue::Bool(true));
    }

    #[test]
    fn register_and_evaluate_disabled() {
        let mut manager = fresh();
        let key = FlagKey::new("beta.feature").unwrap();
        manager.register(key, disabled_state()).unwrap();

        let ctx = EvaluationContext::minimal(1, 100);
        let val = manager.evaluate(key, ctx).unwrap();
        assert_eq!(val, FlagValue::Bool(false));
    }

    #[test]
    fn evaluate_unknown_returns_not_found() {
        let manager = fresh();
        let key = FlagKey::new("missing").unwrap();
        let ctx = EvaluationContext::minimal(1, 100);
        assert!(matches!(
            manager.evaluate(key, ctx),
            Err(FlagError::NotFound(_))
        ));
    }

    #[test]
    fn toggle_changes_evaluation() {
        let mut manager = fresh();
        let key = FlagKey::new("toggle.me").unwrap();
        manager.register(key, enabled_state()).unwrap();

        let ctx = EvaluationContext::minimal(1, 100);
        assert!(manager.evaluate(key, ctx).unwrap().as_bool() == Some(true));

        manager.toggle(key, false).unwrap();
        assert!(manager.evaluate(key, ctx).unwrap().as_bool() == Some(false));
    }

    #[test]
    fn toggle_unknown_returns_not_found() {
        let mut manager = fresh();
        let key = FlagKey::new("missing").unwrap();
        assert!(matches!(
            manager.toggle(key, true),
            Err(FlagError::NotFound(_))
        ));
    }

    #[test]
    fn toggle_frozen_returns_frozen() {
        let mut manager = fresh();
        let key = FlagKey::new("locked").unwrap();
        let frozen_state = FlagState::new(true, FlagValue::bool(true), true, 0); // frozen=true
        manager.register(key, frozen_state).unwrap();

        assert!(matches!(
            manager.toggle(key, false),
            Err(FlagError::Frozen(_))
        ));
    }

    #[test]
    fn is_enabled_convenience() {
        let mut manager = fresh();
        let key = FlagKey::new("quick.check").unwrap();
        manager.register(key, enabled_state()).unwrap();
        assert!(manager.is_enabled(key));

        manager.toggle(key, false).unwrap();
        assert!(!manager.is_enabled(key));

        // Unknown flag → false (fail-closed)
        let missing = FlagKey::new("missing").unwrap();
        assert!(!manager.is_enabled(missing));
    }

    #[test]
    fn len_tracks_registrations() {
        let mut manager = fresh();
        assert_eq!(manager.len(), 0);
        manager
            .register(FlagKey::new("a").unwrap(), enabled_state())
            .unwrap();
        assert_eq!(manager.len(), 1);
        manager
            .register(FlagKey::new("b").unwrap(), disabled_state())
            .unwrap();
        assert_eq!(manager.len(), 2);
    }
}

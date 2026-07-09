//! [`ConfigurationManager`] — the main configuration facade.
//!
//! See `DESIGN.md` Part 1 (architecture), Part 10 (manager).
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a stub
//! error so that callers can code against the API contract today.
//! Phase 1 fills in the real logic.

use crate::config::ConfigurationSystemConfig;
use crate::error::ConfigError;
use crate::health::HealthReport;
use crate::key::ConfigKey;
use crate::stats::ConfigStats;
use crate::value::ConfigValue;

/// The configuration manager facade.
///
/// Composes the layer stack, stats, and hot reload watcher.
/// Phase 0 ships a typed skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct ConfigurationManager {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: ConfigurationSystemConfig,
    /// Atomic observability counters.
    stats: ConfigStats,
}

impl ConfigurationManager {
    /// Construct a manager from validated configuration.
    ///
    /// # Errors
    /// Returns [`ConfigError::InvalidConfig`] if the config fails
    /// validation.
    pub fn new(cfg: ConfigurationSystemConfig) -> Result<Self, ConfigError> {
        cfg.validate()?;
        Ok(Self {
            cfg,
            stats: ConfigStats::new(),
        })
    }

    /// Get a configuration value by key.
    ///
    /// This is the **hot path** — must be O(1) in steady state.
    /// Phase 1 will add the layer merging logic.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn get(&self, _key: ConfigKey) -> Result<ConfigValue, ConfigError> {
        // TODO(phase-1): layer stack lookup + merge.
        self.stats.record_read(false);
        Err(ConfigError::ShuttingDown)
    }

    /// Set a configuration value at runtime.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn set(&mut self, _key: ConfigKey, _value: ConfigValue) -> Result<(), ConfigError> {
        // TODO(phase-1): layer insertion + validation.
        Err(ConfigError::ShuttingDown)
    }

    /// Freeze a configuration key (make it immutable).
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn freeze(&mut self, _key: ConfigKey) -> Result<(), ConfigError> {
        // TODO(phase-1): mark key as frozen, prevent future sets.
        self.stats.record_freeze();
        Err(ConfigError::ShuttingDown)
    }

    /// Reload configuration from sources.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn reload(&mut self) -> Result<u64, ConfigError> {
        // TODO(phase-1): re-read sources + merge layers.
        self.stats.record_reload();
        Err(ConfigError::ShuttingDown)
    }

    /// Snapshot the manager's health.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    /// Reference to the underlying stats (for testing / metrics).
    #[must_use]
    pub fn stats(&self) -> &ConfigStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;

    fn make_manager() -> ConfigurationManager {
        let cfg = Preset::Development.to_config().unwrap();
        ConfigurationManager::new(cfg).unwrap()
    }

    #[test]
    fn manager_constructs_from_valid_config() {
        let mgr = make_manager();
        let h = mgr.health();
        assert!(h.is_healthy());
    }

    #[test]
    fn manager_rejects_invalid_config() {
        let mut cfg = Preset::Development.to_config().unwrap();
        cfg.max_layers = 0; // invalid
        assert!(ConfigurationManager::new(cfg).is_err());
    }

    #[test]
    fn get_phase0_stub() {
        let mgr = make_manager();
        let key = ConfigKey::new("test.key").unwrap();
        assert!(mgr.get(key).is_err());
    }

    #[test]
    fn set_phase0_stub() {
        let mut mgr = make_manager();
        let key = ConfigKey::new("test.key").unwrap();
        assert!(mgr.set(key, ConfigValue::Bool(true)).is_err());
    }

    #[test]
    fn freeze_phase0_stub() {
        let mut mgr = make_manager();
        let key = ConfigKey::new("test.key").unwrap();
        assert!(mgr.freeze(key).is_err());
    }

    #[test]
    fn reload_phase0_stub() {
        let mut mgr = make_manager();
        assert!(mgr.reload().is_err());
    }

    #[test]
    fn health_snapshot_works() {
        let mgr = make_manager();
        let report = mgr.health();
        assert_eq!(report.state, crate::health::HealthState::Healthy);
    }
}

//! [`ConfigurationManager`] — the main configuration facade.
//!
//! Provides an in-process key-value config store backed by a
//! `HashMap<ConfigKey, ConfigEntry>`. Supports runtime updates and
//! key freezing. Single-threaded (reactor-local).

use std::collections::HashMap;

use crate::config::ConfigurationSystemConfig;
use crate::error::ConfigError;
use crate::health::HealthReport;
use crate::key::ConfigKey;
use crate::stats::ConfigStats;
use crate::value::ConfigValue;

/// One stored configuration entry.
#[derive(Debug, Clone)]
struct ConfigEntry {
    value: ConfigValue,
    frozen: bool,
}

/// The configuration manager facade.
///
/// Owns a flat key-value store with freeze support. All methods take
/// `&self` or `&mut self` (single-threaded, no locking).
#[derive(Debug)]
pub struct ConfigurationManager {
    #[allow(dead_code)]
    cfg: ConfigurationSystemConfig,
    stats: ConfigStats,
    entries: HashMap<ConfigKey, ConfigEntry>,
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
            entries: HashMap::new(),
        })
    }

    /// Get a configuration value by key.
    ///
    /// This is the **hot path** — O(1) HashMap lookup.
    ///
    /// # Errors
    /// Returns [`ConfigError::KeyNotFound`] if the key doesn't exist.
    pub fn get(&self, key: ConfigKey) -> Result<ConfigValue, ConfigError> {
        match self.entries.get(&key) {
            Some(entry) => {
                self.stats.record_read(true);
                Ok(entry.value)
            }
            None => {
                self.stats.record_read(false);
                Err(ConfigError::KeyNotFound(key.as_str().to_string()))
            }
        }
    }

    /// Set a configuration value at runtime.
    ///
    /// # Errors
    /// Returns [`ConfigError::Frozen`] if the key is frozen.
    pub fn set(&mut self, key: ConfigKey, value: ConfigValue) -> Result<(), ConfigError> {
        if let Some(entry) = self.entries.get_mut(&key) {
            if entry.frozen {
                return Err(ConfigError::Frozen(key.as_str().to_string()));
            }
            entry.value = value;
        } else {
            self.entries.insert(
                key,
                ConfigEntry {
                    value,
                    frozen: false,
                },
            );
        }
        Ok(())
    }

    /// Freeze a configuration key (make it immutable).
    ///
    /// Future `set()` calls on this key will return
    /// [`ConfigError::Frozen`].
    ///
    /// # Errors
    /// Returns [`ConfigError::KeyNotFound`] if the key doesn't exist.
    pub fn freeze(&mut self, key: ConfigKey) -> Result<(), ConfigError> {
        let entry = self
            .entries
            .get_mut(&key)
            .ok_or_else(|| ConfigError::KeyNotFound(key.as_str().to_string()))?;

        if !entry.frozen {
            entry.frozen = true;
            self.stats.record_freeze();
        }
        Ok(())
    }

    /// Reload configuration from sources.
    ///
    /// Phase 1: returns the count of registered keys (no external
    /// sources to reload from yet).
    pub fn reload(&mut self) -> Result<u64, ConfigError> {
        self.stats.record_reload();
        Ok(self.entries.len() as u64)
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

    /// Number of registered keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the store is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
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
        assert!(mgr.health().is_healthy());
    }

    #[test]
    fn manager_rejects_invalid_config() {
        let mut cfg = Preset::Development.to_config().unwrap();
        cfg.max_layers = 0; // invalid
        assert!(ConfigurationManager::new(cfg).is_err());
    }

    #[test]
    fn set_and_get_roundtrip() {
        let mut mgr = make_manager();
        let key = ConfigKey::new("server.port").unwrap();
        mgr.set(key, ConfigValue::U64(443)).unwrap();
        assert_eq!(mgr.get(key).unwrap(), ConfigValue::U64(443));
    }

    #[test]
    fn get_missing_returns_key_not_found() {
        let mgr = make_manager();
        let key = ConfigKey::new("missing.key").unwrap();
        assert!(matches!(mgr.get(key), Err(ConfigError::KeyNotFound(_))));
    }

    #[test]
    fn set_updates_existing() {
        let mut mgr = make_manager();
        let key = ConfigKey::new("dynamic.value").unwrap();
        mgr.set(key, ConfigValue::I64(1)).unwrap();
        mgr.set(key, ConfigValue::I64(99)).unwrap();
        assert_eq!(mgr.get(key).unwrap(), ConfigValue::I64(99));
    }

    #[test]
    fn freeze_prevents_future_sets() {
        let mut mgr = make_manager();
        let key = ConfigKey::new("locked.value").unwrap();
        mgr.set(key, ConfigValue::Bool(true)).unwrap();
        mgr.freeze(key).unwrap();

        assert!(matches!(
            mgr.set(key, ConfigValue::Bool(false)),
            Err(ConfigError::Frozen(_))
        ));
        // Original value preserved.
        assert_eq!(mgr.get(key).unwrap(), ConfigValue::Bool(true));
    }

    #[test]
    fn freeze_missing_returns_not_found() {
        let mut mgr = make_manager();
        let key = ConfigKey::new("never.set").unwrap();
        assert!(matches!(mgr.freeze(key), Err(ConfigError::KeyNotFound(_))));
    }

    #[test]
    fn double_freeze_is_idempotent() {
        let mut mgr = make_manager();
        let key = ConfigKey::new("once.frozen").unwrap();
        mgr.set(key, ConfigValue::U64(1)).unwrap();
        mgr.freeze(key).unwrap();
        mgr.freeze(key).unwrap(); // no error
    }

    #[test]
    fn reload_returns_key_count() {
        let mut mgr = make_manager();
        mgr.set(ConfigKey::new("a").unwrap(), ConfigValue::Bool(true))
            .unwrap();
        mgr.set(ConfigKey::new("b").unwrap(), ConfigValue::Bool(false))
            .unwrap();
        let count = mgr.reload().unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn health_snapshot_works() {
        let mgr = make_manager();
        assert!(mgr.health().is_healthy());
    }

    #[test]
    fn len_and_is_empty() {
        let mut mgr = make_manager();
        assert!(mgr.is_empty());
        mgr.set(ConfigKey::new("a").unwrap(), ConfigValue::Bool(true))
            .unwrap();
        assert_eq!(mgr.len(), 1);
        assert!(!mgr.is_empty());
    }
}

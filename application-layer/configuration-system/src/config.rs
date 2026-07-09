//! [`ConfigurationSystemConfig`], [`Preset`] — declarative configuration
//! for the configuration system.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).

use crate::error::ConfigError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of layers allowed.
pub const MIN_MAX_LAYERS: u8 = 1;

/// Maximum number of layers allowed.
pub const MAX_MAX_LAYERS: u8 = 16;

/// Minimum watch interval in milliseconds.
pub const MIN_WATCH_INTERVAL_MS: u64 = 100;

/// Maximum watch interval in milliseconds (1 hour).
pub const MAX_WATCH_INTERVAL_MS: u64 = 3_600_000;

// ── ConfigurationSystemConfig ────────────────────────────────────

/// Declarative configuration for the configuration system.
///
/// Constructed via [`ConfigurationSystemConfig::new`] or obtained from
/// a [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConfigurationSystemConfig {
    /// Maximum number of config layers (sources) to stack.
    pub max_layers: u8,
    /// Watch interval for hot reload in milliseconds.
    pub watch_interval_ms: u64,
    /// Whether hot reload is enabled.
    pub enable_hot_reload: bool,
    /// Whether keys are frozen on first load (immutable).
    pub freeze_on_load: bool,
    /// Maximum key length in bytes.
    pub max_key_length: u32,
}

impl ConfigurationSystemConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: 8 layers, 5s watch interval, hot reload enabled,
    /// no freeze on load, 63 byte max key length.
    ///
    /// # Errors
    /// Returns [`ConfigError::InvalidConfig`] only if validation fails.
    pub fn new() -> Result<Self, ConfigError> {
        Ok(Self {
            max_layers: 8,
            watch_interval_ms: 5_000,
            enable_hot_reload: true,
            freeze_on_load: false,
            max_key_length: 63,
        })
    }

    /// Builder-style: set max layers.
    #[must_use]
    pub const fn with_max_layers(mut self, layers: u8) -> Self {
        self.max_layers = layers;
        self
    }

    /// Builder-style: set watch interval (ms).
    #[must_use]
    pub const fn with_watch_interval_ms(mut self, interval_ms: u64) -> Self {
        self.watch_interval_ms = interval_ms;
        self
    }

    /// Builder-style: enable or disable hot reload.
    #[must_use]
    pub const fn with_enable_hot_reload(mut self, enable: bool) -> Self {
        self.enable_hot_reload = enable;
        self
    }

    /// Builder-style: enable or disable freeze on load.
    #[must_use]
    pub const fn with_freeze_on_load(mut self, freeze: bool) -> Self {
        self.freeze_on_load = freeze;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`ConfigError::InvalidConfig`] if any field is out of
    /// range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_layers < MIN_MAX_LAYERS || self.max_layers > MAX_MAX_LAYERS {
            return Err(ConfigError::InvalidConfig(format!(
                "max_layers {layers} out of range [{MIN_MAX_LAYERS}, {MAX_MAX_LAYERS}]",
                layers = self.max_layers
            )));
        }
        if self.watch_interval_ms < MIN_WATCH_INTERVAL_MS
            || self.watch_interval_ms > MAX_WATCH_INTERVAL_MS
        {
            return Err(ConfigError::InvalidConfig(format!(
                "watch_interval_ms {interval} out of range [{MIN_WATCH_INTERVAL_MS}, {MAX_WATCH_INTERVAL_MS}]",
                interval = self.watch_interval_ms
            )));
        }
        if self.max_key_length == 0 || self.max_key_length > 1024 {
            return Err(ConfigError::InvalidConfig(format!(
                "max_key_length {len} out of range [1, 1024]",
                len = self.max_key_length
            )));
        }
        Ok(())
    }
}

impl Default for ConfigurationSystemConfig {
    fn default() -> Self {
        // Construct directly to avoid going through `new()` which returns Result.
        Self {
            max_layers: 8,
            watch_interval_ms: 5_000,
            enable_hot_reload: true,
            freeze_on_load: false,
            max_key_length: 63,
        }
    }
}

// ── Preset ───────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// use case. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Development: fast reload, no freeze, few layers.
    Development,
    /// Staging: moderate reload, some freeze, more layers.
    Staging,
    /// Production: conservative reload, freeze on load, many layers.
    Production,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    ///
    /// # Errors
    /// Returns [`ConfigError::InvalidConfig`] if validation fails.
    pub fn to_config(self) -> Result<ConfigurationSystemConfig, ConfigError> {
        let cfg = match self {
            Self::Development => ConfigurationSystemConfig {
                max_layers: 4,
                watch_interval_ms: 1_000, // 1 second for fast dev iteration
                enable_hot_reload: true,
                freeze_on_load: false,
                max_key_length: 63,
            },
            Self::Staging => ConfigurationSystemConfig {
                max_layers: 8,
                watch_interval_ms: 5_000, // 5 seconds
                enable_hot_reload: true,
                freeze_on_load: true,
                max_key_length: 63,
            },
            Self::Production => ConfigurationSystemConfig {
                max_layers: 16,
                watch_interval_ms: 30_000, // 30 seconds
                enable_hot_reload: true,
                freeze_on_load: true,
                max_key_length: 63,
            },
        };
        cfg.validate()?;
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = ConfigurationSystemConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_layers, 8);
        assert_eq!(cfg.watch_interval_ms, 5_000);
    }

    #[test]
    fn new_config_succeeds() {
        let cfg = ConfigurationSystemConfig::new().unwrap();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_layers, 8);
        assert!(cfg.enable_hot_reload);
        assert!(!cfg.freeze_on_load);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = ConfigurationSystemConfig::new()
            .unwrap()
            .with_max_layers(12)
            .with_watch_interval_ms(10_000)
            .with_enable_hot_reload(false)
            .with_freeze_on_load(true);
        assert_eq!(cfg.max_layers, 12);
        assert_eq!(cfg.watch_interval_ms, 10_000);
        assert!(!cfg.enable_hot_reload);
        assert!(cfg.freeze_on_load);
    }

    #[test]
    fn validate_rejects_bad_max_layers() {
        let mut cfg = ConfigurationSystemConfig::default();
        cfg.max_layers = 0;
        assert!(cfg.validate().is_err());
        cfg.max_layers = MAX_MAX_LAYERS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_watch_interval() {
        let mut cfg = ConfigurationSystemConfig::default();
        cfg.watch_interval_ms = MIN_WATCH_INTERVAL_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.watch_interval_ms = MAX_WATCH_INTERVAL_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_max_key_length() {
        let mut cfg = ConfigurationSystemConfig::default();
        cfg.max_key_length = 0;
        assert!(cfg.validate().is_err());
        cfg.max_key_length = 2048;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn development_preset_uses_fast_reload() {
        let cfg = Preset::Development.to_config().unwrap();
        assert_eq!(cfg.max_layers, 4);
        assert_eq!(cfg.watch_interval_ms, 1_000);
        assert!(!cfg.freeze_on_load);
        assert!(cfg.enable_hot_reload);
    }

    #[test]
    fn staging_preset_uses_moderate_reload() {
        let cfg = Preset::Staging.to_config().unwrap();
        assert_eq!(cfg.max_layers, 8);
        assert_eq!(cfg.watch_interval_ms, 5_000);
        assert!(cfg.freeze_on_load);
        assert!(cfg.enable_hot_reload);
    }

    #[test]
    fn production_preset_uses_slow_reload() {
        let cfg = Preset::Production.to_config().unwrap();
        assert_eq!(cfg.max_layers, 16);
        assert_eq!(cfg.watch_interval_ms, 30_000);
        assert!(cfg.freeze_on_load);
        assert!(cfg.enable_hot_reload);
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::Development, Preset::Staging, Preset::Production] {
            let cfg = preset.to_config().unwrap();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }
}

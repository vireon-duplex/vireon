//! [`ConfigBuilder`] — fluent builder for [`ConfigurationSystemConfig`].
//!
//! See `DESIGN.md` Part 13 (configuration).

use crate::config::{ConfigurationSystemConfig, Preset};
use crate::error::ConfigError;

/// Fluent builder for [`ConfigurationSystemConfig`].
#[derive(Debug, Clone, Default)]
pub struct ConfigBuilder {
    cfg: ConfigurationSystemConfig,
}

impl ConfigBuilder {
    /// Start a new builder with default configuration.
    ///
    /// # Errors
    /// Returns [`ConfigError::InvalidConfig`] if default config fails
    /// validation.
    pub fn new() -> Result<Self, ConfigError> {
        Ok(Self {
            cfg: ConfigurationSystemConfig::new()?,
        })
    }

    /// Start from a preset.
    ///
    /// # Errors
    /// Returns [`ConfigError::InvalidConfig`] if preset fails validation.
    pub fn from_preset(preset: Preset) -> Result<Self, ConfigError> {
        Ok(Self {
            cfg: preset.to_config()?,
        })
    }

    /// Override the max layers.
    #[must_use]
    pub const fn max_layers(mut self, layers: u8) -> Self {
        self.cfg.max_layers = layers;
        self
    }

    /// Override the watch interval (ms).
    #[must_use]
    pub const fn watch_interval_ms(mut self, interval_ms: u64) -> Self {
        self.cfg.watch_interval_ms = interval_ms;
        self
    }

    /// Enable or disable hot reload.
    #[must_use]
    pub const fn enable_hot_reload(mut self, enable: bool) -> Self {
        self.cfg.enable_hot_reload = enable;
        self
    }

    /// Enable or disable freeze on load.
    #[must_use]
    pub const fn freeze_on_load(mut self, freeze: bool) -> Self {
        self.cfg.freeze_on_load = freeze;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`ConfigError::InvalidConfig`] if any field is out of
    /// range.
    pub fn build(self) -> Result<ConfigurationSystemConfig, ConfigError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        MAX_MAX_LAYERS, MAX_WATCH_INTERVAL_MS, MIN_MAX_LAYERS, MIN_WATCH_INTERVAL_MS,
    };

    #[test]
    fn new_builder_succeeds() {
        let builder = ConfigBuilder::new();
        assert!(builder.is_ok());
    }

    #[test]
    fn from_preset_succeeds() {
        for preset in [Preset::Development, Preset::Staging, Preset::Production] {
            let builder = ConfigBuilder::from_preset(preset);
            assert!(builder.is_ok());
        }
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = ConfigBuilder::new()
            .unwrap()
            .max_layers(12)
            .watch_interval_ms(10_000)
            .enable_hot_reload(false)
            .freeze_on_load(true)
            .build()
            .unwrap();
        assert_eq!(cfg.max_layers, 12);
        assert_eq!(cfg.watch_interval_ms, 10_000);
        assert!(!cfg.enable_hot_reload);
        assert!(cfg.freeze_on_load);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = ConfigBuilder::new()
            .unwrap()
            .watch_interval_ms(MIN_WATCH_INTERVAL_MS / 2) // too small
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = ConfigBuilder::new()
            .unwrap()
            .max_layers(MIN_MAX_LAYERS)
            .watch_interval_ms(MIN_WATCH_INTERVAL_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_layers, MIN_MAX_LAYERS);
        assert_eq!(cfg.watch_interval_ms, MIN_WATCH_INTERVAL_MS);

        let cfg = ConfigBuilder::new()
            .unwrap()
            .max_layers(MAX_MAX_LAYERS)
            .watch_interval_ms(MAX_WATCH_INTERVAL_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_layers, MAX_MAX_LAYERS);
        assert_eq!(cfg.watch_interval_ms, MAX_WATCH_INTERVAL_MS);
    }

    #[test]
    fn builder_from_preset_preserves_overrides() {
        let cfg = ConfigBuilder::from_preset(Preset::Production)
            .unwrap()
            .watch_interval_ms(60_000) // override
            .build()
            .unwrap();
        assert_eq!(cfg.watch_interval_ms, 60_000);
        assert_eq!(cfg.max_layers, 16); // from preset
    }

    #[test]
    fn builder_disables_flags() {
        let cfg = ConfigBuilder::new()
            .unwrap()
            .enable_hot_reload(false)
            .freeze_on_load(false)
            .build()
            .unwrap();
        assert!(!cfg.enable_hot_reload);
        assert!(!cfg.freeze_on_load);
    }

    #[test]
    fn default_builder_is_valid() {
        let cfg = ConfigBuilder::default().build().unwrap();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn builder_from_development_preset() {
        let cfg = ConfigBuilder::from_preset(Preset::Development)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(cfg.max_layers, 4);
        assert_eq!(cfg.watch_interval_ms, 1_000);
        assert!(!cfg.freeze_on_load);
    }
}

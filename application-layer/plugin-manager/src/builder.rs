//! [`PluginManagerBuilder`] — fluent builder for [`PluginManagerConfig`].
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `PluginManagerConfig` has several fields and most callers only need
//! to override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching
//! every field.

use crate::config::{PluginManagerConfig, Preset};
use crate::error::PluginError;

// ── PluginManagerBuilder ───────────────────────────────────────────

/// Fluent builder for [`PluginManagerConfig`].
#[derive(Debug, Clone)]
pub struct PluginManagerBuilder {
    cfg: PluginManagerConfig,
}

impl PluginManagerBuilder {
    /// Start a new builder with defaults.
    pub fn new() -> Self {
        Self {
            cfg: PluginManagerConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the max plugin count.
    #[must_use]
    pub const fn max_plugins(mut self, n: u32) -> Self {
        self.cfg.max_plugins = n;
        self
    }

    /// Enable or disable dev plugins.
    #[must_use]
    pub const fn allow_dev(mut self, allow: bool) -> Self {
        self.cfg.allow_dev_plugins = allow;
        self
    }

    /// Enable or disable strict API checking.
    #[must_use]
    pub const fn strict_api(mut self, strict: bool) -> Self {
        self.cfg.strict_api_check = strict;
        self
    }

    /// Enable or disable hot reload.
    #[must_use]
    pub const fn enable_hot_reload(mut self, enable: bool) -> Self {
        self.cfg.enable_hot_reload = enable;
        self
    }

    /// Set the scan interval for hot reload (ms).
    #[must_use]
    pub const fn scan_interval_ms(mut self, ms: u64) -> Self {
        self.cfg.scan_interval_ms = ms;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`PluginError::LoadFailed`] if any field is out of range.
    pub fn build(self) -> Result<PluginManagerConfig, PluginError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for PluginManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MAX_MAX_PLUGINS, MAX_SCAN_MS, MIN_MAX_PLUGINS, MIN_SCAN_MS};

    #[test]
    fn builder_default_is_valid() {
        let cfg = PluginManagerBuilder::new().build().unwrap();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn builder_from_preset() {
        let cfg = PluginManagerBuilder::from_preset(Preset::Strict)
            .build()
            .unwrap();
        assert!(!cfg.allow_dev_plugins);
        assert!(cfg.strict_api_check);
        assert!(!cfg.enable_hot_reload);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = PluginManagerBuilder::new()
            .max_plugins(100)
            .allow_dev(true)
            .strict_api(false)
            .enable_hot_reload(true)
            .scan_interval_ms(10_000)
            .build()
            .unwrap();
        assert_eq!(cfg.max_plugins, 100);
        assert!(cfg.allow_dev_plugins);
        assert!(!cfg.strict_api_check);
        assert!(cfg.enable_hot_reload);
        assert_eq!(cfg.scan_interval_ms, 10_000);
    }

    #[test]
    fn builder_validates_max_plugins() {
        let res = PluginManagerBuilder::new().max_plugins(0).build();
        assert!(res.is_err());

        let res = PluginManagerBuilder::new()
            .max_plugins(MAX_MAX_PLUGINS + 1)
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_validates_scan_interval_with_hot_reload() {
        let res = PluginManagerBuilder::new()
            .enable_hot_reload(true)
            .scan_interval_ms(MIN_SCAN_MS - 1)
            .build();
        assert!(res.is_err());

        let res = PluginManagerBuilder::new()
            .enable_hot_reload(true)
            .scan_interval_ms(MAX_SCAN_MS + 1)
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = PluginManagerBuilder::new()
            .max_plugins(MIN_MAX_PLUGINS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_plugins, MIN_MAX_PLUGINS);

        let cfg = PluginManagerBuilder::new()
            .max_plugins(MAX_MAX_PLUGINS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_plugins, MAX_MAX_PLUGINS);
    }

    #[test]
    fn builder_from_preset_preserves_overrides() {
        let cfg = PluginManagerBuilder::from_preset(Preset::Development)
            .max_plugins(50) // override
            .build()
            .unwrap();
        assert_eq!(cfg.max_plugins, 50);
        assert!(cfg.enable_hot_reload); // from preset
    }

    #[test]
    fn builder_allows_scan_interval_without_hot_reload() {
        // Scan interval is only validated when hot reload is enabled.
        let cfg = PluginManagerBuilder::new()
            .scan_interval_ms(100) // too small, but hot reload off
            .build()
            .unwrap();
        assert_eq!(cfg.scan_interval_ms, 100);
    }
}

//! [`PluginManagerConfig`], [`Preset`] — declarative configuration for
//! the plugin manager.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The three presets encode deployment archetypes for plugin management:
//!
//! | Preset | Dev Plugins | Strict API | Hot Reload | Scan Interval |
//! |--------|-------------|------------|-----------|---------------|
//! | `Strict` | Disallowed | Enabled | Disabled | (N/A) |
//! | `Lenient` | Allowed | Disabled | Disabled | (N/A) |
//! | `Development` | Allowed | Disabled | Enabled | 5s |

use crate::error::PluginError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of plugins the manager will support.
pub const MIN_MAX_PLUGINS: u32 = 1;

/// Maximum number of plugins the manager will support (hard cap).
pub const MAX_MAX_PLUGINS: u32 = 256;

/// Minimum scan interval for hot reload (ms).
pub const MIN_SCAN_MS: u64 = 1_000;

/// Maximum scan interval for hot reload (ms).
pub const MAX_SCAN_MS: u64 = 300_000;

// ── PluginManagerConfig ───────────────────────────────────────────

/// Declarative configuration for the plugin manager.
///
/// Constructed via [`PluginManagerConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PluginManagerConfig {
    /// Maximum number of plugins that can be loaded.
    pub max_plugins: u32,
    /// Whether to allow development (unsigned) plugins.
    pub allow_dev_plugins: bool,
    /// Whether to enforce strict API version checking.
    pub strict_api_check: bool,
    /// Whether to enable hot reload of plugins.
    pub enable_hot_reload: bool,
    /// Scan interval for hot reload (ms).
    pub scan_interval_ms: u64,
}

impl PluginManagerConfig {
    /// Construct a minimal config with safe defaults.
    ///
    /// Defaults: max_plugins 32, dev plugins disallowed, strict API
    /// check enabled, hot reload disabled.
    pub const fn new() -> Self {
        Self {
            max_plugins: 32,
            allow_dev_plugins: false,
            strict_api_check: true,
            enable_hot_reload: false,
            scan_interval_ms: 5_000,
        }
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`PluginError::LoadFailed`] if any field is out of range.
    pub fn validate(&self) -> Result<(), PluginError> {
        if self.max_plugins < MIN_MAX_PLUGINS || self.max_plugins > MAX_MAX_PLUGINS {
            return Err(PluginError::LoadFailed("max_plugins out of range"));
        }
        if self.enable_hot_reload
            && (self.scan_interval_ms < MIN_SCAN_MS || self.scan_interval_ms > MAX_SCAN_MS)
        {
            return Err(PluginError::LoadFailed("scan_interval_ms out of range"));
        }
        Ok(())
    }
}

impl Default for PluginManagerConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ─────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// plugin management profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Strict production mode: no dev plugins, strict API checking,
    /// no hot reload.
    Strict,
    /// Lenient mode: dev plugins allowed, relaxed API checking,
    /// no hot reload.
    Lenient,
    /// Development mode: all features enabled, short scan interval.
    Development,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub const fn to_config(self) -> PluginManagerConfig {
        match self {
            Self::Strict => PluginManagerConfig {
                max_plugins: 64,
                allow_dev_plugins: false,
                strict_api_check: true,
                enable_hot_reload: false,
                scan_interval_ms: 5_000,
            },
            Self::Lenient => PluginManagerConfig {
                max_plugins: 64,
                allow_dev_plugins: true,
                strict_api_check: false,
                enable_hot_reload: false,
                scan_interval_ms: 5_000,
            },
            Self::Development => PluginManagerConfig {
                max_plugins: 128,
                allow_dev_plugins: true,
                strict_api_check: false,
                enable_hot_reload: true,
                scan_interval_ms: 5_000,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = PluginManagerConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_plugins, 32);
        assert_eq!(cfg.allow_dev_plugins, false);
        assert_eq!(cfg.strict_api_check, true);
        assert_eq!(cfg.enable_hot_reload, false);
    }

    #[test]
    fn new_config_matches_defaults() {
        let cfg = PluginManagerConfig::new();
        let default = PluginManagerConfig::default();
        assert_eq!(cfg, default);
    }

    #[test]
    fn validate_rejects_bad_max_plugins() {
        let mut cfg = PluginManagerConfig::default();
        cfg.max_plugins = 0;
        assert!(cfg.validate().is_err());

        cfg.max_plugins = MAX_MAX_PLUGINS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_scan_interval_with_hot_reload() {
        let mut cfg = PluginManagerConfig::default();
        cfg.enable_hot_reload = true;
        cfg.scan_interval_ms = MIN_SCAN_MS - 1;
        assert!(cfg.validate().is_err());

        cfg.scan_interval_ms = MAX_SCAN_MS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_accepts_good_scan_interval() {
        let mut cfg = PluginManagerConfig::default();
        cfg.enable_hot_reload = true;
        cfg.scan_interval_ms = 10_000;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn strict_preset_disallows_dev_plugins() {
        let cfg = Preset::Strict.to_config();
        assert!(!cfg.allow_dev_plugins);
        assert!(cfg.strict_api_check);
        assert!(!cfg.enable_hot_reload);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn lenient_preset_allows_dev_plugins() {
        let cfg = Preset::Lenient.to_config();
        assert!(cfg.allow_dev_plugins);
        assert!(!cfg.strict_api_check);
        assert!(!cfg.enable_hot_reload);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn development_preset_enables_hot_reload() {
        let cfg = Preset::Development.to_config();
        assert!(cfg.allow_dev_plugins);
        assert!(!cfg.strict_api_check);
        assert!(cfg.enable_hot_reload);
        assert_eq!(cfg.scan_interval_ms, 5_000);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn all_presets_are_valid() {
        for preset in [Preset::Strict, Preset::Lenient, Preset::Development] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn preset_equality_works() {
        assert_eq!(Preset::Strict, Preset::Strict);
        assert_ne!(Preset::Strict, Preset::Lenient);
        assert_ne!(Preset::Lenient, Preset::Development);
    }
}

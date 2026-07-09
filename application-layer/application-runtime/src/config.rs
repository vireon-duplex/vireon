//! [`AppRuntimeConfig`], [`Preset`] — declarative configuration for
//! the application-runtime crate.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The three presets encode deployment archetypes for the runtime:
//!
//! | Preset | Modules | Startup | Shutdown | Health | Use case |
//! |--------|---------|---------|----------|--------|----------|
//! | `Development` | 64 | 10s | 5s | 1s | Local dev / testing |
//! | `Production` | 256 | 30s | 15s | 5s | Production deployment |
//! | `Minimal` | 8 | 5s | 2s | 10s | Edge / embedded |

use crate::error::AppRuntimeError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of modules the runtime will manage.
pub const MIN_MAX_MODULES: u32 = 1;
/// Maximum number of modules the runtime will manage (hard cap).
pub const MAX_MAX_MODULES: u32 = 1_024;

/// Minimum startup timeout in milliseconds (100ms).
pub const MIN_STARTUP_TIMEOUT_MS: u64 = 100;
/// Maximum startup timeout in milliseconds (5 minutes).
pub const MAX_STARTUP_TIMEOUT_MS: u64 = 300_000;

/// Minimum shutdown timeout in milliseconds (100ms).
pub const MIN_SHUTDOWN_TIMEOUT_MS: u64 = 100;
/// Maximum shutdown timeout in milliseconds (5 minutes).
pub const MAX_SHUTDOWN_TIMEOUT_MS: u64 = 300_000;

/// Minimum health check interval in milliseconds (100ms).
pub const MIN_HEALTH_CHECK_INTERVAL_MS: u64 = 100;
/// Maximum health check interval in milliseconds (5 minutes).
pub const MAX_HEALTH_CHECK_INTERVAL_MS: u64 = 300_000;

// ── AppRuntimeConfig ────────────────────────────────────────────

/// Declarative configuration for the application runtime.
///
/// Constructed via [`AppRuntimeConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppRuntimeConfig {
    /// Maximum number of modules the runtime may manage.
    pub max_modules: u32,
    /// Startup timeout in milliseconds.
    pub startup_timeout_ms: u64,
    /// Shutdown timeout in milliseconds.
    pub shutdown_timeout_ms: u64,
    /// Health check interval in milliseconds.
    pub health_check_interval_ms: u64,
}

impl AppRuntimeConfig {
    /// Construct a config with sensible production defaults.
    ///
    /// Defaults: 256 modules, 30s startup, 15s shutdown, 5s health.
    pub const fn new() -> Self {
        Self {
            max_modules: 256,
            startup_timeout_ms: 30_000,
            shutdown_timeout_ms: 15_000,
            health_check_interval_ms: 5_000,
        }
    }

    /// Builder-style: set the max modules.
    #[must_use]
    pub const fn with_max_modules(mut self, max: u32) -> Self {
        self.max_modules = max;
        self
    }

    /// Builder-style: set the startup timeout (ms).
    #[must_use]
    pub const fn with_startup_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.startup_timeout_ms = timeout_ms;
        self
    }

    /// Builder-style: set the shutdown timeout (ms).
    #[must_use]
    pub const fn with_shutdown_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.shutdown_timeout_ms = timeout_ms;
        self
    }

    /// Builder-style: set the health check interval (ms).
    #[must_use]
    pub const fn with_health_check_interval_ms(mut self, interval_ms: u64) -> Self {
        self.health_check_interval_ms = interval_ms;
        self
    }

    /// Validate the config.
    ///
    /// # Errors
    /// Returns [`AppRuntimeError::CompositionFailed`] if any field is
    /// out of range.
    pub fn validate(&self) -> Result<(), AppRuntimeError> {
        if self.max_modules < MIN_MAX_MODULES || self.max_modules > MAX_MAX_MODULES {
            return Err(AppRuntimeError::CompositionFailed(
                "max_modules out of range",
            ));
        }
        if self.startup_timeout_ms < MIN_STARTUP_TIMEOUT_MS
            || self.startup_timeout_ms > MAX_STARTUP_TIMEOUT_MS
        {
            return Err(AppRuntimeError::CompositionFailed(
                "startup_timeout_ms out of range",
            ));
        }
        if self.shutdown_timeout_ms < MIN_SHUTDOWN_TIMEOUT_MS
            || self.shutdown_timeout_ms > MAX_SHUTDOWN_TIMEOUT_MS
        {
            return Err(AppRuntimeError::CompositionFailed(
                "shutdown_timeout_ms out of range",
            ));
        }
        if self.health_check_interval_ms < MIN_HEALTH_CHECK_INTERVAL_MS
            || self.health_check_interval_ms > MAX_HEALTH_CHECK_INTERVAL_MS
        {
            return Err(AppRuntimeError::CompositionFailed(
                "health_check_interval_ms out of range",
            ));
        }
        Ok(())
    }
}

impl Default for AppRuntimeConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// runtime profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Development: 64 modules, 10s startup, 5s shutdown, 1s health.
    Development,
    /// Production: 256 modules, 30s startup, 15s shutdown, 5s health.
    Production,
    /// Minimal: 8 modules, 5s startup, 2s shutdown, 10s health.
    Minimal,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub const fn to_config(self) -> AppRuntimeConfig {
        match self {
            Self::Development => AppRuntimeConfig {
                max_modules: 64,
                startup_timeout_ms: 10_000,
                shutdown_timeout_ms: 5_000,
                health_check_interval_ms: 1_000,
            },
            Self::Production => AppRuntimeConfig {
                max_modules: 256,
                startup_timeout_ms: 30_000,
                shutdown_timeout_ms: 15_000,
                health_check_interval_ms: 5_000,
            },
            Self::Minimal => AppRuntimeConfig {
                max_modules: 8,
                startup_timeout_ms: 5_000,
                shutdown_timeout_ms: 2_000,
                health_check_interval_ms: 10_000,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = AppRuntimeConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_modules, 256);
        assert_eq!(cfg.startup_timeout_ms, 30_000);
        assert_eq!(cfg.shutdown_timeout_ms, 15_000);
        assert_eq!(cfg.health_check_interval_ms, 5_000);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = AppRuntimeConfig::new()
            .with_max_modules(128)
            .with_startup_timeout_ms(20_000)
            .with_shutdown_timeout_ms(10_000)
            .with_health_check_interval_ms(3_000);
        assert_eq!(cfg.max_modules, 128);
        assert_eq!(cfg.startup_timeout_ms, 20_000);
        assert_eq!(cfg.shutdown_timeout_ms, 10_000);
        assert_eq!(cfg.health_check_interval_ms, 3_000);
    }

    #[test]
    fn validate_rejects_bad_max_modules() {
        let mut cfg = AppRuntimeConfig::default();
        cfg.max_modules = 0;
        assert!(cfg.validate().is_err());
        cfg.max_modules = MAX_MAX_MODULES + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_startup_timeout() {
        let mut cfg = AppRuntimeConfig::default();
        cfg.startup_timeout_ms = MIN_STARTUP_TIMEOUT_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.startup_timeout_ms = MAX_STARTUP_TIMEOUT_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_shutdown_timeout() {
        let mut cfg = AppRuntimeConfig::default();
        cfg.shutdown_timeout_ms = MIN_SHUTDOWN_TIMEOUT_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.shutdown_timeout_ms = MAX_SHUTDOWN_TIMEOUT_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_health_check_interval() {
        let mut cfg = AppRuntimeConfig::default();
        cfg.health_check_interval_ms = MIN_HEALTH_CHECK_INTERVAL_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.health_check_interval_ms = MAX_HEALTH_CHECK_INTERVAL_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn development_preset() {
        let cfg = Preset::Development.to_config();
        assert_eq!(cfg.max_modules, 64);
        assert_eq!(cfg.startup_timeout_ms, 10_000);
        assert_eq!(cfg.shutdown_timeout_ms, 5_000);
        assert_eq!(cfg.health_check_interval_ms, 1_000);
    }

    #[test]
    fn production_preset() {
        let cfg = Preset::Production.to_config();
        assert_eq!(cfg.max_modules, 256);
        assert_eq!(cfg.startup_timeout_ms, 30_000);
        assert_eq!(cfg.shutdown_timeout_ms, 15_000);
        assert_eq!(cfg.health_check_interval_ms, 5_000);
    }

    #[test]
    fn minimal_preset() {
        let cfg = Preset::Minimal.to_config();
        assert_eq!(cfg.max_modules, 8);
        assert_eq!(cfg.startup_timeout_ms, 5_000);
        assert_eq!(cfg.shutdown_timeout_ms, 2_000);
        assert_eq!(cfg.health_check_interval_ms, 10_000);
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::Development, Preset::Production, Preset::Minimal] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }
}

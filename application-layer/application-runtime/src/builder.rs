//! [`AppRuntimeBuilder`] — fluent builder for [`AppRuntimeConfig`].
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `AppRuntimeConfig` has several fields and most callers only need
//! to override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching every
//! field.

use crate::config::{AppRuntimeConfig, Preset};
use crate::error::AppRuntimeError;

/// Fluent builder for [`AppRuntimeConfig`].
#[derive(Debug, Clone)]
pub struct AppRuntimeBuilder {
    cfg: AppRuntimeConfig,
}

impl AppRuntimeBuilder {
    /// Start a new builder with defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cfg: AppRuntimeConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub const fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the max modules.
    #[must_use]
    pub const fn max_modules(mut self, max: u32) -> Self {
        self.cfg.max_modules = max;
        self
    }

    /// Override the startup timeout (ms).
    #[must_use]
    pub const fn startup_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.cfg.startup_timeout_ms = timeout_ms;
        self
    }

    /// Override the shutdown timeout (ms).
    #[must_use]
    pub const fn shutdown_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.cfg.shutdown_timeout_ms = timeout_ms;
        self
    }

    /// Override the health check interval (ms).
    #[must_use]
    pub const fn health_check_interval_ms(mut self, interval_ms: u64) -> Self {
        self.cfg.health_check_interval_ms = interval_ms;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`AppRuntimeError::CompositionFailed`] if any field is
    /// out of range.
    pub fn build(self) -> Result<AppRuntimeConfig, AppRuntimeError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for AppRuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        MAX_HEALTH_CHECK_INTERVAL_MS, MAX_MAX_MODULES, MAX_SHUTDOWN_TIMEOUT_MS,
        MAX_STARTUP_TIMEOUT_MS, MIN_HEALTH_CHECK_INTERVAL_MS, MIN_MAX_MODULES,
        MIN_SHUTDOWN_TIMEOUT_MS, MIN_STARTUP_TIMEOUT_MS,
    };

    #[test]
    fn new_builder_has_defaults() {
        let cfg = AppRuntimeBuilder::new().build().unwrap();
        assert_eq!(cfg.max_modules, 256);
        assert_eq!(cfg.startup_timeout_ms, 30_000);
        assert_eq!(cfg.shutdown_timeout_ms, 15_000);
        assert_eq!(cfg.health_check_interval_ms, 5_000);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = AppRuntimeBuilder::new()
            .max_modules(128)
            .startup_timeout_ms(20_000)
            .shutdown_timeout_ms(10_000)
            .health_check_interval_ms(3_000)
            .build()
            .unwrap();
        assert_eq!(cfg.max_modules, 128);
        assert_eq!(cfg.startup_timeout_ms, 20_000);
        assert_eq!(cfg.shutdown_timeout_ms, 10_000);
        assert_eq!(cfg.health_check_interval_ms, 3_000);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = AppRuntimeBuilder::new()
            .max_modules(0) // too small
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = AppRuntimeBuilder::new()
            .max_modules(MIN_MAX_MODULES)
            .startup_timeout_ms(MIN_STARTUP_TIMEOUT_MS)
            .shutdown_timeout_ms(MIN_SHUTDOWN_TIMEOUT_MS)
            .health_check_interval_ms(MIN_HEALTH_CHECK_INTERVAL_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_modules, MIN_MAX_MODULES);
        assert_eq!(cfg.startup_timeout_ms, MIN_STARTUP_TIMEOUT_MS);

        let cfg = AppRuntimeBuilder::new()
            .max_modules(MAX_MAX_MODULES)
            .startup_timeout_ms(MAX_STARTUP_TIMEOUT_MS)
            .shutdown_timeout_ms(MAX_SHUTDOWN_TIMEOUT_MS)
            .health_check_interval_ms(MAX_HEALTH_CHECK_INTERVAL_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_modules, MAX_MAX_MODULES);
        assert_eq!(cfg.startup_timeout_ms, MAX_STARTUP_TIMEOUT_MS);
        assert_eq!(cfg.shutdown_timeout_ms, MAX_SHUTDOWN_TIMEOUT_MS);
        assert_eq!(cfg.health_check_interval_ms, MAX_HEALTH_CHECK_INTERVAL_MS);
    }

    #[test]
    fn builder_from_preset_development() {
        let cfg = AppRuntimeBuilder::from_preset(Preset::Development)
            .max_modules(32) // override
            .build()
            .unwrap();
        assert_eq!(cfg.max_modules, 32);
        assert_eq!(cfg.startup_timeout_ms, 10_000); // from preset
    }

    #[test]
    fn builder_from_preset_production() {
        let cfg = AppRuntimeBuilder::from_preset(Preset::Production)
            .build()
            .unwrap();
        assert_eq!(cfg.max_modules, 256);
        assert_eq!(cfg.startup_timeout_ms, 30_000);
    }

    #[test]
    fn builder_from_preset_minimal() {
        let cfg = AppRuntimeBuilder::from_preset(Preset::Minimal)
            .build()
            .unwrap();
        assert_eq!(cfg.max_modules, 8);
        assert_eq!(cfg.startup_timeout_ms, 5_000);
    }

    #[test]
    fn builder_default_is_same_as_new() {
        let a = AppRuntimeBuilder::new().build().unwrap();
        let b = AppRuntimeBuilder::default().build().unwrap();
        assert_eq!(a, b);
    }
}

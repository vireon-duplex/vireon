//! [`LifecycleBuilder`] — fluent builder for [`LifecycleConfig`].
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `LifecycleConfig` has many fields and most callers only need to
//! override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching
//! every field.

use crate::config::{LifecycleConfig, Preset};
use crate::error::ModuleLifecycleError;

/// Fluent builder for [`LifecycleConfig`].
#[derive(Debug, Clone)]
pub struct LifecycleBuilder {
    cfg: LifecycleConfig,
}

impl LifecycleBuilder {
    /// Start a new builder with defaults.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: LifecycleConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the max module count.
    #[must_use]
    pub const fn max_modules(mut self, n: u32) -> Self {
        self.cfg.max_modules = n;
        self
    }

    /// Override the init timeout (ms).
    #[must_use]
    pub const fn init_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg.init_timeout_ms = ms;
        self
    }

    /// Override the start timeout (ms).
    #[must_use]
    pub const fn start_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg.start_timeout_ms = ms;
        self
    }

    /// Override the stop timeout (ms).
    #[must_use]
    pub const fn stop_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg.stop_timeout_ms = ms;
        self
    }

    /// Enable or disable parallel init.
    #[must_use]
    pub const fn parallel_init(mut self, parallel: bool) -> Self {
        self.cfg.parallel_init = parallel;
        self
    }

    /// Enable or disable fail-fast.
    #[must_use]
    pub const fn fail_fast(mut self, fail_fast: bool) -> Self {
        self.cfg.fail_fast = fail_fast;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`ModuleLifecycleError::InvalidTransition`] if any field is out
    /// of range.
    pub fn build(self) -> Result<LifecycleConfig, ModuleLifecycleError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for LifecycleBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MAX_MAX_MODULES, MAX_TIMEOUT_MS, MIN_MAX_MODULES, MIN_TIMEOUT_MS};

    #[test]
    fn builder_starts_with_defaults() {
        let builder = LifecycleBuilder::new();
        assert_eq!(builder.cfg.max_modules, 256);
        assert_eq!(builder.cfg.init_timeout_ms, 5_000);
    }

    #[test]
    fn builder_from_preset_uses_preset_values() {
        let builder = LifecycleBuilder::from_preset(Preset::Eager);
        assert_eq!(builder.cfg.max_modules, 128);
        assert_eq!(builder.cfg.init_timeout_ms, 2_000);
        assert!(builder.cfg.parallel_init);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = LifecycleBuilder::from_preset(Preset::Lazy)
            .max_modules(64)
            .init_timeout_ms(15_000)
            .parallel_init(true)
            .fail_fast(true)
            .build()
            .unwrap();
        assert_eq!(cfg.max_modules, 64);
        assert_eq!(cfg.init_timeout_ms, 15_000);
        assert!(cfg.parallel_init);
        assert!(cfg.fail_fast);
        // Preset values that weren't overridden remain.
        assert_eq!(cfg.start_timeout_ms, 60_000);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = LifecycleBuilder::new()
            .max_modules(0) // invalid
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = LifecycleBuilder::new()
            .max_modules(MIN_MAX_MODULES)
            .init_timeout_ms(MIN_TIMEOUT_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_modules, MIN_MAX_MODULES);
        assert_eq!(cfg.init_timeout_ms, MIN_TIMEOUT_MS);

        let cfg = LifecycleBuilder::new()
            .max_modules(MAX_MAX_MODULES)
            .init_timeout_ms(MAX_TIMEOUT_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_modules, MAX_MAX_MODULES);
        assert_eq!(cfg.init_timeout_ms, MAX_TIMEOUT_MS);
    }

    #[test]
    fn builder_from_all_presets() {
        for preset in [Preset::Eager, Preset::Lazy, Preset::Critical] {
            let cfg = LifecycleBuilder::from_preset(preset).build().unwrap();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn builder_disables_flags() {
        let cfg = LifecycleBuilder::new()
            .parallel_init(false)
            .fail_fast(true)
            .build()
            .unwrap();
        assert!(!cfg.parallel_init);
        assert!(cfg.fail_fast);
    }

    #[test]
    fn builder_default_is_same_as_new() {
        let b1 = LifecycleBuilder::new();
        let b2 = LifecycleBuilder::default();
        assert_eq!(b1.cfg.max_modules, b2.cfg.max_modules);
        assert_eq!(b1.cfg.init_timeout_ms, b2.cfg.init_timeout_ms);
    }
}

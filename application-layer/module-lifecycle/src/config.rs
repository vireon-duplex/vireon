//! [`LifecycleConfig`], [`Preset`] — declarative configuration for
//! the module lifecycle system.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The three presets encode deployment archetypes for different
//! module load profiles:
//!
//! | Preset | Parallel Init | Timeouts | Fail-fast |
//!--------|---------------|----------|-----------|
//! | `Eager` | Yes | Short | No |
//! | `Lazy` | No | Long | No |
//! | `Critical` | Yes | Short | Yes |

use crate::error::ModuleLifecycleError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of modules the lifecycle system will accept.
pub const MIN_MAX_MODULES: u32 = 1;

/// Maximum number of modules the lifecycle system will accept.
pub const MAX_MAX_MODULES: u32 = 1_024;

/// Minimum timeout for any lifecycle phase (ms).
pub const MIN_TIMEOUT_MS: u64 = 100;

/// Maximum timeout for any lifecycle phase (ms).
pub const MAX_TIMEOUT_MS: u64 = 300_000;

// ── LifecycleConfig ──────────────────────────────────────────────

/// Declarative configuration for the module lifecycle system.
///
/// Constructed via [`LifecycleConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LifecycleConfig {
    /// Maximum number of modules that can be registered.
    pub max_modules: u32,
    /// Initialization timeout per module (ms).
    pub init_timeout_ms: u64,
    /// Startup timeout per module (ms).
    pub start_timeout_ms: u64,
    /// Shutdown timeout per module (ms).
    pub stop_timeout_ms: u64,
    /// Whether to initialize modules in parallel.
    pub parallel_init: bool,
    /// Whether to fail fast on first error or continue.
    pub fail_fast: bool,
}

impl LifecycleConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: 256 max modules, 5s init timeout, 10s start timeout,
    /// 30s stop timeout, parallel init enabled, fail-fast disabled.
    pub const fn new() -> Self {
        Self {
            max_modules: 256,
            init_timeout_ms: 5_000,
            start_timeout_ms: 10_000,
            stop_timeout_ms: 30_000,
            parallel_init: true,
            fail_fast: false,
        }
    }

    /// Builder-style: set the max module count.
    #[must_use]
    pub const fn with_max_modules(mut self, n: u32) -> Self {
        self.max_modules = n;
        self
    }

    /// Builder-style: set the init timeout (ms).
    #[must_use]
    pub const fn with_init_timeout_ms(mut self, ms: u64) -> Self {
        self.init_timeout_ms = ms;
        self
    }

    /// Builder-style: set the start timeout (ms).
    #[must_use]
    pub const fn with_start_timeout_ms(mut self, ms: u64) -> Self {
        self.start_timeout_ms = ms;
        self
    }

    /// Builder-style: set the stop timeout (ms).
    #[must_use]
    pub const fn with_stop_timeout_ms(mut self, ms: u64) -> Self {
        self.stop_timeout_ms = ms;
        self
    }

    /// Builder-style: enable or disable parallel init.
    #[must_use]
    pub const fn with_parallel_init(mut self, parallel: bool) -> Self {
        self.parallel_init = parallel;
        self
    }

    /// Builder-style: enable or disable fail-fast.
    #[must_use]
    pub const fn with_fail_fast(mut self, fail_fast: bool) -> Self {
        self.fail_fast = fail_fast;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`ModuleLifecycleError::InvalidConfig`] if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), ModuleLifecycleError> {
        if self.max_modules < MIN_MAX_MODULES || self.max_modules > MAX_MAX_MODULES {
            return Err(ModuleLifecycleError::InvalidTransition(
                format!(
                    "max_modules {n} out of range [{MIN_MAX_MODULES}, {MAX_MAX_MODULES}]",
                    n = self.max_modules
                )
                .leak(),
            ));
        }
        if self.init_timeout_ms < MIN_TIMEOUT_MS || self.init_timeout_ms > MAX_TIMEOUT_MS {
            return Err(ModuleLifecycleError::InvalidTransition(
                format!(
                    "init_timeout_ms {t} out of range [{MIN_TIMEOUT_MS}, {MAX_TIMEOUT_MS}]",
                    t = self.init_timeout_ms
                )
                .leak(),
            ));
        }
        if self.start_timeout_ms < MIN_TIMEOUT_MS || self.start_timeout_ms > MAX_TIMEOUT_MS {
            return Err(ModuleLifecycleError::InvalidTransition(
                format!(
                    "start_timeout_ms {t} out of range [{MIN_TIMEOUT_MS}, {MAX_TIMEOUT_MS}]",
                    t = self.start_timeout_ms
                )
                .leak(),
            ));
        }
        if self.stop_timeout_ms < MIN_TIMEOUT_MS || self.stop_timeout_ms > MAX_TIMEOUT_MS {
            return Err(ModuleLifecycleError::InvalidTransition(
                format!(
                    "stop_timeout_ms {t} out of range [{MIN_TIMEOUT_MS}, {MAX_TIMEOUT_MS}]",
                    t = self.stop_timeout_ms
                )
                .leak(),
            ));
        }
        Ok(())
    }
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// module load profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Fast parallel initialization, short timeouts. Good for dev/testing.
    Eager,
    /// Sequential initialization, long timeouts. Good for production.
    Lazy,
    /// Fail-fast enabled, short timeouts. Good for critical systems.
    Critical,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub const fn to_config(self) -> LifecycleConfig {
        match self {
            Self::Eager => LifecycleConfig {
                max_modules: 128,
                init_timeout_ms: 2_000,
                start_timeout_ms: 5_000,
                stop_timeout_ms: 10_000,
                parallel_init: true,
                fail_fast: false,
            },
            Self::Lazy => LifecycleConfig {
                max_modules: 512,
                init_timeout_ms: 30_000,
                start_timeout_ms: 60_000,
                stop_timeout_ms: 120_000,
                parallel_init: false,
                fail_fast: false,
            },
            Self::Critical => LifecycleConfig {
                max_modules: 256,
                init_timeout_ms: 3_000,
                start_timeout_ms: 5_000,
                stop_timeout_ms: 15_000,
                parallel_init: true,
                fail_fast: true,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = LifecycleConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_modules, 256);
        assert_eq!(cfg.init_timeout_ms, 5_000);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = LifecycleConfig::new()
            .with_max_modules(128)
            .with_init_timeout_ms(10_000)
            .with_parallel_init(false)
            .with_fail_fast(true);
        assert_eq!(cfg.max_modules, 128);
        assert_eq!(cfg.init_timeout_ms, 10_000);
        assert!(!cfg.parallel_init);
        assert!(cfg.fail_fast);
    }

    #[test]
    fn validate_rejects_bad_max_modules() {
        let mut cfg = LifecycleConfig::default();
        cfg.max_modules = 0;
        assert!(cfg.validate().is_err());
        cfg.max_modules = MAX_MAX_MODULES + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_init_timeout() {
        let mut cfg = LifecycleConfig::default();
        cfg.init_timeout_ms = MIN_TIMEOUT_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.init_timeout_ms = MAX_TIMEOUT_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_start_timeout() {
        let mut cfg = LifecycleConfig::default();
        cfg.start_timeout_ms = MIN_TIMEOUT_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.start_timeout_ms = MAX_TIMEOUT_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_stop_timeout() {
        let mut cfg = LifecycleConfig::default();
        cfg.stop_timeout_ms = MIN_TIMEOUT_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.stop_timeout_ms = MAX_TIMEOUT_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn eager_preset_uses_parallel_init() {
        let cfg = Preset::Eager.to_config();
        assert!(cfg.parallel_init);
        assert!(!cfg.fail_fast);
        assert_eq!(cfg.max_modules, 128);
    }

    #[test]
    fn lazy_preset_uses_sequential_init() {
        let cfg = Preset::Lazy.to_config();
        assert!(!cfg.parallel_init);
        assert!(!cfg.fail_fast);
        assert_eq!(cfg.init_timeout_ms, 30_000);
    }

    #[test]
    fn critical_preset_enables_fail_fast() {
        let cfg = Preset::Critical.to_config();
        assert!(cfg.parallel_init);
        assert!(cfg.fail_fast);
        assert_eq!(cfg.init_timeout_ms, 3_000);
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::Eager, Preset::Lazy, Preset::Critical] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn config_is_const_constructible() {
        const CFG: LifecycleConfig = LifecycleConfig::new().with_max_modules(64);
        assert_eq!(CFG.max_modules, 64);
    }
}

//! [`MiddlewareConfig`], [`Preset`] — declarative configuration for
//! the middleware pipeline system.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The three presets encode deployment archetypes for different
//! middleware load profiles:
//!
//! | Preset | Stages | Middlewares | Short-circuit |
//!--------|--------|--------------|---------------|
//! | `Minimal` | Auth only | 16 | No |
//! | `Standard` | Auth + Dispatch | 64 | Yes |
//! | `Full` | All stages | 128 | Yes |

use crate::error::MiddlewareError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of middlewares the pipeline will accept.
pub const MIN_MAX_MIDDLEWARES: u32 = 1;

/// Maximum number of middlewares the pipeline will accept.
pub const MAX_MAX_MIDDLEWARES: u32 = 1_024;

/// Minimum number of middlewares per stage.
pub const MIN_MAX_PER_STAGE: u8 = 1;

/// Maximum number of middlewares per stage.
pub const MAX_MAX_PER_STAGE: u8 = 128;

/// Minimum timeout for any middleware operation (ms).
pub const MIN_TIMEOUT_MS: u64 = 100;

/// Maximum timeout for any middleware operation (ms).
pub const MAX_TIMEOUT_MS: u64 = 300_000;

// ── MiddlewareConfig ─────────────────────────────────────────────

/// Declarative configuration for the middleware pipeline system.
///
/// Constructed via [`MiddlewareConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MiddlewareConfig {
    /// Maximum number of middlewares that can be registered.
    pub max_middlewares: u32,
    /// Maximum middlewares per stage.
    pub max_per_stage: u8,
    /// Whether to enable short-circuit (early exit on rejection).
    pub enable_short_circuit: bool,
    /// Default timeout per middleware execution (ms).
    pub default_timeout_ms: u64,
}

impl MiddlewareConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: 64 max middlewares, 16 per stage, short-circuit
    /// enabled, 5s default timeout.
    pub const fn new() -> Self {
        Self {
            max_middlewares: 64,
            max_per_stage: 16,
            enable_short_circuit: true,
            default_timeout_ms: 5_000,
        }
    }

    /// Builder-style: set the max middleware count.
    #[must_use]
    pub const fn with_max_middlewares(mut self, n: u32) -> Self {
        self.max_middlewares = n;
        self
    }

    /// Builder-style: set the max per-stage count.
    #[must_use]
    pub const fn with_max_per_stage(mut self, n: u8) -> Self {
        self.max_per_stage = n;
        self
    }

    /// Builder-style: enable or disable short-circuit.
    #[must_use]
    pub const fn with_short_circuit(mut self, enable: bool) -> Self {
        self.enable_short_circuit = enable;
        self
    }

    /// Builder-style: set the default timeout (ms).
    #[must_use]
    pub const fn with_default_timeout_ms(mut self, ms: u64) -> Self {
        self.default_timeout_ms = ms;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`MiddlewareError::InvalidOrder`] if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), MiddlewareError> {
        if self.max_middlewares < MIN_MAX_MIDDLEWARES || self.max_middlewares > MAX_MAX_MIDDLEWARES
        {
            return Err(MiddlewareError::InvalidOrder(
                format!(
                    "max_middlewares {n} out of range [{MIN_MAX_MIDDLEWARES}, {MAX_MAX_MIDDLEWARES}]",
                    n = self.max_middlewares
                )
                .leak(),
            ));
        }
        if self.max_per_stage < MIN_MAX_PER_STAGE || self.max_per_stage > MAX_MAX_PER_STAGE {
            return Err(MiddlewareError::InvalidOrder(
                format!(
                    "max_per_stage {n} out of range [{MIN_MAX_PER_STAGE}, {MAX_MAX_PER_STAGE}]",
                    n = self.max_per_stage
                )
                .leak(),
            ));
        }
        if self.default_timeout_ms < MIN_TIMEOUT_MS || self.default_timeout_ms > MAX_TIMEOUT_MS {
            return Err(MiddlewareError::InvalidOrder(
                format!(
                    "default_timeout_ms {t} out of range [{MIN_TIMEOUT_MS}, {MAX_TIMEOUT_MS}]",
                    t = self.default_timeout_ms
                )
                .leak(),
            ));
        }
        Ok(())
    }
}

impl Default for MiddlewareConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ─────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// middleware load profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Minimal: authentication only, few middlewares.
    Minimal,
    /// Standard: auth + dispatch stages, moderate middlewares.
    Standard,
    /// Full: all stages, many middlewares.
    Full,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub const fn to_config(self) -> MiddlewareConfig {
        match self {
            Self::Minimal => MiddlewareConfig {
                max_middlewares: 16,
                max_per_stage: 4,
                enable_short_circuit: false,
                default_timeout_ms: 3_000,
            },
            Self::Standard => MiddlewareConfig {
                max_middlewares: 64,
                max_per_stage: 16,
                enable_short_circuit: true,
                default_timeout_ms: 5_000,
            },
            Self::Full => MiddlewareConfig {
                max_middlewares: 128,
                max_per_stage: 32,
                enable_short_circuit: true,
                default_timeout_ms: 5_000,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = MiddlewareConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_middlewares, 64);
        assert_eq!(cfg.max_per_stage, 16);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = MiddlewareConfig::new()
            .with_max_middlewares(128)
            .with_max_per_stage(32)
            .with_short_circuit(false)
            .with_default_timeout_ms(10_000);
        assert_eq!(cfg.max_middlewares, 128);
        assert_eq!(cfg.max_per_stage, 32);
        assert!(!cfg.enable_short_circuit);
        assert_eq!(cfg.default_timeout_ms, 10_000);
    }

    #[test]
    fn validate_rejects_bad_max_middlewares() {
        let mut cfg = MiddlewareConfig::default();
        cfg.max_middlewares = 0;
        assert!(cfg.validate().is_err());
        cfg.max_middlewares = MAX_MAX_MIDDLEWARES + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_max_per_stage() {
        let mut cfg = MiddlewareConfig::default();
        cfg.max_per_stage = 0;
        assert!(cfg.validate().is_err());
        cfg.max_per_stage = MAX_MAX_PER_STAGE + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_timeout() {
        let mut cfg = MiddlewareConfig::default();
        cfg.default_timeout_ms = MIN_TIMEOUT_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.default_timeout_ms = MAX_TIMEOUT_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn minimal_preset_uses_few_middlewares() {
        let cfg = Preset::Minimal.to_config();
        assert_eq!(cfg.max_middlewares, 16);
        assert_eq!(cfg.max_per_stage, 4);
        assert!(!cfg.enable_short_circuit);
    }

    #[test]
    fn standard_preset_enables_short_circuit() {
        let cfg = Preset::Standard.to_config();
        assert_eq!(cfg.max_middlewares, 64);
        assert_eq!(cfg.max_per_stage, 16);
        assert!(cfg.enable_short_circuit);
    }

    #[test]
    fn full_preset_uses_many_middlewares() {
        let cfg = Preset::Full.to_config();
        assert_eq!(cfg.max_middlewares, 128);
        assert_eq!(cfg.max_per_stage, 32);
        assert!(cfg.enable_short_circuit);
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::Minimal, Preset::Standard, Preset::Full] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn config_is_const_constructible() {
        const CFG: MiddlewareConfig = MiddlewareConfig::new().with_max_middlewares(32);
        assert_eq!(CFG.max_middlewares, 32);
    }
}

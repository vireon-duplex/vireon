//! [`MiddlewarePipelineBuilder`] — fluent builder for [`MiddlewareConfig`].
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `MiddlewareConfig` has many fields and most callers only need to
//! override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching
//! every field.

use crate::config::{MiddlewareConfig, Preset};
use crate::error::MiddlewareError;

/// Fluent builder for [`MiddlewareConfig`].
#[derive(Debug, Clone)]
pub struct MiddlewarePipelineBuilder {
    cfg: MiddlewareConfig,
}

impl MiddlewarePipelineBuilder {
    /// Start a new builder with defaults.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: MiddlewareConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the max middleware count.
    #[must_use]
    pub const fn max_middlewares(mut self, n: u32) -> Self {
        self.cfg.max_middlewares = n;
        self
    }

    /// Override the max per-stage count.
    #[must_use]
    pub const fn max_per_stage(mut self, n: u8) -> Self {
        self.cfg.max_per_stage = n;
        self
    }

    /// Enable or disable short-circuit.
    #[must_use]
    pub const fn short_circuit(mut self, enable: bool) -> Self {
        self.cfg.enable_short_circuit = enable;
        self
    }

    /// Override the default timeout (ms).
    #[must_use]
    pub const fn default_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg.default_timeout_ms = ms;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`MiddlewareError::InvalidOrder`] if any field is out
    /// of range.
    pub fn build(self) -> Result<MiddlewareConfig, MiddlewareError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for MiddlewarePipelineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        MAX_MAX_MIDDLEWARES, MAX_MAX_PER_STAGE, MAX_TIMEOUT_MS, MIN_MAX_MIDDLEWARES,
        MIN_MAX_PER_STAGE, MIN_TIMEOUT_MS,
    };

    #[test]
    fn builder_starts_with_defaults() {
        let builder = MiddlewarePipelineBuilder::new();
        assert_eq!(builder.cfg.max_middlewares, 64);
        assert_eq!(builder.cfg.max_per_stage, 16);
    }

    #[test]
    fn builder_from_preset_uses_preset_values() {
        let builder = MiddlewarePipelineBuilder::from_preset(Preset::Minimal);
        assert_eq!(builder.cfg.max_middlewares, 16);
        assert_eq!(builder.cfg.max_per_stage, 4);
        assert!(!builder.cfg.enable_short_circuit);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = MiddlewarePipelineBuilder::from_preset(Preset::Standard)
            .max_middlewares(128)
            .max_per_stage(32)
            .short_circuit(false)
            .default_timeout_ms(10_000)
            .build()
            .unwrap();
        assert_eq!(cfg.max_middlewares, 128);
        assert_eq!(cfg.max_per_stage, 32);
        assert!(!cfg.enable_short_circuit);
        assert_eq!(cfg.default_timeout_ms, 10_000);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = MiddlewarePipelineBuilder::new()
            .max_middlewares(0) // invalid
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = MiddlewarePipelineBuilder::new()
            .max_middlewares(MIN_MAX_MIDDLEWARES)
            .max_per_stage(MIN_MAX_PER_STAGE)
            .default_timeout_ms(MIN_TIMEOUT_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_middlewares, MIN_MAX_MIDDLEWARES);
        assert_eq!(cfg.max_per_stage, MIN_MAX_PER_STAGE);
        assert_eq!(cfg.default_timeout_ms, MIN_TIMEOUT_MS);

        let cfg = MiddlewarePipelineBuilder::new()
            .max_middlewares(MAX_MAX_MIDDLEWARES)
            .max_per_stage(MAX_MAX_PER_STAGE)
            .default_timeout_ms(MAX_TIMEOUT_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_middlewares, MAX_MAX_MIDDLEWARES);
        assert_eq!(cfg.max_per_stage, MAX_MAX_PER_STAGE);
        assert_eq!(cfg.default_timeout_ms, MAX_TIMEOUT_MS);
    }

    #[test]
    fn builder_from_all_presets() {
        for preset in [Preset::Minimal, Preset::Standard, Preset::Full] {
            let cfg = MiddlewarePipelineBuilder::from_preset(preset)
                .build()
                .unwrap();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn builder_disables_short_circuit() {
        let cfg = MiddlewarePipelineBuilder::new()
            .short_circuit(false)
            .build()
            .unwrap();
        assert!(!cfg.enable_short_circuit);
    }

    #[test]
    fn builder_default_is_same_as_new() {
        let b1 = MiddlewarePipelineBuilder::new();
        let b2 = MiddlewarePipelineBuilder::default();
        assert_eq!(b1.cfg.max_middlewares, b2.cfg.max_middlewares);
        assert_eq!(b1.cfg.max_per_stage, b2.cfg.max_per_stage);
    }
}

//! [`FeatureFlagsBuilder`] — fluent builder for feature flag config.
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `FeatureFlagsConfig` has multiple fields and most callers only need
//! to override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching every
//! field.

use crate::config::{FeatureFlagsConfig, Preset};

/// Fluent builder for [`FeatureFlagsConfig`].
#[derive(Clone, Debug)]
pub struct FeatureFlagsBuilder {
    cfg: FeatureFlagsConfig,
}

impl FeatureFlagsBuilder {
    /// Start a new builder with default config.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: FeatureFlagsConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the max flags.
    #[must_use]
    pub const fn max_flags(mut self, n: u32) -> Self {
        self.cfg.max_flags = n;
        self
    }

    /// Override the cache TTL (ms).
    #[must_use]
    pub const fn cache_ttl_ms(mut self, ms: u64) -> Self {
        self.cfg.evaluation_cache_ttl_ms = ms;
        self
    }

    /// Enable or disable rules.
    #[must_use]
    pub const fn enable_rules(mut self, enable: bool) -> Self {
        self.cfg.enable_rules = enable;
        self
    }

    /// Set the default offline value.
    #[must_use]
    pub const fn default_offline_value(mut self, val: bool) -> Self {
        self.cfg.default_offline_value = val;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns `FlagError::InvalidValue` if any field is out of range.
    pub fn build(self) -> Result<FeatureFlagsConfig, crate::error::FlagError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for FeatureFlagsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MAX_CACHE_TTL_MS, MAX_FLAGS, MIN_CACHE_TTL_MS, MIN_FLAGS};

    #[test]
    fn builder_from_default() {
        let builder = FeatureFlagsBuilder::new();
        let cfg = builder.build().unwrap();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn builder_from_preset() {
        let builder = FeatureFlagsBuilder::from_preset(Preset::HighPerformance);
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_flags, 50_000);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = FeatureFlagsBuilder::from_preset(Preset::Default)
            .max_flags(25_000)
            .cache_ttl_ms(90_000)
            .enable_rules(false)
            .default_offline_value(true)
            .build()
            .unwrap();
        assert_eq!(cfg.max_flags, 25_000);
        assert_eq!(cfg.evaluation_cache_ttl_ms, 90_000);
        assert!(!cfg.enable_rules);
        assert!(cfg.default_offline_value);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = FeatureFlagsBuilder::new()
            .max_flags(5) // too small
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = FeatureFlagsBuilder::new()
            .max_flags(MIN_FLAGS)
            .cache_ttl_ms(MIN_CACHE_TTL_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_flags, MIN_FLAGS);
        assert_eq!(cfg.evaluation_cache_ttl_ms, MIN_CACHE_TTL_MS);

        let cfg = FeatureFlagsBuilder::new()
            .max_flags(MAX_FLAGS)
            .cache_ttl_ms(MAX_CACHE_TTL_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_flags, MAX_FLAGS);
        assert_eq!(cfg.evaluation_cache_ttl_ms, MAX_CACHE_TTL_MS);
    }

    #[test]
    fn builder_preserves_preset_overrides() {
        let cfg = FeatureFlagsBuilder::from_preset(Preset::Minimal)
            .max_flags(5_000) // override
            .build()
            .unwrap();
        assert_eq!(cfg.max_flags, 5_000);
        assert!(!cfg.enable_rules); // from preset
        assert!(cfg.default_offline_value); // from preset
    }

    #[test]
    fn builder_default_impl() {
        let builder = FeatureFlagsBuilder::default();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_flags, 10_000);
    }

    #[test]
    fn builder_clone() {
        let builder1 = FeatureFlagsBuilder::new();
        let builder2 = builder1.clone();
        let cfg1 = builder1.build().unwrap();
        let cfg2 = builder2.build().unwrap();
        assert_eq!(cfg1, cfg2);
    }
}

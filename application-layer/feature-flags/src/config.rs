//! [`FeatureFlagsConfig`], [`Preset`] — configuration for feature flags.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The presets encode common deployment patterns:
//!
//! | Preset | max_flags | cache_ttl_ms | enable_rules | default_offline |
//! |--------|-----------|--------------|-------------|-----------------|
//! | `Default` | 10,000 | 60,000 | true | false |
//! | `HighPerformance` | 50,000 | 30,000 | true | false |
//! | `Minimal` | 1,000 | 120,000 | false | true |

use crate::error::FlagError;

// ── Constants ─────────────────────────────────────────────────────

/// Minimum number of flags the registry can store.
pub const MIN_FLAGS: u32 = 10;

/// Maximum number of flags the registry can store.
pub const MAX_FLAGS: u32 = 1_000_000;

/// Minimum cache TTL in milliseconds.
pub const MIN_CACHE_TTL_MS: u64 = 1_000;

/// Maximum cache TTL in milliseconds.
pub const MAX_CACHE_TTL_MS: u64 = 3_600_000;

// ── FeatureFlagsConfig ─────────────────────────────────────────────

/// Configuration for the feature flag system.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeatureFlagsConfig {
    /// Maximum number of flags in the registry.
    pub max_flags: u32,
    /// Evaluation cache TTL in milliseconds.
    pub evaluation_cache_ttl_ms: u64,
    /// Whether rule-based evaluation is enabled.
    pub enable_rules: bool,
    /// Default value for flags when offline (no provider).
    pub default_offline_value: bool,
}

impl FeatureFlagsConfig {
    /// Construct a default config.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_flags: 10_000,
            evaluation_cache_ttl_ms: 60_000,
            enable_rules: true,
            default_offline_value: false,
        }
    }

    /// Validate the config.
    ///
    /// # Errors
    /// Returns `FlagError::InvalidValue` if any field is out of range.
    pub fn validate(&self) -> Result<(), FlagError> {
        if self.max_flags < MIN_FLAGS || self.max_flags > MAX_FLAGS {
            return Err(FlagError::InvalidValue(format!(
                "max_flags {} out of range [{}, {}]",
                self.max_flags, MIN_FLAGS, MAX_FLAGS
            )));
        }
        if self.evaluation_cache_ttl_ms < MIN_CACHE_TTL_MS
            || self.evaluation_cache_ttl_ms > MAX_CACHE_TTL_MS
        {
            return Err(FlagError::InvalidValue(format!(
                "evaluation_cache_ttl_ms {} out of range [{}, {}]",
                self.evaluation_cache_ttl_ms, MIN_CACHE_TTL_MS, MAX_CACHE_TTL_MS
            )));
        }
        Ok(())
    }

    /// Builder-style: set max flags.
    #[must_use]
    pub const fn with_max_flags(mut self, n: u32) -> Self {
        self.max_flags = n;
        self
    }

    /// Builder-style: set cache TTL.
    #[must_use]
    pub const fn with_cache_ttl_ms(mut self, ms: u64) -> Self {
        self.evaluation_cache_ttl_ms = ms;
        self
    }

    /// Builder-style: set enable rules.
    #[must_use]
    pub const fn with_enable_rules(mut self, enable: bool) -> Self {
        self.enable_rules = enable;
        self
    }

    /// Builder-style: set default offline value.
    #[must_use]
    pub const fn with_default_offline_value(mut self, val: bool) -> Self {
        self.default_offline_value = val;
        self
    }
}

impl Default for FeatureFlagsConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ───────────────────────────────────────────────────────

/// Deployment presets for feature flag configuration.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Default configuration: balanced for most use cases.
    Default,
    /// High-performance: more flags, shorter cache TTL.
    HighPerformance,
    /// Minimal: few flags, longer cache TTL, rules disabled.
    Minimal,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub fn to_config(self) -> FeatureFlagsConfig {
        let base = FeatureFlagsConfig::new();
        match self {
            Self::Default => base,
            Self::HighPerformance => base.with_max_flags(50_000).with_cache_ttl_ms(30_000),
            Self::Minimal => base
                .with_max_flags(1_000)
                .with_cache_ttl_ms(120_000)
                .with_enable_rules(false)
                .with_default_offline_value(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = FeatureFlagsConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_flags, 10_000);
        assert_eq!(cfg.evaluation_cache_ttl_ms, 60_000);
        assert!(cfg.enable_rules);
        assert!(!cfg.default_offline_value);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = FeatureFlagsConfig::new()
            .with_max_flags(20_000)
            .with_cache_ttl_ms(30_000)
            .with_enable_rules(false)
            .with_default_offline_value(true);
        assert_eq!(cfg.max_flags, 20_000);
        assert_eq!(cfg.evaluation_cache_ttl_ms, 30_000);
        assert!(!cfg.enable_rules);
        assert!(cfg.default_offline_value);
    }

    #[test]
    fn validate_rejects_bad_max_flags() {
        let cfg = FeatureFlagsConfig::new().with_max_flags(5);
        assert!(cfg.validate().is_err());

        let cfg = FeatureFlagsConfig::new().with_max_flags(MAX_FLAGS + 1);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_cache_ttl() {
        let cfg = FeatureFlagsConfig::new().with_cache_ttl_ms(500);
        assert!(cfg.validate().is_err());

        let cfg = FeatureFlagsConfig::new().with_cache_ttl_ms(MAX_CACHE_TTL_MS * 2);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_accepts_boundary_values() {
        let cfg = FeatureFlagsConfig::new()
            .with_max_flags(MIN_FLAGS)
            .with_cache_ttl_ms(MIN_CACHE_TTL_MS);
        assert!(cfg.validate().is_ok());

        let cfg = FeatureFlagsConfig::new()
            .with_max_flags(MAX_FLAGS)
            .with_cache_ttl_ms(MAX_CACHE_TTL_MS);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn default_preset_matches_default_config() {
        let preset_cfg = Preset::Default.to_config();
        let default_cfg = FeatureFlagsConfig::default();
        assert_eq!(preset_cfg, default_cfg);
    }

    #[test]
    fn high_performance_preset() {
        let cfg = Preset::HighPerformance.to_config();
        assert_eq!(cfg.max_flags, 50_000);
        assert_eq!(cfg.evaluation_cache_ttl_ms, 30_000);
        assert!(cfg.enable_rules);
    }

    #[test]
    fn minimal_preset() {
        let cfg = Preset::Minimal.to_config();
        assert_eq!(cfg.max_flags, 1_000);
        assert_eq!(cfg.evaluation_cache_ttl_ms, 120_000);
        assert!(!cfg.enable_rules);
        assert!(cfg.default_offline_value);
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::Default, Preset::HighPerformance, Preset::Minimal] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {:?} produced invalid config",
                preset
            );
        }
    }

    #[test]
    fn config_copy_and_clone() {
        let cfg1 = FeatureFlagsConfig::new();
        let cfg2 = cfg1;
        assert_eq!(cfg1, cfg2);
    }

    #[test]
    fn preset_hash() {
        // Ensure Preset derives Hash.
        let _ = std::collections::HashSet::from([Preset::Default, Preset::Minimal]);
    }
}

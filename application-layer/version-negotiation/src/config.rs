//! [`VersionNegotiationConfig`], [`Preset`] — declarative configuration
//! for the version negotiation system.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).

use crate::error::VersionNegotiationError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum negotiation timeout in milliseconds.
pub const MIN_TIMEOUT_MS: u64 = 100;

/// Maximum negotiation timeout in milliseconds (1 minute).
pub const MAX_TIMEOUT_MS: u64 = 60_000;

// ── VersionNegotiationConfig ─────────────────────────────────────

/// Declarative configuration for the version negotiation system.
///
/// Constructed via [`VersionNegotiationConfig::new`] or obtained from
/// a [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VersionNegotiationConfig {
    /// Maximum supported version number.
    pub max_supported: u32,
    /// Minimum supported version number.
    pub min_supported: u32,
    /// Whether to allow experimental versions (major == 0).
    pub allow_experimental: bool,
    /// Whether to allow deprecated versions (patch == 255).
    pub allow_deprecated: bool,
    /// Negotiation timeout in milliseconds.
    pub negotiation_timeout_ms: u64,
}

impl VersionNegotiationConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: max 1.0.0, min 1.0.0, no experimental, no deprecated,
    /// 5s timeout.
    ///
    /// # Errors
    /// Returns [`VersionNegotiationError::InvalidConfig`] only if
    /// validation fails.
    pub fn new() -> Result<Self, VersionNegotiationError> {
        Ok(Self {
            max_supported: 0x01000000, // 1.0.0
            min_supported: 0x01000000, // 1.0.0
            allow_experimental: false,
            allow_deprecated: false,
            negotiation_timeout_ms: 5_000,
        })
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`VersionNegotiationError::InvalidVersion`] if any field
    /// is out of range.
    pub fn validate(&self) -> Result<(), VersionNegotiationError> {
        if self.max_supported < self.min_supported {
            return Err(VersionNegotiationError::InvalidVersion(self.max_supported));
        }
        if self.negotiation_timeout_ms < MIN_TIMEOUT_MS
            || self.negotiation_timeout_ms > MAX_TIMEOUT_MS
        {
            return Err(VersionNegotiationError::InvalidVersion(
                self.negotiation_timeout_ms as u32,
            ));
        }
        Ok(())
    }
}

impl Default for VersionNegotiationConfig {
    fn default() -> Self {
        // Construct directly to avoid going through `new()` which returns Result.
        Self {
            max_supported: 0x01000000, // 1.0.0
            min_supported: 0x01000000, // 1.0.0
            allow_experimental: false,
            allow_deprecated: false,
            negotiation_timeout_ms: 5_000,
        }
    }
}

// ── Preset ───────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// use case. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Strict: only stable versions, no deprecated.
    Strict,
    /// Lenient: allow deprecated, no experimental.
    Lenient,
    /// Experimental: allow both experimental and deprecated.
    Experimental,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    ///
    /// # Errors
    /// Returns [`VersionNegotiationError::InvalidVersion`] if validation fails.
    pub fn to_config(self) -> Result<VersionNegotiationConfig, VersionNegotiationError> {
        let cfg = match self {
            Self::Strict => VersionNegotiationConfig {
                max_supported: 0x01000000, // 1.0.0
                min_supported: 0x01000000, // 1.0.0
                allow_experimental: false,
                allow_deprecated: false,
                negotiation_timeout_ms: 5_000,
            },
            Self::Lenient => VersionNegotiationConfig {
                max_supported: 0x01000000, // 1.0.0
                min_supported: 0x01000000, // 1.0.0
                allow_experimental: false,
                allow_deprecated: true,
                negotiation_timeout_ms: 10_000,
            },
            Self::Experimental => VersionNegotiationConfig {
                max_supported: 0x00010000, // 0.1.0
                min_supported: 0x00000100, // 0.0.1
                allow_experimental: true,
                allow_deprecated: true,
                negotiation_timeout_ms: 5_000,
            },
        };
        cfg.validate()?;
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = VersionNegotiationConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_supported, 0x01000000);
        assert_eq!(cfg.min_supported, 0x01000000);
    }

    #[test]
    fn new_config_succeeds() {
        let cfg = VersionNegotiationConfig::new().unwrap();
        assert!(cfg.validate().is_ok());
        assert!(!cfg.allow_experimental);
        assert!(!cfg.allow_deprecated);
    }

    #[test]
    fn validate_rejects_max_less_than_min() {
        let mut cfg = VersionNegotiationConfig::default();
        cfg.max_supported = 0x01000000; // 1.0.0
        cfg.min_supported = 0x02000000; // 2.0.0
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_timeout() {
        let mut cfg = VersionNegotiationConfig::default();
        cfg.negotiation_timeout_ms = MIN_TIMEOUT_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.negotiation_timeout_ms = MAX_TIMEOUT_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn strict_preset_rejects_experimental() {
        let cfg = Preset::Strict.to_config().unwrap();
        assert!(!cfg.allow_experimental);
        assert!(!cfg.allow_deprecated);
        assert_eq!(cfg.max_supported, 0x01000000);
    }

    #[test]
    fn lenient_preset_allows_deprecated() {
        let cfg = Preset::Lenient.to_config().unwrap();
        assert!(!cfg.allow_experimental);
        assert!(cfg.allow_deprecated);
        assert_eq!(cfg.negotiation_timeout_ms, 10_000);
    }

    #[test]
    fn experimental_preset_allows_all() {
        let cfg = Preset::Experimental.to_config().unwrap();
        assert!(cfg.allow_experimental);
        assert!(cfg.allow_deprecated);
        assert_eq!(cfg.min_supported, 0x00000100);
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::Strict, Preset::Lenient, Preset::Experimental] {
            let cfg = preset.to_config().unwrap();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }
}

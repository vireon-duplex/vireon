//! [`VersionNegotiationBuilder`] — fluent builder for
//! [`VersionNegotiationConfig`].
//!
//! See `DESIGN.md` Part 13 (configuration).

use crate::config::{Preset, VersionNegotiationConfig};
use crate::error::VersionNegotiationError;

/// Fluent builder for [`VersionNegotiationConfig`].
#[derive(Debug, Clone, Default)]
pub struct VersionNegotiationBuilder {
    cfg: VersionNegotiationConfig,
}

impl VersionNegotiationBuilder {
    /// Start a new builder with default configuration.
    pub fn new() -> Result<Self, VersionNegotiationError> {
        Ok(Self {
            cfg: VersionNegotiationConfig::new()?,
        })
    }

    /// Start from a preset.
    pub fn from_preset(preset: Preset) -> Result<Self, VersionNegotiationError> {
        Ok(Self {
            cfg: preset.to_config()?,
        })
    }

    /// Override the max supported version.
    pub fn max_supported(mut self, version: u32) -> Self {
        self.cfg.max_supported = version;
        self
    }

    /// Override the min supported version.
    pub fn min_supported(mut self, version: u32) -> Self {
        self.cfg.min_supported = version;
        self
    }

    /// Enable or disable experimental versions.
    pub fn allow_experimental(mut self, allow: bool) -> Self {
        self.cfg.allow_experimental = allow;
        self
    }

    /// Enable or disable deprecated versions.
    pub fn allow_deprecated(mut self, allow: bool) -> Self {
        self.cfg.allow_deprecated = allow;
        self
    }

    /// Override the negotiation timeout (ms).
    pub fn negotiation_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.cfg.negotiation_timeout_ms = timeout_ms;
        self
    }

    /// Validate and produce the final config.
    pub fn build(self) -> Result<VersionNegotiationConfig, VersionNegotiationError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MAX_TIMEOUT_MS, MIN_TIMEOUT_MS};

    #[test]
    fn new_builder_succeeds() {
        let builder = VersionNegotiationBuilder::new();
        assert!(builder.is_ok());
    }

    #[test]
    fn from_preset_succeeds() {
        for preset in [Preset::Strict, Preset::Lenient, Preset::Experimental] {
            let builder = VersionNegotiationBuilder::from_preset(preset);
            assert!(builder.is_ok());
        }
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = VersionNegotiationBuilder::new()
            .unwrap()
            .max_supported(0x02000000)
            .min_supported(0x01000000)
            .allow_experimental(true)
            .allow_deprecated(true)
            .negotiation_timeout_ms(10_000)
            .build()
            .unwrap();
        assert_eq!(cfg.max_supported, 0x02000000);
        assert!(cfg.allow_experimental);
        assert!(cfg.allow_deprecated);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = VersionNegotiationBuilder::new()
            .unwrap()
            .negotiation_timeout_ms(MIN_TIMEOUT_MS / 2)
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = VersionNegotiationBuilder::new()
            .unwrap()
            .negotiation_timeout_ms(MIN_TIMEOUT_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.negotiation_timeout_ms, MIN_TIMEOUT_MS);

        let cfg = VersionNegotiationBuilder::new()
            .unwrap()
            .negotiation_timeout_ms(MAX_TIMEOUT_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.negotiation_timeout_ms, MAX_TIMEOUT_MS);
    }
}

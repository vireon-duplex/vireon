//! [`ResourceQuotasBuilder`] — fluent builder for
//! [`ResourceQuotasConfig`].
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `ResourceQuotasConfig` has many fields and most callers only need to
//! override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching
//! every field.

use crate::config::{Preset, ResourceQuotasConfig};
use crate::error::QuotaError;

/// Fluent builder for [`ResourceQuotasConfig`].
#[derive(Debug, Clone)]
pub struct ResourceQuotasBuilder {
    cfg: ResourceQuotasConfig,
}

impl ResourceQuotasBuilder {
    /// Start a new builder with defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cfg: ResourceQuotasConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub const fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the max tenants.
    #[must_use]
    pub const fn max_tenants(mut self, max: u32) -> Self {
        self.cfg.max_tenants = max;
        self
    }

    /// Enable or disable enforcement.
    #[must_use]
    pub const fn enforcement(mut self, enabled: bool) -> Self {
        self.cfg.enforcement_enabled = enabled;
        self
    }

    /// Override the default max sessions.
    #[must_use]
    pub const fn default_max_sessions(mut self, max: u32) -> Self {
        self.cfg.default_max_sessions = max;
        self
    }

    /// Override the default max streams.
    #[must_use]
    pub const fn default_max_streams(mut self, max: u32) -> Self {
        self.cfg.default_max_streams = max;
        self
    }

    /// Override the default max bandwidth (bps).
    #[must_use]
    pub const fn default_max_bandwidth_bps(mut self, max_bps: u64) -> Self {
        self.cfg.default_max_bandwidth_bps = max_bps;
        self
    }

    /// Override the check interval (ms).
    #[must_use]
    pub const fn check_interval_ms(mut self, interval_ms: u64) -> Self {
        self.cfg.check_interval_ms = interval_ms;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`QuotaError::InvalidQuota`] if any field is out
    /// of range.
    pub fn build(self) -> Result<ResourceQuotasConfig, QuotaError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for ResourceQuotasBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        MAX_CHECK_INTERVAL_MS, MAX_DEFAULT_BANDWIDTH_BPS, MAX_DEFAULT_SESSIONS,
        MAX_DEFAULT_STREAMS, MAX_TENANTS, MIN_CHECK_INTERVAL_MS, MIN_DEFAULT_BANDWIDTH_BPS,
        MIN_DEFAULT_SESSIONS, MIN_DEFAULT_STREAMS, MIN_TENANTS,
    };

    #[test]
    fn new_builder_has_defaults() {
        let cfg = ResourceQuotasBuilder::new().build().unwrap();
        assert_eq!(cfg.max_tenants, 10_000);
        assert_eq!(cfg.default_max_sessions, 10_000);
        assert_eq!(cfg.default_max_streams, 100_000);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = ResourceQuotasBuilder::new()
            .max_tenants(50_000)
            .enforcement(false)
            .default_max_sessions(20_000)
            .default_max_streams(200_000)
            .default_max_bandwidth_bps(2_000_000_000)
            .check_interval_ms(10_000)
            .build()
            .unwrap();
        assert_eq!(cfg.max_tenants, 50_000);
        assert!(!cfg.enforcement_enabled);
        assert_eq!(cfg.default_max_sessions, 20_000);
        assert_eq!(cfg.default_max_streams, 200_000);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = ResourceQuotasBuilder::new()
            .max_tenants(0) // too small
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = ResourceQuotasBuilder::new()
            .max_tenants(MIN_TENANTS)
            .default_max_sessions(MIN_DEFAULT_SESSIONS)
            .default_max_streams(MIN_DEFAULT_STREAMS)
            .default_max_bandwidth_bps(MIN_DEFAULT_BANDWIDTH_BPS)
            .check_interval_ms(MIN_CHECK_INTERVAL_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_tenants, MIN_TENANTS);

        let cfg = ResourceQuotasBuilder::new()
            .max_tenants(MAX_TENANTS)
            .default_max_sessions(MAX_DEFAULT_SESSIONS)
            .default_max_streams(MAX_DEFAULT_STREAMS)
            .default_max_bandwidth_bps(MAX_DEFAULT_BANDWIDTH_BPS)
            .check_interval_ms(MAX_CHECK_INTERVAL_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_tenants, MAX_TENANTS);
    }

    #[test]
    fn builder_from_preset() {
        let cfg = ResourceQuotasBuilder::from_preset(Preset::Standard)
            .default_max_sessions(20_000) // override
            .build()
            .unwrap();
        assert_eq!(cfg.default_max_sessions, 20_000);
        assert!(cfg.enforcement_enabled); // from preset
    }

    #[test]
    fn builder_disables_enforcement() {
        let cfg = ResourceQuotasBuilder::new()
            .enforcement(false)
            .build()
            .unwrap();
        assert!(!cfg.enforcement_enabled);
    }

    #[test]
    fn builder_default_is_same_as_new() {
        let a = ResourceQuotasBuilder::new().build().unwrap();
        let b = ResourceQuotasBuilder::default().build().unwrap();
        assert_eq!(a, b);
    }
}

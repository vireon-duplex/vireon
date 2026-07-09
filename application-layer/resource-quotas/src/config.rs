//! [`ResourceQuotasConfig`], [`Preset`] — declarative configuration for
//! the resource quotas system.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The three presets encode deployment archetypes for quota enforcement:
//!
//! | Preset | Session Limit | Stream Limit | Bandwidth | Use case |
//! |--------|---------------|---------------|-----------|----------|
//! | `Permissive` | 100K | 1M | 10 Gbps | Development, testing |
//! | `Standard` | 10K | 100K | 1 Gbps | Production, balanced |
//! | `Strict` | 1K | 10K | 100 Mbps | Production, conservative |

use crate::error::QuotaError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of tenants the quota manager will accept.
pub const MIN_TENANTS: u32 = 1;
/// Maximum number of tenants the quota manager will accept (hard cap).
pub const MAX_TENANTS: u32 = 1_000_000;

/// Minimum default max sessions per tenant.
pub const MIN_DEFAULT_SESSIONS: u32 = 1;
/// Maximum default max sessions per tenant (hard cap).
pub const MAX_DEFAULT_SESSIONS: u32 = 1_000_000;

/// Minimum default max streams per tenant.
pub const MIN_DEFAULT_STREAMS: u32 = 1;
/// Maximum default max streams per tenant (hard cap).
pub const MAX_DEFAULT_STREAMS: u32 = 10_000_000;

/// Minimum default max bandwidth per tenant (bps).
pub const MIN_DEFAULT_BANDWIDTH_BPS: u64 = 1_000; // 1 Kbps
/// Maximum default max bandwidth per tenant (bps).
pub const MAX_DEFAULT_BANDWIDTH_BPS: u64 = 100_000_000_000; // 100 Gbps

/// Minimum check interval in milliseconds (100ms).
pub const MIN_CHECK_INTERVAL_MS: u64 = 100;
/// Maximum check interval in milliseconds (60s).
pub const MAX_CHECK_INTERVAL_MS: u64 = 60_000;

// ── ResourceQuotasConfig ──────────────────────────────────────────────

/// Declarative configuration for the resource quotas system.
///
/// Constructed via [`ResourceQuotasConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceQuotasConfig {
    /// Maximum tenants the quota manager may track.
    pub max_tenants: u32,
    /// Whether quota enforcement is enabled.
    pub enforcement_enabled: bool,
    /// Default max sessions per tenant.
    pub default_max_sessions: u32,
    /// Default max streams per tenant.
    pub default_max_streams: u32,
    /// Default max bandwidth per tenant (bps).
    pub default_max_bandwidth_bps: u64,
    /// Check interval for quota enforcement (ms).
    pub check_interval_ms: u64,
}

impl ResourceQuotasConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: 10K tenants, enforcement enabled, 10K sessions,
    /// 100K streams, 1 Gbps bandwidth, 5s check interval.
    pub const fn new() -> Self {
        Self {
            max_tenants: 10_000,
            enforcement_enabled: true,
            default_max_sessions: 10_000,
            default_max_streams: 100_000,
            default_max_bandwidth_bps: 1_000_000_000, // 1 Gbps
            check_interval_ms: 5_000,
        }
    }

    /// Builder-style: set the max tenants.
    #[must_use]
    pub const fn with_max_tenants(mut self, max: u32) -> Self {
        self.max_tenants = max;
        self
    }

    /// Builder-style: enable or disable enforcement.
    #[must_use]
    pub const fn with_enforcement(mut self, enabled: bool) -> Self {
        self.enforcement_enabled = enabled;
        self
    }

    /// Builder-style: set the default max sessions.
    #[must_use]
    pub const fn with_default_max_sessions(mut self, max: u32) -> Self {
        self.default_max_sessions = max;
        self
    }

    /// Builder-style: set the default max streams.
    #[must_use]
    pub const fn with_default_max_streams(mut self, max: u32) -> Self {
        self.default_max_streams = max;
        self
    }

    /// Builder-style: set the default max bandwidth (bps).
    #[must_use]
    pub const fn with_default_max_bandwidth_bps(mut self, max_bps: u64) -> Self {
        self.default_max_bandwidth_bps = max_bps;
        self
    }

    /// Builder-style: set the check interval (ms).
    #[must_use]
    pub const fn with_check_interval_ms(mut self, interval_ms: u64) -> Self {
        self.check_interval_ms = interval_ms;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`QuotaError::InvalidQuota`] if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), QuotaError> {
        if self.max_tenants < MIN_TENANTS || self.max_tenants > MAX_TENANTS {
            return Err(QuotaError::InvalidQuota("max_tenants out of range"));
        }
        if self.default_max_sessions < MIN_DEFAULT_SESSIONS
            || self.default_max_sessions > MAX_DEFAULT_SESSIONS
        {
            return Err(QuotaError::InvalidQuota(
                "default_max_sessions out of range",
            ));
        }
        if self.default_max_streams < MIN_DEFAULT_STREAMS
            || self.default_max_streams > MAX_DEFAULT_STREAMS
        {
            return Err(QuotaError::InvalidQuota("default_max_streams out of range"));
        }
        if self.default_max_bandwidth_bps < MIN_DEFAULT_BANDWIDTH_BPS
            || self.default_max_bandwidth_bps > MAX_DEFAULT_BANDWIDTH_BPS
        {
            return Err(QuotaError::InvalidQuota(
                "default_max_bandwidth_bps out of range",
            ));
        }
        if self.check_interval_ms < MIN_CHECK_INTERVAL_MS
            || self.check_interval_ms > MAX_CHECK_INTERVAL_MS
        {
            return Err(QuotaError::InvalidQuota("check_interval_ms out of range"));
        }
        Ok(())
    }
}

impl Default for ResourceQuotasConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// quota enforcement profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Permissive: high limits for development/testing.
    Permissive,
    /// Standard: moderate limits for production.
    Standard,
    /// Strict: low limits for conservative production.
    Strict,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub const fn to_config(self) -> ResourceQuotasConfig {
        match self {
            Self::Permissive => ResourceQuotasConfig {
                max_tenants: 1_000,
                enforcement_enabled: true,
                default_max_sessions: 100_000,
                default_max_streams: 1_000_000,
                default_max_bandwidth_bps: 10_000_000_000, // 10 Gbps
                check_interval_ms: 5_000,
            },
            Self::Standard => ResourceQuotasConfig {
                max_tenants: 10_000,
                enforcement_enabled: true,
                default_max_sessions: 10_000,
                default_max_streams: 100_000,
                default_max_bandwidth_bps: 1_000_000_000, // 1 Gbps
                check_interval_ms: 5_000,
            },
            Self::Strict => ResourceQuotasConfig {
                max_tenants: 100_000,
                enforcement_enabled: true,
                default_max_sessions: 1_000,
                default_max_streams: 10_000,
                default_max_bandwidth_bps: 100_000_000, // 100 Mbps
                check_interval_ms: 5_000,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = ResourceQuotasConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_tenants, 10_000);
        assert_eq!(cfg.default_max_sessions, 10_000);
        assert_eq!(cfg.default_max_streams, 100_000);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = ResourceQuotasConfig::new()
            .with_max_tenants(50_000)
            .with_enforcement(false)
            .with_default_max_sessions(20_000)
            .with_default_max_streams(200_000)
            .with_default_max_bandwidth_bps(2_000_000_000)
            .with_check_interval_ms(10_000);
        assert_eq!(cfg.max_tenants, 50_000);
        assert!(!cfg.enforcement_enabled);
        assert_eq!(cfg.default_max_sessions, 20_000);
        assert_eq!(cfg.default_max_streams, 200_000);
    }

    #[test]
    fn validate_rejects_bad_max_tenants() {
        let mut cfg = ResourceQuotasConfig::default();
        cfg.max_tenants = 0;
        assert!(cfg.validate().is_err());
        cfg.max_tenants = MAX_TENANTS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_default_sessions() {
        let mut cfg = ResourceQuotasConfig::default();
        cfg.default_max_sessions = 0;
        assert!(cfg.validate().is_err());
        cfg.default_max_sessions = MAX_DEFAULT_SESSIONS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_default_streams() {
        let mut cfg = ResourceQuotasConfig::default();
        cfg.default_max_streams = 0;
        assert!(cfg.validate().is_err());
        cfg.default_max_streams = MAX_DEFAULT_STREAMS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_bandwidth() {
        let mut cfg = ResourceQuotasConfig::default();
        cfg.default_max_bandwidth_bps = MIN_DEFAULT_BANDWIDTH_BPS / 2;
        assert!(cfg.validate().is_err());
        cfg.default_max_bandwidth_bps = MAX_DEFAULT_BANDWIDTH_BPS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn permissive_preset_has_high_limits() {
        let cfg = Preset::Permissive.to_config();
        assert_eq!(cfg.default_max_sessions, 100_000);
        assert_eq!(cfg.default_max_streams, 1_000_000);
        assert_eq!(cfg.default_max_bandwidth_bps, 10_000_000_000);
    }

    #[test]
    fn standard_preset_has_moderate_limits() {
        let cfg = Preset::Standard.to_config();
        assert_eq!(cfg.default_max_sessions, 10_000);
        assert_eq!(cfg.default_max_streams, 100_000);
        assert_eq!(cfg.default_max_bandwidth_bps, 1_000_000_000);
    }

    #[test]
    fn strict_preset_has_low_limits() {
        let cfg = Preset::Strict.to_config();
        assert_eq!(cfg.default_max_sessions, 1_000);
        assert_eq!(cfg.default_max_streams, 10_000);
        assert_eq!(cfg.default_max_bandwidth_bps, 100_000_000);
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::Permissive, Preset::Standard, Preset::Strict] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn enforcement_can_be_disabled() {
        let cfg = ResourceQuotasConfig::new().with_enforcement(false);
        assert!(!cfg.enforcement_enabled);
        assert!(cfg.validate().is_ok());
    }
}

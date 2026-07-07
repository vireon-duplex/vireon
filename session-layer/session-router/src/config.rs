//! [`RouterConfig`], [`Preset`] — declarative configuration for
//! the session router.
//!
//! See `DESIGN.md` Part 12 (configuration & presets).
//!
//! # Presets
//!
//! The four presets encode deployment archetypes:
//!
//! | Preset       | Cache TTL | Max Entries | RPC Timeout | Retries |
//! |--------------|-----------|-------------|-------------|---------|
//! | `LowLatency` | 5 s       | 10 K        | 2 ms        | 0       |
//! | `Balanced`   | 15 s      | 50 K        | 10 ms       | 2       |
//! | `CacheHeavy` | 60 s      | 500 K       | 20 ms       | 3       |
//! | `Cacheless`  | 0 (off)   | 0           | 50 ms       | 1       |

use crate::decision::RoutingMode;
use crate::error::RouterError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum cache TTL (ms). 0 means cache disabled.
pub const MIN_CACHE_TTL_MS: u64 = 0;

/// Maximum cache TTL (ms). 5 minutes.
pub const MAX_CACHE_TTL_MS: u64 = 300_000;

/// Minimum cache max entries.
pub const MIN_CACHE_MAX_ENTRIES: u32 = 0;

/// Maximum cache max entries. 10 M entries.
pub const MAX_CACHE_MAX_ENTRIES: u32 = 10_000_000;

/// Minimum RPC timeout (ms).
pub const MIN_RPC_TIMEOUT_MS: u64 = 1;

/// Maximum RPC timeout (ms). 10 seconds.
pub const MAX_RPC_TIMEOUT_MS: u64 = 10_000;

/// Minimum health check interval (ms).
pub const MIN_HEALTH_CHECK_INTERVAL_MS: u64 = 100;

/// Maximum health check interval (ms). 5 minutes.
pub const MAX_HEALTH_CHECK_INTERVAL_MS: u64 = 300_000;

// ── RouterConfig ────────────────────────────────────────────────

/// Declarative configuration for the session router.
///
/// Constructed via [`RouterConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RouterConfig {
    /// Cache entry TTL in milliseconds. 0 disables the cache.
    pub cache_ttl_ms: u64,
    /// Maximum number of entries the cache can hold.
    pub cache_max_entries: u32,
    /// The default routing mode for incoming requests.
    pub default_mode: RoutingMode,
    /// RPC timeout for inter-node route lookups (ms).
    pub rpc_timeout_ms: u64,
    /// Maximum retry attempts on transient failures.
    pub max_retries: u8,
    /// Whether anycast routing is enabled.
    pub enable_anycast: bool,
    /// Whether broadcast routing is enabled.
    pub enable_broadcast: bool,
    /// Health check probe interval (ms).
    pub health_check_interval_ms: u64,
}

impl RouterConfig {
    /// Construct a default config with the `Balanced` preset.
    ///
    /// # Errors
    /// Never errors in Phase 0; returns `Ok` always.
    pub fn new() -> Result<Self, RouterError> {
        Ok(Self::default())
    }

    /// Builder-style: set the cache TTL (ms).
    #[must_use]
    pub const fn with_cache_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.cache_ttl_ms = ttl_ms;
        self
    }

    /// Builder-style: set the cache max entries.
    #[must_use]
    pub const fn with_cache_max_entries(mut self, max: u32) -> Self {
        self.cache_max_entries = max;
        self
    }

    /// Builder-style: set the default routing mode.
    #[must_use]
    pub const fn with_default_mode(mut self, mode: RoutingMode) -> Self {
        self.default_mode = mode;
        self
    }

    /// Builder-style: set the RPC timeout (ms).
    #[must_use]
    pub const fn with_rpc_timeout_ms(mut self, ms: u64) -> Self {
        self.rpc_timeout_ms = ms;
        self
    }

    /// Builder-style: set the max retries.
    #[must_use]
    pub const fn with_max_retries(mut self, retries: u8) -> Self {
        self.max_retries = retries;
        self
    }

    /// Builder-style: enable/disable anycast.
    #[must_use]
    pub const fn with_anycast(mut self, on: bool) -> Self {
        self.enable_anycast = on;
        self
    }

    /// Builder-style: enable/disable broadcast.
    #[must_use]
    pub const fn with_broadcast(mut self, on: bool) -> Self {
        self.enable_broadcast = on;
        self
    }

    /// Builder-style: set the health check interval (ms).
    #[must_use]
    pub const fn with_health_check_interval_ms(mut self, ms: u64) -> Self {
        self.health_check_interval_ms = ms;
        self
    }

    /// Validate the config.
    ///
    /// # Errors
    /// Returns [`RouterError::InvalidConfig`] if any field is out of
    /// range or contradictory.
    pub fn validate(&self) -> Result<(), RouterError> {
        if self.cache_ttl_ms > MAX_CACHE_TTL_MS {
            return Err(RouterError::InvalidConfig(format!(
                "cache_ttl_ms {ttl} exceeds maximum {MAX_CACHE_TTL_MS}",
                ttl = self.cache_ttl_ms
            )));
        }
        if self.cache_max_entries > MAX_CACHE_MAX_ENTRIES {
            return Err(RouterError::InvalidConfig(format!(
                "cache_max_entries {n} exceeds maximum {MAX_CACHE_MAX_ENTRIES}",
                n = self.cache_max_entries
            )));
        }
        if self.rpc_timeout_ms < MIN_RPC_TIMEOUT_MS || self.rpc_timeout_ms > MAX_RPC_TIMEOUT_MS {
            return Err(RouterError::InvalidConfig(format!(
                "rpc_timeout_ms {ms} out of range [{MIN_RPC_TIMEOUT_MS}, {MAX_RPC_TIMEOUT_MS}]",
                ms = self.rpc_timeout_ms
            )));
        }
        if self.health_check_interval_ms < MIN_HEALTH_CHECK_INTERVAL_MS
            || self.health_check_interval_ms > MAX_HEALTH_CHECK_INTERVAL_MS
        {
            return Err(RouterError::InvalidConfig(format!(
                "health_check_interval_ms {ms} out of range [{MIN_HEALTH_CHECK_INTERVAL_MS}, {MAX_HEALTH_CHECK_INTERVAL_MS}]",
                ms = self.health_check_interval_ms
            )));
        }
        if self.default_mode.is_anycast() && !self.enable_anycast {
            return Err(RouterError::InvalidConfig(
                "default_mode is Anycast but anycast is disabled".to_string(),
            ));
        }
        if self.default_mode.is_broadcast() && !self.enable_broadcast {
            return Err(RouterError::InvalidConfig(
                "default_mode is Broadcast but broadcast is disabled".to_string(),
            ));
        }
        Ok(())
    }

    /// Whether the cache is enabled (TTL > 0 and max_entries > 0).
    #[must_use]
    pub const fn cache_enabled(&self) -> bool {
        self.cache_ttl_ms > 0 && self.cache_max_entries > 0
    }
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            cache_ttl_ms: 15_000,
            cache_max_entries: 50_000,
            default_mode: RoutingMode::Unicast,
            rpc_timeout_ms: 10,
            max_retries: 2,
            enable_anycast: true,
            enable_broadcast: false,
            health_check_interval_ms: 5_000,
        }
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// routing workload. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Ultra-low-latency routing with minimal caching.
    LowLatency,
    /// Balanced caching and latency for general-purpose use.
    Balanced,
    /// Aggressive caching for read-heavy workloads.
    CacheHeavy,
    /// No caching; every lookup hits the registry.
    Cacheless,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub const fn to_config(self) -> RouterConfig {
        match self {
            Self::LowLatency => RouterConfig {
                cache_ttl_ms: 5_000,
                cache_max_entries: 10_000,
                default_mode: RoutingMode::Unicast,
                rpc_timeout_ms: 2,
                max_retries: 0,
                enable_anycast: true,
                enable_broadcast: false,
                health_check_interval_ms: 1_000,
            },
            Self::Balanced => RouterConfig {
                cache_ttl_ms: 15_000,
                cache_max_entries: 50_000,
                default_mode: RoutingMode::Unicast,
                rpc_timeout_ms: 10,
                max_retries: 2,
                enable_anycast: true,
                enable_broadcast: false,
                health_check_interval_ms: 5_000,
            },
            Self::CacheHeavy => RouterConfig {
                cache_ttl_ms: 60_000,
                cache_max_entries: 500_000,
                default_mode: RoutingMode::Unicast,
                rpc_timeout_ms: 20,
                max_retries: 3,
                enable_anycast: true,
                enable_broadcast: true,
                health_check_interval_ms: 10_000,
            },
            Self::Cacheless => RouterConfig {
                cache_ttl_ms: 0,
                cache_max_entries: 0,
                default_mode: RoutingMode::Unicast,
                rpc_timeout_ms: 50,
                max_retries: 1,
                enable_anycast: true,
                enable_broadcast: false,
                health_check_interval_ms: 2_000,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = RouterConfig::default();
        assert!(cfg.validate().is_ok());
        assert!(cfg.cache_enabled());
    }

    #[test]
    fn new_returns_balanced_defaults() {
        let cfg = RouterConfig::new().unwrap();
        let balanced = Preset::Balanced.to_config();
        assert_eq!(cfg, balanced);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = RouterConfig::default()
            .with_cache_ttl_ms(30_000)
            .with_cache_max_entries(100_000)
            .with_rpc_timeout_ms(5)
            .with_max_retries(5);
        assert_eq!(cfg.cache_ttl_ms, 30_000);
        assert_eq!(cfg.cache_max_entries, 100_000);
        assert_eq!(cfg.rpc_timeout_ms, 5);
        assert_eq!(cfg.max_retries, 5);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_excessive_ttl() {
        let cfg = RouterConfig::default().with_cache_ttl_ms(MAX_CACHE_TTL_MS + 1);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_rpc_timeout() {
        let cfg = RouterConfig::default().with_rpc_timeout_ms(0);
        assert!(cfg.validate().is_err());
        let cfg = RouterConfig::default().with_rpc_timeout_ms(MAX_RPC_TIMEOUT_MS + 1);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_health_check_interval() {
        let cfg = RouterConfig::default().with_health_check_interval_ms(50);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_anycast_mode_with_anycast_disabled() {
        let cfg = RouterConfig::default()
            .with_default_mode(RoutingMode::Anycast)
            .with_anycast(false);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn cache_disabled_when_ttl_zero() {
        let cfg = RouterConfig::default().with_cache_ttl_ms(0);
        assert!(!cfg.cache_enabled());
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [
            Preset::LowLatency,
            Preset::Balanced,
            Preset::CacheHeavy,
            Preset::Cacheless,
        ] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn cacheless_preset_has_no_cache() {
        let cfg = Preset::Cacheless.to_config();
        assert!(!cfg.cache_enabled());
        assert_eq!(cfg.cache_ttl_ms, 0);
        assert_eq!(cfg.cache_max_entries, 0);
    }

    #[test]
    fn low_latency_preset_has_short_ttl() {
        let cfg = Preset::LowLatency.to_config();
        assert_eq!(cfg.cache_ttl_ms, 5_000);
        assert_eq!(cfg.rpc_timeout_ms, 2);
        assert_eq!(cfg.max_retries, 0);
    }
}

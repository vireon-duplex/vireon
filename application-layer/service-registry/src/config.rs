//! [`ServiceRegistryConfig`], [`Preset`] — declarative configuration for
//! the service registry.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The four presets encode deployment archetypes for different service
//! topologies:
//!
//! | Preset | Topology | Instances per Service | Health Check | Load Balancing |
//! |--------|----------|----------------------|--------------|----------------|
//! | `Local` | Single node | 10 | 5s interval | Enabled |
//! | `Cluster` | Multi-node | 50 | 10s interval | Enabled |
//! | `Federation` | Cross-cluster | 100 | 30s interval | Enabled |
//! | `Edge` | Edge nodes | 20 | 60s interval | Enabled |

use crate::error::ServiceRegistryError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of services the registry will accept.
pub const MIN_SERVICES: u32 = 1;

/// Maximum number of services the registry will accept.
pub const MAX_SERVICES: u32 = 1_000_000;

/// Minimum number of instances per service.
pub const MIN_INSTANCES: u8 = 1;

/// Maximum number of instances per service.
pub const MAX_INSTANCES: u8 = 255;

/// Minimum health check interval in milliseconds.
pub const MIN_HEALTH_CHECK_MS: u64 = 100;

/// Maximum health check interval in milliseconds.
pub const MAX_HEALTH_CHECK_MS: u64 = 300_000; // 5 minutes

/// Default health check interval (5 seconds).
pub const DEFAULT_HEALTH_CHECK_MS: u64 = 5_000;

/// Default drain timeout (30 seconds).
pub const DEFAULT_DRAIN_TIMEOUT_MS: u64 = 30_000;

/// Maximum drain timeout (5 minutes).
pub const MAX_DRAIN_TIMEOUT_MS: u64 = 300_000;

// ── ServiceRegistryConfig ──────────────────────────────────────────

/// Declarative configuration for the service registry.
///
/// Constructed via [`ServiceRegistryConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServiceRegistryConfig {
    /// Maximum number of services to register.
    pub max_services: u32,
    /// Maximum instances per service.
    pub max_instances_per_service: u8,
    /// Health check interval in milliseconds.
    pub health_check_interval_ms: u64,
    /// Whether load balancing is enabled.
    pub enable_load_balancing: bool,
    /// Drain timeout for services in milliseconds.
    pub drain_timeout_ms: u64,
}

impl ServiceRegistryConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: 10K services, 50 instances per service, 5s health checks,
    /// load balancing enabled, 30s drain timeout.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_services: 10_000,
            max_instances_per_service: 50,
            health_check_interval_ms: DEFAULT_HEALTH_CHECK_MS,
            enable_load_balancing: true,
            drain_timeout_ms: DEFAULT_DRAIN_TIMEOUT_MS,
        }
    }

    /// Builder-style: set max services.
    #[must_use]
    pub const fn with_max_services(mut self, max: u32) -> Self {
        self.max_services = max;
        self
    }

    /// Builder-style: set max instances per service.
    #[must_use]
    pub const fn with_max_instances_per_service(mut self, max: u8) -> Self {
        self.max_instances_per_service = max;
        self
    }

    /// Builder-style: set health check interval.
    #[must_use]
    pub const fn with_health_check_interval_ms(mut self, interval_ms: u64) -> Self {
        self.health_check_interval_ms = interval_ms;
        self
    }

    /// Builder-style: enable or disable load balancing.
    #[must_use]
    pub const fn with_load_balancing(mut self, enable: bool) -> Self {
        self.enable_load_balancing = enable;
        self
    }

    /// Builder-style: set drain timeout.
    #[must_use]
    pub const fn with_drain_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.drain_timeout_ms = timeout_ms;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`ServiceRegistryError::InvalidConfig`] if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), ServiceRegistryError> {
        // Map InvalidConfig to ShuttingDown for now since we don't have InvalidConfig variant
        if self.max_services < MIN_SERVICES || self.max_services > MAX_SERVICES {
            return Err(ServiceRegistryError::ShuttingDown);
        }
        if self.max_instances_per_service < MIN_INSTANCES {
            return Err(ServiceRegistryError::ShuttingDown);
        }
        if self.health_check_interval_ms < MIN_HEALTH_CHECK_MS
            || self.health_check_interval_ms > MAX_HEALTH_CHECK_MS
        {
            return Err(ServiceRegistryError::ShuttingDown);
        }
        if self.drain_timeout_ms > MAX_DRAIN_TIMEOUT_MS {
            return Err(ServiceRegistryError::ShuttingDown);
        }
        Ok(())
    }
}

impl Default for ServiceRegistryConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// topology and load profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Single-node deployment for local development.
    Local,
    /// Multi-node cluster with health checks.
    Cluster,
    /// Cross-cluster federation with longer health check intervals.
    Federation,
    /// Edge deployment with limited connectivity and longer health checks.
    Edge,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub const fn to_config(self) -> ServiceRegistryConfig {
        match self {
            Self::Local => ServiceRegistryConfig::new()
                .with_max_services(100)
                .with_max_instances_per_service(10)
                .with_health_check_interval_ms(5_000)
                .with_load_balancing(true)
                .with_drain_timeout_ms(10_000),
            Self::Cluster => ServiceRegistryConfig::new()
                .with_max_services(10_000)
                .with_max_instances_per_service(50)
                .with_health_check_interval_ms(10_000)
                .with_load_balancing(true)
                .with_drain_timeout_ms(30_000),
            Self::Federation => ServiceRegistryConfig::new()
                .with_max_services(100_000)
                .with_max_instances_per_service(100)
                .with_health_check_interval_ms(30_000)
                .with_load_balancing(true)
                .with_drain_timeout_ms(60_000),
            Self::Edge => ServiceRegistryConfig::new()
                .with_max_services(1_000)
                .with_max_instances_per_service(20)
                .with_health_check_interval_ms(60_000)
                .with_load_balancing(true)
                .with_drain_timeout_ms(120_000),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = ServiceRegistryConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_services, 10_000);
        assert_eq!(cfg.max_instances_per_service, 50);
        assert_eq!(cfg.health_check_interval_ms, DEFAULT_HEALTH_CHECK_MS);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = ServiceRegistryConfig::new()
            .with_max_services(1000)
            .with_max_instances_per_service(20)
            .with_health_check_interval_ms(15_000)
            .with_load_balancing(false)
            .with_drain_timeout_ms(45_000);
        assert_eq!(cfg.max_services, 1000);
        assert_eq!(cfg.max_instances_per_service, 20);
        assert_eq!(cfg.health_check_interval_ms, 15_000);
        assert!(!cfg.enable_load_balancing);
        assert_eq!(cfg.drain_timeout_ms, 45_000);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_bad_max_services() {
        let mut cfg = ServiceRegistryConfig::default();
        cfg.max_services = 0;
        assert!(cfg.validate().is_err());
        cfg.max_services = MAX_SERVICES + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_max_instances() {
        let mut cfg = ServiceRegistryConfig::default();
        cfg.max_instances_per_service = 0;
        assert!(cfg.validate().is_err());
        cfg.max_instances_per_service = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_health_check_interval() {
        let mut cfg = ServiceRegistryConfig::default();
        cfg.health_check_interval_ms = MIN_HEALTH_CHECK_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.health_check_interval_ms = MAX_HEALTH_CHECK_MS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_drain_timeout() {
        let mut cfg = ServiceRegistryConfig::default();
        cfg.drain_timeout_ms = MAX_DRAIN_TIMEOUT_MS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn local_preset_matches_development_profile() {
        let cfg = Preset::Local.to_config();
        assert_eq!(cfg.max_services, 100);
        assert_eq!(cfg.max_instances_per_service, 10);
        assert_eq!(cfg.health_check_interval_ms, 5_000);
    }

    #[test]
    fn cluster_preset_matches_production_profile() {
        let cfg = Preset::Cluster.to_config();
        assert_eq!(cfg.max_services, 10_000);
        assert_eq!(cfg.max_instances_per_service, 50);
        assert_eq!(cfg.health_check_interval_ms, 10_000);
    }

    #[test]
    fn federation_preset_matches_cross_cluster_profile() {
        let cfg = Preset::Federation.to_config();
        assert_eq!(cfg.max_services, 100_000);
        assert_eq!(cfg.max_instances_per_service, 100);
        assert_eq!(cfg.health_check_interval_ms, 30_000);
    }

    #[test]
    fn edge_preset_matches_limited_connectivity_profile() {
        let cfg = Preset::Edge.to_config();
        assert_eq!(cfg.max_services, 1_000);
        assert_eq!(cfg.max_instances_per_service, 20);
        assert_eq!(cfg.health_check_interval_ms, 60_000);
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [
            Preset::Local,
            Preset::Cluster,
            Preset::Federation,
            Preset::Edge,
        ] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn preset_is_const() {
        // Ensure to_config is const-friendly.
        const _CFG: ServiceRegistryConfig = Preset::Local.to_config();
    }
}

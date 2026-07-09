//! [`LoadBalancer`] — load balancer for service instances.
//!
//! See `DESIGN.md` Part 8 (load balancing).
//!
//! # Role
//!
//! `LoadBalancer` provides multiple strategies for selecting instances:
//!
//! - Round-robin (fair distribution)
//! - Least-loaded (routing to instance with fewest active requests)
//! - Weighted (based on endpoint weights)
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a
//! stub error so that callers can code against the API contract
//! today. Phase 1 fills in the real logic.

use crate::config::ServiceRegistryConfig;
use crate::endpoint::ServiceEndpoint;
use crate::error::ServiceRegistryError;
use crate::health::HealthReport;
use crate::instance::ServiceInstance;
use crate::stats::ServiceRegistryStats;

/// Load balancer for service instance selection.
///
/// Supports multiple strategies: round-robin, least-loaded, weighted.
/// Phase 0 ships a typed skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct LoadBalancer {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: ServiceRegistryConfig,
    /// Atomic observability counters.
    stats: ServiceRegistryStats,
}

impl LoadBalancer {
    /// Construct a load balancer from validated configuration.
    #[must_use]
    pub const fn new(cfg: ServiceRegistryConfig) -> Self {
        Self {
            cfg,
            stats: ServiceRegistryStats::new(),
        }
    }

    /// Select the best instance from a list.
    ///
    /// # Errors
    /// Returns [`ServiceRegistryError::NoHealthyInstance`] if no healthy instances.
    pub fn select(
        &self,
        instances: &[ServiceInstance],
    ) -> Result<ServiceInstance, ServiceRegistryError> {
        // TODO(phase-1): implement selection logic + update stats.
        if instances.is_empty() {
            return Err(ServiceRegistryError::NoHealthyInstance(
                crate::service::ServiceId::nil(),
            ));
        }
        Err(ServiceRegistryError::ShuttingDown)
    }

    /// Round-robin selection from endpoints.
    ///
    /// # Errors
    /// Returns [`ServiceRegistryError::NoHealthyInstance`] if no healthy endpoints.
    pub fn round_robin(
        &self,
        endpoints: &[ServiceEndpoint],
    ) -> Result<ServiceEndpoint, ServiceRegistryError> {
        // TODO(phase-1): implement round-robin + update stats.
        if endpoints.is_empty() {
            return Err(ServiceRegistryError::NoHealthyInstance(
                crate::service::ServiceId::nil(),
            ));
        }
        Err(ServiceRegistryError::ShuttingDown)
    }

    /// Least-loaded selection from instances.
    ///
    /// # Errors
    /// Returns [`ServiceRegistryError::NoHealthyInstance`] if no healthy instances.
    pub fn least_loaded(
        &self,
        instances: &[ServiceInstance],
    ) -> Result<ServiceInstance, ServiceRegistryError> {
        // TODO(phase-1): find instance with lowest load factor + update stats.
        if instances.is_empty() {
            return Err(ServiceRegistryError::NoHealthyInstance(
                crate::service::ServiceId::nil(),
            ));
        }
        Err(ServiceRegistryError::ShuttingDown)
    }

    /// Snapshot the load balancer's health.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    /// Reference to the underlying stats (for testing / metrics).
    #[must_use]
    pub fn stats(&self) -> &ServiceRegistryStats {
        &self.stats
    }

    /// Get the configuration.
    #[must_use]
    pub const fn config(&self) -> &ServiceRegistryConfig {
        &self.cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;
    use session_registry::node::NodeId;

    fn make_load_balancer() -> LoadBalancer {
        let cfg = Preset::Local.to_config();
        LoadBalancer::new(cfg)
    }

    #[test]
    fn load_balancer_constructs() {
        let lb = make_load_balancer();
        let h = lb.health();
        assert!(h.is_healthy());
    }

    #[test]
    fn select_empty_instances_returns_error() {
        let lb = make_load_balancer();
        let instances: &[ServiceInstance] = &[];
        let res = lb.select(instances);
        assert!(matches!(
            res,
            Err(ServiceRegistryError::NoHealthyInstance(_))
        ));
    }

    #[test]
    fn round_robin_empty_endpoints_returns_error() {
        let lb = make_load_balancer();
        let endpoints: &[ServiceEndpoint] = &[];
        let res = lb.round_robin(endpoints);
        assert!(matches!(
            res,
            Err(ServiceRegistryError::NoHealthyInstance(_))
        ));
    }

    #[test]
    fn least_loaded_empty_instances_returns_error() {
        let lb = make_load_balancer();
        let instances: &[ServiceInstance] = &[];
        let res = lb.least_loaded(instances);
        assert!(matches!(
            res,
            Err(ServiceRegistryError::NoHealthyInstance(_))
        ));
    }

    #[test]
    fn select_phase0_stub() {
        let lb = make_load_balancer();
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let instance = ServiceInstance::new(endpoint);
        let instances = &[instance];
        assert!(lb.select(instances).is_err());
    }

    #[test]
    fn config_accessor() {
        let lb = make_load_balancer();
        assert_eq!(lb.config().max_services, 100);
    }

    #[test]
    fn stats_accessor() {
        let lb = make_load_balancer();
        assert_eq!(lb.stats().snapshot().load_balance_decisions, 0);
    }

    #[test]
    fn load_balancer_new_works() {
        let cfg = Preset::Local.to_config();
        let lb = LoadBalancer::new(cfg);
        assert_eq!(lb.stats().snapshot().load_balance_decisions, 0);
    }
}

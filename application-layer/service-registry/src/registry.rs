//! [`ServiceRegistry`] — the main registry facade.
//!
//! See `DESIGN.md` Part 1 (architecture), Part 3 (service model),
//! Part 4 (endpoint management).
//!
//! # Role
//!
//! `ServiceRegistry` is the single entry point that callers use to
//! register services, add/remove endpoints, discover services, and
//! track health. It composes:
//!
//! - a service store (Phase 1),
//! - an endpoint manager (Phase 1),
//! - a health tracker (Phase 1),
//! - and a [`ServiceRegistryStats`] (observability).
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
use crate::service::ServiceInfo;
use crate::stats::ServiceRegistryStats;

/// The service registry facade.
///
/// Composes the service store, endpoint manager, health tracker, and stats.
/// Phase 0 ships a typed skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct ServiceRegistry {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: ServiceRegistryConfig,
    /// Atomic observability counters.
    stats: ServiceRegistryStats,
}

impl ServiceRegistry {
    /// Construct a registry from validated configuration.
    ///
    /// # Errors
    /// Returns [`ServiceRegistryError::ShuttingDown`] only if the config fails
    /// validation (mapped to ShuttingDown).
    pub fn new(cfg: ServiceRegistryConfig) -> Result<Self, ServiceRegistryError> {
        cfg.validate()?;
        Ok(Self {
            cfg,
            stats: ServiceRegistryStats::new(),
        })
    }

    /// Register a new service.
    ///
    /// # Errors
    /// Returns [`ServiceRegistryError::ShuttingDown`] in Phase 0.
    /// Phase 1 will return [`ServiceRegistryError::ServiceExists`] if already registered.
    pub fn register(&mut self, _info: ServiceInfo) -> Result<(), ServiceRegistryError> {
        // TODO(phase-1): store service + update stats.
        Err(ServiceRegistryError::ShuttingDown)
    }

    /// Unregister a service (graceful shutdown).
    ///
    /// # Errors
    /// Returns [`ServiceRegistryError::ServiceNotFound`] in Phase 0.
    /// Phase 1 will return [`ServiceRegistryError::ServiceDraining`] if still draining.
    pub fn unregister(
        &mut self,
        _id: crate::service::ServiceId,
    ) -> Result<(), ServiceRegistryError> {
        // TODO(phase-1): mark service draining + update stats.
        Err(ServiceRegistryError::ServiceNotFound(
            crate::service::ServiceId::nil(),
        ))
    }

    /// Discover a service's endpoint.
    ///
    /// # Errors
    /// Returns [`ServiceRegistryError::ServiceNotFound`] in Phase 0.
    /// Phase 1 will return [`ServiceRegistryError::NoHealthyInstance`] if no healthy endpoints.
    pub fn discover(
        &self,
        _id: crate::service::ServiceId,
    ) -> Result<ServiceEndpoint, ServiceRegistryError> {
        // TODO(phase-1): lookup service + select endpoint.
        self.stats.record_lookup(false);
        Err(ServiceRegistryError::ServiceNotFound(
            crate::service::ServiceId::nil(),
        ))
    }

    /// Add an endpoint to a service.
    ///
    /// # Errors
    /// Returns [`ServiceRegistryError::ShuttingDown`] in Phase 0.
    /// Phase 1 will return [`ServiceRegistryError::EndpointExists`] if endpoint already registered.
    pub fn add_endpoint(
        &mut self,
        _service: crate::service::ServiceId,
        _endpoint: ServiceEndpoint,
    ) -> Result<(), ServiceRegistryError> {
        // TODO(phase-1): add endpoint to service + update stats.
        Err(ServiceRegistryError::ShuttingDown)
    }

    /// Remove an endpoint from a service.
    ///
    /// # Errors
    /// Returns [`ServiceRegistryError::ShuttingDown`] in Phase 0.
    pub fn remove_endpoint(
        &mut self,
        _service: crate::service::ServiceId,
        _endpoint: ServiceEndpoint,
    ) -> Result<(), ServiceRegistryError> {
        // TODO(phase-1): remove endpoint + update stats.
        Err(ServiceRegistryError::ShuttingDown)
    }

    /// Snapshot the registry's health.
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

    fn make_registry() -> ServiceRegistry {
        let cfg = Preset::Local.to_config();
        ServiceRegistry::new(cfg).unwrap()
    }

    #[test]
    fn registry_constructs_from_valid_config() {
        let registry = make_registry();
        let h = registry.health();
        assert!(h.is_healthy());
    }

    #[test]
    fn registry_rejects_invalid_config() {
        let mut cfg = Preset::Local.to_config();
        cfg.max_services = 0; // invalid
        assert!(ServiceRegistry::new(cfg).is_err());
    }

    #[test]
    fn register_phase0_stub() {
        let mut registry = make_registry();
        let info = ServiceInfo::new(crate::service::ServiceId::new(1));
        assert!(registry.register(info).is_err());
    }

    #[test]
    fn unregister_phase0_stub() {
        let mut registry = make_registry();
        assert!(
            registry
                .unregister(crate::service::ServiceId::new(1))
                .is_err()
        );
    }

    #[test]
    fn discover_phase0_stub() {
        let registry = make_registry();
        let res = registry.discover(crate::service::ServiceId::new(1));
        assert!(matches!(res, Err(ServiceRegistryError::ServiceNotFound(_))));
    }

    #[test]
    fn add_endpoint_phase0_stub() {
        let mut registry = make_registry();
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        assert!(
            registry
                .add_endpoint(crate::service::ServiceId::new(1), endpoint)
                .is_err()
        );
    }

    #[test]
    fn remove_endpoint_phase0_stub() {
        let mut registry = make_registry();
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        assert!(
            registry
                .remove_endpoint(crate::service::ServiceId::new(1), endpoint)
                .is_err()
        );
    }

    #[test]
    fn config_accessor() {
        let registry = make_registry();
        assert_eq!(registry.config().max_services, 100);
    }

    #[test]
    fn stats_accessor() {
        let registry = make_registry();
        assert_eq!(registry.stats().snapshot().services_registered, 0);
    }
}

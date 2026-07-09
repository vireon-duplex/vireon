//! [`Container`] — dependency injection container (skeleton for Phase 0).
//!
//! See `DESIGN.md` Part 8 (container core).
//!
//! # Phase 0 Status
//!
//! This is a skeleton implementation. The full container with
//! service resolution, lifecycle management, and scope handling lands
//! in Phase 1+.

use crate::config::ContainerConfig;
use crate::error::DependencyError;
use crate::health::HealthReport;
use crate::stats::ContainerStats;
use crate::token::TokenId;

/// Dependency injection container (skeleton).
///
/// In Phase 0, this provides the structure and basic registration
/// without full resolution logic.
#[derive(Debug)]
pub struct Container {
    /// Container configuration.
    cfg: ContainerConfig,
    /// Statistics counter.
    stats: ContainerStats,
    /// Number of bindings registered (skeleton field).
    #[allow(dead_code)]
    binding_count: u32,
}

impl Container {
    /// Create a new container with the given configuration.
    #[must_use]
    pub fn new(cfg: ContainerConfig) -> Self {
        Self {
            cfg,
            stats: ContainerStats::new(),
            binding_count: 0,
        }
    }

    /// Register a service binding (skeleton).
    ///
    /// # Errors
    /// Returns [`DependencyError::ShuttingDown`] in this skeleton.
    pub fn register(&mut self, _binding: ()) -> Result<(), DependencyError> {
        // Phase 0: always return shutting down
        Err(DependencyError::ShuttingDown)
    }

    /// Resolve a service by token (skeleton).
    ///
    /// # Errors
    /// Returns [`DependencyError::ServiceNotRegistered`] in this skeleton.
    pub fn resolve(&mut self, _token: TokenId) -> Result<(), DependencyError> {
        // Phase 0: always return service not registered
        Err(DependencyError::ServiceNotRegistered(TokenId::nil()))
    }

    /// Create a new scope (skeleton).
    ///
    /// # Errors
    /// Returns [`DependencyError::ShuttingDown`] in this skeleton.
    pub fn create_scope(&mut self) -> Result<u64, DependencyError> {
        // Phase 0: always return shutting down
        Err(DependencyError::ShuttingDown)
    }

    /// Dispose a scope (skeleton).
    ///
    /// # Errors
    /// Returns [`DependencyError::ScopeClosed`] in this skeleton.
    pub fn dispose_scope(&mut self, _scope_id: u64) -> Result<(), DependencyError> {
        // Phase 0: always return scope closed
        Err(DependencyError::ScopeClosed)
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    /// Get the container configuration.
    #[must_use]
    pub const fn config(&self) -> &ContainerConfig {
        &self.cfg
    }

    /// Get the statistics.
    #[must_use]
    pub const fn stats(&self) -> &ContainerStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::ContainerBuilder;
    use crate::lifetime::ServiceLifetime;

    #[test]
    fn container_creation() {
        let cfg = ContainerBuilder::new().build().unwrap();
        let container = Container::new(cfg);
        let _ = format!("{container:?}"); // Silence unused warning
    }

    #[test]
    fn register_returns_shutting_down() {
        let cfg = ContainerBuilder::new().build().unwrap();
        let mut container = Container::new(cfg);
        let result = container.register(());
        assert!(matches!(result, Err(DependencyError::ShuttingDown)));
    }

    #[test]
    fn resolve_returns_service_not_registered() {
        let cfg = ContainerBuilder::new().build().unwrap();
        let mut container = Container::new(cfg);
        let token = TokenId::new(1);
        let result = container.resolve(token);
        assert!(matches!(
            result,
            Err(DependencyError::ServiceNotRegistered(_))
        ));
    }

    #[test]
    fn create_scope_returns_shutting_down() {
        let cfg = ContainerBuilder::new().build().unwrap();
        let mut container = Container::new(cfg);
        let result = container.create_scope();
        assert!(matches!(result, Err(DependencyError::ShuttingDown)));
    }

    #[test]
    fn dispose_scope_returns_scope_closed() {
        let cfg = ContainerBuilder::new().build().unwrap();
        let mut container = Container::new(cfg);
        let result = container.dispose_scope(123);
        assert!(matches!(result, Err(DependencyError::ScopeClosed)));
    }

    #[test]
    fn health_returns_healthy() {
        let cfg = ContainerBuilder::new().build().unwrap();
        let container = Container::new(cfg);
        let report = container.health();
        assert!(report.is_healthy());
    }

    #[test]
    fn config_returns_reference() {
        let cfg = ContainerBuilder::new().max_services(100).build().unwrap();
        let container = Container::new(cfg);
        let retrieved = container.config();
        assert_eq!(retrieved.max_services, 100);
    }

    #[test]
    fn stats_returns_reference() {
        let cfg = ContainerBuilder::new().build().unwrap();
        let container = Container::new(cfg);
        let stats = container.stats();
        let snap = stats.snapshot();
        assert_eq!(snap.registrations_total, 0);
    }

    #[test]
    fn container_with_custom_config() {
        let cfg = ContainerConfig {
            max_services: 50,
            max_scope_depth: 8,
            enable_lazy_resolution: false,
            default_lifetime: ServiceLifetime::Singleton,
            validate_on_register: true,
        };
        let container = Container::new(cfg);
        assert_eq!(container.config().max_services, 50);
        assert_eq!(container.config().max_scope_depth, 8);
    }

    #[test]
    fn container_stats_are_initialized() {
        let cfg = ContainerBuilder::new().build().unwrap();
        let container = Container::new(cfg);
        let snap = container.stats().snapshot();
        // All counters should start at 0
        assert_eq!(snap.registrations_total, 0);
        assert_eq!(snap.resolutions_total, 0);
        assert_eq!(snap.singletons_active, 0);
        assert_eq!(snap.scopes_active, 0);
    }

    #[test]
    fn health_report_derives_from_stats() {
        let cfg = ContainerBuilder::new().build().unwrap();
        let container = Container::new(cfg);
        let report = container.health();
        assert_eq!(report.state, crate::health::HealthState::Healthy);
    }
}

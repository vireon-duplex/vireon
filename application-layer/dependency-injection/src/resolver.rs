//! [`DependencyResolver`] — dependency graph resolution (skeleton for
//! Phase 0).
//!
//! See `DESIGN.md` Part 9 (dependency resolution).
//!
//! # Phase 0 Status
//!
//! This is a skeleton implementation. The full resolver with
//! topological sorting, cycle detection, and lazy resolution lands
//! in Phase 1+.

use crate::binding::BindingDescriptor;
use crate::config::ContainerConfig;
use crate::error::DependencyError;
use crate::stats::ContainerStats;
use crate::token::TokenId;

/// Dependency resolver (skeleton).
///
/// In Phase 0, this provides the structure without full resolution
/// logic.
#[derive(Debug)]
pub struct DependencyResolver {
    /// Container configuration.
    cfg: ContainerConfig,
    /// Statistics counter.
    stats: ContainerStats,
}

impl DependencyResolver {
    /// Create a new resolver with the given configuration.
    #[must_use]
    pub fn new(cfg: ContainerConfig) -> Self {
        Self {
            cfg,
            stats: ContainerStats::new(),
        }
    }

    /// Resolve the dependency graph starting from a root token (skeleton).
    ///
    /// # Errors
    /// Returns [`DependencyError::ShuttingDown`] in this skeleton.
    pub fn resolve_graph(
        &mut self,
        _root: TokenId,
        _bindings: &[BindingDescriptor],
    ) -> Result<Vec<TokenId>, DependencyError> {
        // Phase 0: always return shutting down
        Err(DependencyError::ShuttingDown)
    }

    /// Detect circular dependencies (skeleton).
    ///
    /// # Errors
    /// Returns [`DependencyError::ShuttingDown`] in this skeleton.
    pub fn detect_cycle(
        _root: TokenId,
        _bindings: &[BindingDescriptor],
    ) -> Result<bool, DependencyError> {
        // Phase 0: always return shutting down
        Err(DependencyError::ShuttingDown)
    }

    /// Perform topological sort on the dependency graph (skeleton).
    ///
    /// # Errors
    /// Returns [`DependencyError::ShuttingDown`] in this skeleton.
    pub fn topological_order(
        _bindings: &[BindingDescriptor],
    ) -> Result<Vec<TokenId>, DependencyError> {
        // Phase 0: always return shutting down
        Err(DependencyError::ShuttingDown)
    }

    /// Get the resolver configuration.
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

    #[test]
    fn resolver_creation() {
        let cfg = ContainerBuilder::new().build().unwrap();
        let resolver = DependencyResolver::new(cfg);
        let _ = format!("{resolver:?}"); // Silence unused warning
    }

    #[test]
    fn resolve_graph_returns_shutting_down() {
        let cfg = ContainerBuilder::new().build().unwrap();
        let mut resolver = DependencyResolver::new(cfg);
        let root = TokenId::new(1);
        let bindings = &[];
        let result = resolver.resolve_graph(root, bindings);
        assert!(matches!(result, Err(DependencyError::ShuttingDown)));
    }

    #[test]
    fn detect_cycle_returns_shutting_down() {
        let root = TokenId::new(1);
        let bindings = &[];
        let result = DependencyResolver::detect_cycle(root, bindings);
        assert!(matches!(result, Err(DependencyError::ShuttingDown)));
    }

    #[test]
    fn topological_order_returns_shutting_down() {
        let bindings = &[];
        let result = DependencyResolver::topological_order(bindings);
        assert!(matches!(result, Err(DependencyError::ShuttingDown)));
    }

    #[test]
    fn config_returns_reference() {
        let cfg = ContainerBuilder::new().max_services(100).build().unwrap();
        let resolver = DependencyResolver::new(cfg);
        let retrieved = resolver.config();
        assert_eq!(retrieved.max_services, 100);
    }

    #[test]
    fn stats_returns_reference() {
        let cfg = ContainerBuilder::new().build().unwrap();
        let resolver = DependencyResolver::new(cfg);
        let stats = resolver.stats();
        let snap = stats.snapshot();
        assert_eq!(snap.registrations_total, 0);
    }

    #[test]
    fn resolver_with_custom_config() {
        let cfg = ContainerConfig {
            max_services: 50,
            max_scope_depth: 8,
            enable_lazy_resolution: false,
            default_lifetime: crate::lifetime::ServiceLifetime::Singleton,
            validate_on_register: true,
        };
        let resolver = DependencyResolver::new(cfg);
        assert_eq!(resolver.config().max_services, 50);
        assert_eq!(resolver.config().max_scope_depth, 8);
    }

    #[test]
    fn resolver_stats_are_initialized() {
        let cfg = ContainerBuilder::new().build().unwrap();
        let resolver = DependencyResolver::new(cfg);
        let snap = resolver.stats().snapshot();
        // All counters should start at 0
        assert_eq!(snap.registrations_total, 0);
        assert_eq!(snap.resolutions_total, 0);
    }
}

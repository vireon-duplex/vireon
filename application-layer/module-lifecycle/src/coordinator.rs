//! [`LifecycleCoordinator`] — the main lifecycle coordinator facade.
//!
//! See `DESIGN.md` Part 1 (architecture), Part 8 (coordinator).
//!
//! # Role
//!
//! `LifecycleCoordinator` is the single entry point that callers use to
//! register modules and drive phase transitions. It composes:
//!
//! - a [`DependencyGraph`] (dependency resolution),
//! - module state tracking,
//! - a [`ModuleLifecycleStats`] (observability).
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a
//! stub error so that callers can code against the API contract
//! today. Phase 1 fills in the real logic.

use crate::ModuleDescriptor;
use crate::config::LifecycleConfig;
use crate::error::ModuleLifecycleError;
use crate::health::HealthReport;
use crate::stats::ModuleLifecycleStats;

/// The lifecycle coordinator facade.
///
/// Composes the dependency graph and stats. Phase 0 ships a typed
/// skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct LifecycleCoordinator {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: LifecycleConfig,
    /// Atomic observability counters.
    stats: ModuleLifecycleStats,
}

impl LifecycleCoordinator {
    /// Construct a coordinator from validated configuration.
    ///
    /// # Errors
    /// Returns [`ModuleLifecycleError::InvalidTransition`] if the config fails
    /// validation.
    pub fn new(cfg: LifecycleConfig) -> Result<Self, ModuleLifecycleError> {
        cfg.validate()?;
        Ok(Self {
            cfg,
            stats: ModuleLifecycleStats::new(),
        })
    }

    /// Register a module descriptor.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn register(&mut self, _desc: ModuleDescriptor) -> Result<(), ModuleLifecycleError> {
        // TODO(phase-1): add to graph + stats tracking.
        self.stats.record_registered();
        Err(ModuleLifecycleError::ShuttingDown)
    }

    /// Initialize all registered modules in dependency order.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn init_all(&mut self) -> Result<u64, ModuleLifecycleError> {
        // TODO(phase-1): topo sort + parallel init.
        Err(ModuleLifecycleError::ShuttingDown)
    }

    /// Start all initialized modules.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn start_all(&mut self) -> Result<u64, ModuleLifecycleError> {
        // TODO(phase-1): start in dependency order.
        Err(ModuleLifecycleError::ShuttingDown)
    }

    /// Stop all active modules.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn stop_all(&mut self) -> Result<u64, ModuleLifecycleError> {
        // TODO(phase-1): stop in reverse dependency order.
        Err(ModuleLifecycleError::ShuttingDown)
    }

    /// Snapshot the coordinator's health.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    /// Reference to the underlying stats (for testing / metrics).
    #[must_use]
    pub fn stats(&self) -> &ModuleLifecycleStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;

    fn make_coordinator() -> LifecycleCoordinator {
        let cfg = Preset::Eager.to_config();
        LifecycleCoordinator::new(cfg).unwrap()
    }

    #[test]
    fn coordinator_constructs_from_valid_config() {
        let coord = make_coordinator();
        let h = coord.health();
        assert!(h.is_healthy());
    }

    #[test]
    fn coordinator_rejects_invalid_config() {
        let mut cfg = Preset::Eager.to_config();
        cfg.max_modules = 0; // invalid
        assert!(LifecycleCoordinator::new(cfg).is_err());
    }

    #[test]
    fn register_phase0_stub() {
        let mut coord = make_coordinator();
        let desc = ModuleDescriptor::new(crate::ModuleId::new(1)).with_name("test");
        assert!(coord.register(desc).is_err());
    }

    #[test]
    fn init_all_phase0_stub() {
        let mut coord = make_coordinator();
        assert!(coord.init_all().is_err());
    }

    #[test]
    fn start_all_phase0_stub() {
        let mut coord = make_coordinator();
        assert!(coord.start_all().is_err());
    }

    #[test]
    fn stop_all_phase0_stub() {
        let mut coord = make_coordinator();
        assert!(coord.stop_all().is_err());
    }
}

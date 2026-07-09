//! [`HookRegistry`] — typed skeleton for hook registration and
//! invocation.
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a
//! stub error so that callers can code against the API contract
//! today. Phase 1 fills in the real logic.

use crate::config::SchedulerHooksConfig;
use crate::error::SchedulerHookError;
use crate::health::HealthReport;
use crate::hook::{HookTrigger, SchedulerHookId};
use crate::priority::HookPriority;
use crate::stats::SchedulerHookStats;

/// Hook registry facade.
///
/// Manages hook registration, unregistration, and invocation.
/// Phase 0 ships a typed skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct HookRegistry {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: SchedulerHooksConfig,
    /// Atomic observability counters.
    stats: SchedulerHookStats,
}

impl HookRegistry {
    /// Construct a registry from validated configuration.
    ///
    /// # Errors
    /// Returns `SchedulerHookError::InvalidPhase` if the config fails
    /// validation.
    pub fn new(cfg: SchedulerHooksConfig) -> Result<Self, SchedulerHookError> {
        cfg.validate()?;
        Ok(Self {
            cfg,
            stats: SchedulerHookStats::new(),
        })
    }

    /// Register a new hook.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn register(
        &mut self,
        _id: SchedulerHookId,
        _trigger: HookTrigger,
        _priority: HookPriority,
    ) -> Result<(), SchedulerHookError> {
        // TODO(phase-1): registry insert + validation.
        Err(SchedulerHookError::ShuttingDown)
    }

    /// Unregister a hook.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn unregister(&mut self, _id: SchedulerHookId) -> Result<(), SchedulerHookError> {
        // TODO(phase-1): registry remove.
        Err(SchedulerHookError::ShuttingDown)
    }

    /// Invoke all hooks for a given trigger.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn invoke(&mut self, _trigger: HookTrigger) -> Result<u64, SchedulerHookError> {
        // TODO(phase-1): lookup hooks by trigger + priority order + invoke.
        Err(SchedulerHookError::ShuttingDown)
    }

    /// Snapshot the registry's health.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    /// Reference to the underlying stats (for testing / metrics).
    #[must_use]
    pub fn stats(&self) -> &SchedulerHookStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;

    #[test]
    fn registry_constructs_from_valid_config() {
        let cfg = Preset::Minimal.to_config();
        let registry = HookRegistry::new(cfg).unwrap();
        let h = registry.health();
        assert!(h.is_healthy());
    }

    #[test]
    fn registry_rejects_invalid_config() {
        let mut cfg = Preset::Minimal.to_config();
        cfg.max_hooks = 0; // invalid
        assert!(HookRegistry::new(cfg).is_err());
    }

    #[test]
    fn register_phase0_stub() {
        let cfg = Preset::Minimal.to_config();
        let mut registry = HookRegistry::new(cfg).unwrap();
        let id = SchedulerHookId::new(1);
        assert!(
            registry
                .register(id, HookTrigger::PreTick, HookPriority::NORMAL)
                .is_err()
        );
    }

    #[test]
    fn unregister_phase0_stub() {
        let cfg = Preset::Minimal.to_config();
        let mut registry = HookRegistry::new(cfg).unwrap();
        let id = SchedulerHookId::new(1);
        assert!(registry.unregister(id).is_err());
    }

    #[test]
    fn invoke_phase0_stub() {
        let cfg = Preset::Minimal.to_config();
        let mut registry = HookRegistry::new(cfg).unwrap();
        assert!(registry.invoke(HookTrigger::PreTick).is_err());
    }
}

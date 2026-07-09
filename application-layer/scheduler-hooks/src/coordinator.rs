//! [`HookCoordinator`] — typed skeleton for tick lifecycle coordination.
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a
//! stub error so that callers can code against the API contract
//! today. Phase 1 fills in the real logic.

use crate::config::SchedulerHooksConfig;
use crate::error::SchedulerHookError;
use crate::health::HealthReport;
use crate::stats::SchedulerHookStats;

/// Hook coordinator facade.
///
/// Coordinates hook invocation across the tick lifecycle.
/// Phase 0 ships a typed skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct HookCoordinator {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: SchedulerHooksConfig,
    /// Atomic observability counters.
    stats: SchedulerHookStats,
    /// Tick count (for health monitoring).
    tick_count: u64,
}

impl HookCoordinator {
    /// Construct a coordinator from validated configuration.
    #[must_use]
    pub fn new(cfg: SchedulerHooksConfig) -> Self {
        Self {
            cfg,
            stats: SchedulerHookStats::new(),
            tick_count: 0,
        }
    }

    /// Called on each tick.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn on_tick(&mut self) -> Result<u64, SchedulerHookError> {
        // TODO(phase-1): invoke pre-tick hooks, track time, invoke post-tick hooks.
        self.tick_count = self.tick_count.saturating_add(1);
        self.stats.record_tick();
        Err(SchedulerHookError::ShuttingDown)
    }

    /// Called before submitting work.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn on_submit(&mut self) -> Result<(), SchedulerHookError> {
        // TODO(phase-1): invoke pre-submit hooks.
        Err(SchedulerHookError::ShuttingDown)
    }

    /// Snapshot the coordinator's health.
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
    fn coordinator_constructs() {
        let cfg = Preset::Minimal.to_config();
        let coordinator = HookCoordinator::new(cfg);
        let h = coordinator.health();
        assert!(h.is_healthy());
    }

    #[test]
    fn on_tick_phase0_stub() {
        let cfg = Preset::Minimal.to_config();
        let mut coordinator = HookCoordinator::new(cfg);
        assert!(coordinator.on_tick().is_err());
    }

    #[test]
    fn on_submit_phase0_stub() {
        let cfg = Preset::Minimal.to_config();
        let mut coordinator = HookCoordinator::new(cfg);
        assert!(coordinator.on_submit().is_err());
    }
}

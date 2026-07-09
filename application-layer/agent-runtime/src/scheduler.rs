//! [`AgentScheduler`] — agent scheduling facade (typed skeleton).
//!
//! This is a **Phase 0 skeleton**. The full implementation lands in
//! Phase 1+.
//!
//! See `DESIGN.md` Part 11 (scheduler).

use crate::agent::AgentId;
use crate::config::AgentRuntimeConfig;
use crate::error::AgentRuntimeError;
use crate::health::HealthReport;
use crate::stats::AgentRuntimeStats;
use std::fmt;

/// Agent scheduler facade.
///
/// In Phase 0, this is a typed skeleton. Phase 1+ will include:
/// - Priority-based agent scheduling
/// - Fair scheduling across agents
/// - Preemption and yielding
/// - Scheduler queue management
pub struct AgentScheduler {
    /// Scheduler configuration.
    #[allow(dead_code)]
    cfg: AgentRuntimeConfig,
    /// Scheduler statistics.
    stats: AgentRuntimeStats,
}

impl fmt::Debug for AgentScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentScheduler")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .finish()
    }
}

impl AgentScheduler {
    /// Create a new agent scheduler with the given config.
    #[must_use]
    pub fn new(cfg: AgentRuntimeConfig) -> Self {
        Self {
            cfg,
            stats: AgentRuntimeStats::new(),
        }
    }

    /// Schedule an agent for execution.
    ///
    /// # Errors
    /// Returns [`AgentRuntimeError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    // TODO(phase-1): Implement agent scheduling.
    pub fn schedule(&self, _agent: AgentId) -> Result<(), AgentRuntimeError> {
        // TODO(phase-1): Implement agent scheduling.
        Err(AgentRuntimeError::ShuttingDown)
    }

    /// Get the next agent to execute.
    ///
    /// # Errors
    /// Returns [`AgentRuntimeError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    // TODO(phase-1): Implement next-agent selection.
    pub fn next_agent(&self) -> Result<AgentId, AgentRuntimeError> {
        // TODO(phase-1): Implement next-agent selection.
        Err(AgentRuntimeError::ShuttingDown)
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_creation() {
        let cfg = AgentRuntimeConfig::new();
        let scheduler = AgentScheduler::new(cfg);
        assert_eq!(scheduler.stats.snapshot().agents_created, 0);
    }

    #[test]
    fn schedule_returns_shutting_down() {
        let scheduler = AgentScheduler::new(AgentRuntimeConfig::new());
        let result = scheduler.schedule(AgentId::new(1));
        assert!(matches!(result, Err(AgentRuntimeError::ShuttingDown)));
    }

    #[test]
    fn next_agent_returns_shutting_down() {
        let scheduler = AgentScheduler::new(AgentRuntimeConfig::new());
        let result = scheduler.next_agent();
        assert!(matches!(result, Err(AgentRuntimeError::ShuttingDown)));
    }

    #[test]
    fn health_report_initially_healthy() {
        let scheduler = AgentScheduler::new(AgentRuntimeConfig::new());
        let health = scheduler.health();
        assert!(health.is_healthy());
        assert_eq!(health.active_agents, 0);
    }
}

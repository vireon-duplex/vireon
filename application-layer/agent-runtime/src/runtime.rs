//! [`AgentRuntime`] — agent runtime facade (typed skeleton).
//!
//! This is a **Phase 0 skeleton**. The full implementation lands in
//! Phase 1+.
//!
//! See `DESIGN.md` Part 9 (runtime).

use crate::agent::AgentId;
use crate::config::AgentRuntimeConfig;
use crate::error::AgentRuntimeError;
use crate::health::HealthReport;
use crate::stats::AgentRuntimeStats;
use std::fmt;

/// Agent runtime facade.
///
/// In Phase 0, this is a typed skeleton. Phase 1+ will include:
/// - Agent lifecycle management (create/stop)
/// - Agent state tracking
/// - Tool registration and dispatch
/// - Context management per agent
pub struct AgentRuntime {
    /// Runtime configuration.
    #[allow(dead_code)]
    cfg: AgentRuntimeConfig,
    /// Runtime statistics.
    stats: AgentRuntimeStats,
}

impl fmt::Debug for AgentRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentRuntime")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .finish()
    }
}

impl AgentRuntime {
    /// Create a new agent runtime with the given config.
    #[must_use]
    pub fn new(cfg: AgentRuntimeConfig) -> Self {
        Self {
            cfg,
            stats: AgentRuntimeStats::new(),
        }
    }

    /// Create a new agent.
    ///
    /// # Errors
    /// Returns [`AgentRuntimeError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    // TODO(phase-1): Implement agent creation with ID generation.
    pub fn create_agent(&self) -> Result<AgentId, AgentRuntimeError> {
        // TODO(phase-1): Implement agent creation.
        Err(AgentRuntimeError::ShuttingDown)
    }

    /// Stop an agent.
    ///
    /// # Errors
    /// Returns [`AgentRuntimeError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    // TODO(phase-1): Implement agent stopping.
    pub fn stop_agent(&self, _agent: AgentId) -> Result<(), AgentRuntimeError> {
        // TODO(phase-1): Implement agent stopping.
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
    fn runtime_creation() {
        let cfg = AgentRuntimeConfig::new();
        let runtime = AgentRuntime::new(cfg);
        assert_eq!(runtime.stats.snapshot().agents_created, 0);
    }

    #[test]
    fn create_agent_returns_shutting_down() {
        let runtime = AgentRuntime::new(AgentRuntimeConfig::new());
        let result = runtime.create_agent();
        assert!(matches!(result, Err(AgentRuntimeError::ShuttingDown)));
    }

    #[test]
    fn stop_agent_returns_shutting_down() {
        let runtime = AgentRuntime::new(AgentRuntimeConfig::new());
        let result = runtime.stop_agent(AgentId::new(1));
        assert!(matches!(result, Err(AgentRuntimeError::ShuttingDown)));
    }

    #[test]
    fn health_report_initially_healthy() {
        let runtime = AgentRuntime::new(AgentRuntimeConfig::new());
        let health = runtime.health();
        assert!(health.is_healthy());
        assert_eq!(health.active_agents, 0);
    }
}

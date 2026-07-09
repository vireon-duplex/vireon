//! [`AgentExecutor`] — tool execution facade (typed skeleton).
//!
//! This is a **Phase 0 skeleton**. The full implementation lands in
//! Phase 1+.
//!
//! See `DESIGN.md` Part 10 (executor).

use crate::agent::AgentId;
use crate::config::AgentRuntimeConfig;
use crate::error::AgentRuntimeError;
use crate::health::HealthReport;
use crate::stats::AgentRuntimeStats;
use crate::tool::{ToolDescriptor, ToolResult};
use std::fmt;

/// Agent executor facade.
///
/// In Phase 0, this is a typed skeleton. Phase 1+ will include:
/// - Tool execution with timeout enforcement
/// - Tool result aggregation
/// - Concurrent tool dispatch
/// - Tool failure tracking and retry
pub struct AgentExecutor {
    /// Executor configuration.
    #[allow(dead_code)]
    cfg: AgentRuntimeConfig,
    /// Executor statistics.
    stats: AgentRuntimeStats,
}

impl fmt::Debug for AgentExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentExecutor")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .finish()
    }
}

impl AgentExecutor {
    /// Create a new agent executor with the given config.
    #[must_use]
    pub fn new(cfg: AgentRuntimeConfig) -> Self {
        Self {
            cfg,
            stats: AgentRuntimeStats::new(),
        }
    }

    /// Execute a tool for the given agent.
    ///
    /// # Errors
    /// Returns [`AgentRuntimeError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    // TODO(phase-1): Implement tool execution with timeout.
    pub fn execute_tool(
        &self,
        _agent: AgentId,
        _tool: &ToolDescriptor,
    ) -> Result<ToolResult, AgentRuntimeError> {
        // TODO(phase-1): Implement tool execution.
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
    use crate::tool::ToolId;

    #[test]
    fn executor_creation() {
        let cfg = AgentRuntimeConfig::new();
        let executor = AgentExecutor::new(cfg);
        assert_eq!(executor.stats.snapshot().tool_calls_initiated, 0);
    }

    #[test]
    fn execute_tool_returns_shutting_down() {
        let executor = AgentExecutor::new(AgentRuntimeConfig::new());
        let tool = ToolDescriptor::new(ToolId::new(1), "search");
        let result = executor.execute_tool(AgentId::new(1), &tool);
        assert!(matches!(result, Err(AgentRuntimeError::ShuttingDown)));
    }

    #[test]
    fn health_report_initially_healthy() {
        let executor = AgentExecutor::new(AgentRuntimeConfig::new());
        let health = executor.health();
        assert!(health.is_healthy());
        assert_eq!(health.tool_success_rate_bps, 10_000);
    }
}

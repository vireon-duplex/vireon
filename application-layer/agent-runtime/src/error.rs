//! [`AgentRuntimeError`] — the typed failure surface of the agent
//! runtime.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into categories, mirroring the
//! [`AgentRuntimeError::is_fatal`] / [`AgentRuntimeError::is_retryable`]
//! predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `AgentExists`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `ToolFailed` | Back off + retry |
//! | Not found | `AgentNotFound` | Check agent ID |
//! | Capacity | `SchedulerFull` | Back off + retry |

use crate::agent::AgentId;
use thiserror::Error;

// ── AgentRuntimeError ────────────────────────────────────────────

/// Typed failure surface for the agent runtime.
///
/// Every fallible agent runtime API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum AgentRuntimeError {
    /// The requested agent does not exist.
    #[error("agent not found: {0}")]
    AgentNotFound(AgentId),

    /// An agent with the given ID already exists.
    #[error("agent already exists: {0}")]
    AgentExists(AgentId),

    /// A tool execution failed.
    #[error("tool failed: {0}")]
    ToolFailed(&'static str),

    /// The scheduler is at capacity and cannot accept new work.
    #[error("scheduler full")]
    SchedulerFull,

    /// The agent runtime is shutting down.
    #[error("agent runtime is shutting down")]
    ShuttingDown,
}

impl AgentRuntimeError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`AgentExists`)
    /// or terminal lifecycle states (`ShuttingDown`) that no amount
    /// of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::AgentExists(_) | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (tool execution
    /// failures, scheduler capacity).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::ToolFailed(_) | Self::SchedulerFull)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(AgentRuntimeError::AgentExists(AgentId::new(1)).is_fatal());
        assert!(AgentRuntimeError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!AgentRuntimeError::AgentNotFound(AgentId::new(1)).is_fatal());
        assert!(!AgentRuntimeError::ToolFailed("temp").is_fatal());
        assert!(!AgentRuntimeError::SchedulerFull.is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(AgentRuntimeError::ToolFailed("temp overload").is_retryable());
        assert!(AgentRuntimeError::SchedulerFull.is_retryable());
        // Non-retryable examples.
        assert!(!AgentRuntimeError::ShuttingDown.is_retryable());
        assert!(!AgentRuntimeError::AgentExists(AgentId::new(1)).is_retryable());
        assert!(!AgentRuntimeError::AgentNotFound(AgentId::new(1)).is_retryable());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = AgentRuntimeError::AgentNotFound(AgentId::new(42));
        let s = format!("{e}");
        assert!(s.contains("agent not found"));
        assert!(s.contains("42"));

        let e = AgentRuntimeError::AgentExists(AgentId::new(1));
        assert!(format!("{e}").contains("agent already exists"));

        let e = AgentRuntimeError::ToolFailed("timeout");
        assert!(format!("{e}").contains("tool failed"));

        let e = AgentRuntimeError::SchedulerFull;
        assert!(format!("{e}").contains("scheduler full"));

        let e = AgentRuntimeError::ShuttingDown;
        assert!(format!("{e}").contains("shutting down"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = AgentRuntimeError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let retry = AgentRuntimeError::ToolFailed("temp");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
    }

    #[test]
    fn agent_not_found_with_valid_id() {
        let e = AgentRuntimeError::AgentNotFound(AgentId::new(123));
        assert!(format!("{e}").contains("not found"));
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn agent_exists_is_fatal() {
        let e = AgentRuntimeError::AgentExists(AgentId::new(1));
        assert!(e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn tool_failed_is_retryable() {
        let e = AgentRuntimeError::ToolFailed("network error");
        assert!(!e.is_fatal());
        assert!(e.is_retryable());
    }

    #[test]
    fn scheduler_full_is_retryable() {
        let e = AgentRuntimeError::SchedulerFull;
        assert!(!e.is_fatal());
        assert!(e.is_retryable());
    }

    #[test]
    fn shutting_down_is_fatal() {
        let e = AgentRuntimeError::ShuttingDown;
        assert!(e.is_fatal());
        assert!(!e.is_retryable());
    }
}

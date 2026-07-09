//! [`ShutdownError`] — the typed failure surface of the graceful
//! startup and shutdown system.
//!
//! See `DESIGN.md` Part 11 (failure modes).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`ShutdownError::is_fatal`] / [`ShutdownError::is_retryable`] /
//! [`ShutdownError::is_lifecycle`] predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `ForcedShutdown`, `StartupFailed`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `PhaseTimeout`, `DependencyFailed` | Back off + retry |
//! | Lifecycle | `AlreadyShuttingDown`, `NotStarted`, `InvalidPhase` | Check state + retry |

use thiserror::Error;

// ── ShutdownError ───────────────────────────────────────────────

/// Typed failure surface for the graceful startup and shutdown system.
///
/// Every fallible API returns one of these variants so that callers can
/// branch on a small, exhaustive set of failure modes instead of
/// string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ShutdownError {
    /// A startup or shutdown phase timed out.
    #[error("phase timeout: {0}")]
    PhaseTimeout(&'static str),

    /// A required dependency failed to start or stop.
    #[error("dependency failed: {0}")]
    DependencyFailed(&'static str),

    /// Forced shutdown was initiated (unsafe).
    #[error("forced shutdown initiated")]
    ForcedShutdown,

    /// Shutdown was already in progress.
    #[error("already shutting down")]
    AlreadyShuttingDown,

    /// Startup failed and cannot proceed.
    #[error("startup failed: {0}")]
    StartupFailed(&'static str),

    /// Operation attempted before startup began.
    #[error("not started")]
    NotStarted,

    /// Invalid phase transition requested.
    #[error("invalid phase: {0}")]
    InvalidPhase(&'static str),

    /// System is shutting down; reject new operations.
    #[error("shutting down")]
    ShuttingDown,
}

impl ShutdownError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate terminal states or unrecoverable
    /// conditions that no amount of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::ForcedShutdown | Self::StartupFailed(_) | Self::ShuttingDown
        )
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (timeout, dependency
    /// temporarily unavailable).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::PhaseTimeout(_) | Self::DependencyFailed(_))
    }

    /// Whether the error reflects a lifecycle state issue.
    ///
    /// Lifecycle errors signal the system is in a state that doesn't
    /// support the requested operation.
    #[must_use]
    pub const fn is_lifecycle(&self) -> bool {
        matches!(
            self,
            Self::AlreadyShuttingDown | Self::NotStarted | Self::InvalidPhase(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(ShutdownError::ForcedShutdown.is_fatal());
        assert!(ShutdownError::StartupFailed("test").is_fatal());
        assert!(ShutdownError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!ShutdownError::PhaseTimeout("test").is_fatal());
        assert!(!ShutdownError::AlreadyShuttingDown.is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(ShutdownError::PhaseTimeout("test").is_retryable());
        assert!(ShutdownError::DependencyFailed("test").is_retryable());
        // Non-retryable examples.
        assert!(!ShutdownError::ForcedShutdown.is_retryable());
        assert!(!ShutdownError::StartupFailed("test").is_retryable());
    }

    #[test]
    fn lifecycle_classification() {
        assert!(ShutdownError::AlreadyShuttingDown.is_lifecycle());
        assert!(ShutdownError::NotStarted.is_lifecycle());
        assert!(ShutdownError::InvalidPhase("test").is_lifecycle());
        // Non-lifecycle examples.
        assert!(!ShutdownError::PhaseTimeout("test").is_lifecycle());
        assert!(!ShutdownError::ForcedShutdown.is_lifecycle());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = ShutdownError::PhaseTimeout("Init");
        let s = format!("{e}");
        assert!(s.contains("phase timeout"));
        assert!(s.contains("Init"));

        let e = ShutdownError::DependencyFailed("Config");
        assert!(format!("{e}").contains("dependency failed"));

        let e = ShutdownError::ForcedShutdown;
        assert!(format!("{e}").contains("forced shutdown"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = ShutdownError::ForcedShutdown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());
        assert!(!fatal.is_lifecycle());

        let retry = ShutdownError::PhaseTimeout("test");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
        assert!(!retry.is_lifecycle());

        let lifecycle = ShutdownError::AlreadyShuttingDown;
        assert!(!lifecycle.is_fatal());
        assert!(!lifecycle.is_retryable());
        assert!(lifecycle.is_lifecycle());
    }

    #[test]
    fn invalid_phase_is_lifecycle() {
        let e = ShutdownError::InvalidPhase("InvalidTransition");
        assert!(e.is_lifecycle());
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn shutting_down_is_fatal() {
        let e = ShutdownError::ShuttingDown;
        assert!(e.is_fatal());
        assert!(!e.is_retryable());
        assert!(!e.is_lifecycle());
    }
}

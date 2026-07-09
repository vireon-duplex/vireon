//! [`SchedulerHookError`] — typed failure surface for the scheduler
//! hooks system.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12 (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`SchedulerHookError::is_fatal`] / [`SchedulerHookError::is_retryable`]
//! predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `HookPanic`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `HookFailed` | Back off + retry |
//! | Programming | `DuplicateHook`, `InvalidPhase` | Fix code |

use crate::hook::SchedulerHookId;

// ── SchedulerHookError ─────────────────────────────────────────────

/// Typed failure surface for the scheduler hooks system.
///
/// Every fallible hook API returns one of these variants so that
/// callers can branch on a small, exhaustive set of failure modes
/// instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SchedulerHookError {
    /// A hook panicked during invocation.
    #[error("hook panicked: {0}")]
    HookPanic(String),

    /// A hook with this ID is already registered.
    #[error("duplicate hook: {0:?}")]
    DuplicateHook(SchedulerHookId),

    /// The hook phase is invalid for this operation.
    #[error("invalid phase: {0}")]
    InvalidPhase(&'static str),

    /// A hook failed to execute (non-panic error).
    #[error("hook failed: {0}")]
    HookFailed(&'static str),

    /// The hook system is shutting down; reject new operations.
    #[error("hook system is shutting down")]
    ShuttingDown,
}

impl SchedulerHookError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate terminal lifecycle states
    /// (`ShuttingDown`) or unrecoverable errors (`HookPanic`) that
    /// no amount of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::HookPanic(_) | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (hook temporarily
    /// failing due to resource constraints).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::HookFailed(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(SchedulerHookError::HookPanic("panic".into()).is_fatal());
        assert!(SchedulerHookError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!SchedulerHookError::DuplicateHook(SchedulerHookId::nil()).is_fatal());
        assert!(!SchedulerHookError::InvalidPhase("test").is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(SchedulerHookError::HookFailed("temp").is_retryable());
        // Non-retryable examples.
        assert!(!SchedulerHookError::ShuttingDown.is_retryable());
        assert!(!SchedulerHookError::HookPanic("x".into()).is_retryable());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = SchedulerHookError::DuplicateHook(SchedulerHookId::new(42));
        let s = format!("{e}");
        assert!(s.contains("duplicate hook"));
        assert!(s.contains("42"));

        let e = SchedulerHookError::InvalidPhase("pre_tick");
        assert!(format!("{e}").contains("invalid phase"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = SchedulerHookError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let retry = SchedulerHookError::HookFailed("temp");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());

        let neither = SchedulerHookError::InvalidPhase("x");
        assert!(!neither.is_fatal());
        assert!(!neither.is_retryable());
    }

    #[test]
    fn hook_panic_error() {
        let e = SchedulerHookError::HookPanic("division by zero".into());
        assert!(format!("{e}").contains("hook panicked"));
        assert!(e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn duplicate_hook_error() {
        let id = SchedulerHookId::new(123);
        let e = SchedulerHookError::DuplicateHook(id);
        assert!(format!("{e}").contains("duplicate hook"));
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn invalid_phase_error() {
        let e = SchedulerHookError::InvalidPhase("unknown_phase");
        assert!(format!("{e}").contains("invalid phase"));
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }
}

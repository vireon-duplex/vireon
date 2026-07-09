//! [`ModuleLifecycleError`] — the typed failure surface of the module
//! lifecycle system.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`ModuleLifecycleError::is_fatal`] / [`ModuleLifecycleError::is_retryable`]
//! predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `DependencyCycle`, `InvalidTransition`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `InitFailed`, `StartFailed`, `StopFailed` | Back off + retry |
//! | Capacity | `DuplicateModule`, `AlreadyRunning`, `NotRunning` | Shed load / fix state |

use crate::id::ModuleId;
use thiserror::Error;

// ── ModuleLifecycleError ─────────────────────────────────────────────

/// Typed failure surface for the module lifecycle system.
///
/// Every fallible lifecycle API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ModuleLifecycleError {
    /// A dependency cycle was detected in the module graph.
    #[error("dependency cycle detected involving module {0}")]
    DependencyCycle(ModuleId),

    /// Module initialization failed.
    #[error("initialization failed for module: {0}")]
    InitFailed(&'static str),

    /// Module startup failed.
    #[error("startup failed for module: {0}")]
    StartFailed(&'static str),

    /// Module shutdown failed.
    #[error("shutdown failed for module: {0}")]
    StopFailed(&'static str),

    /// Module is already running; operation not permitted.
    #[error("module {0:?} is already running")]
    AlreadyRunning(ModuleId),

    /// Module is not running; operation not permitted.
    #[error("module {0:?} is not running")]
    NotRunning(ModuleId),

    /// Invalid phase transition requested.
    #[error("invalid phase transition: {0}")]
    InvalidTransition(&'static str),

    /// A module with the given id is already registered.
    #[error("module {0:?} is already registered")]
    DuplicateModule(ModuleId),

    /// A module dependency failed; this module cannot proceed.
    #[error("dependency {0:?} failed")]
    DependencyFailed(ModuleId),

    /// System is shutting down; reject new operations.
    #[error("lifecycle coordinator is shutting down")]
    ShuttingDown,
}

impl ModuleLifecycleError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`DependencyCycle`,
    /// `InvalidTransition`) or terminal lifecycle states
    /// (`ShuttingDown`) that no amount of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::DependencyCycle(_) | Self::InvalidTransition(_) | Self::ShuttingDown
        )
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (initialization
    /// temporarily failed, startup racing with another operation).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::InitFailed(_) | Self::StartFailed(_) | Self::StopFailed(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(ModuleLifecycleError::DependencyCycle(ModuleId::new(1)).is_fatal());
        assert!(ModuleLifecycleError::InvalidTransition("bad").is_fatal());
        assert!(ModuleLifecycleError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!ModuleLifecycleError::InitFailed("temp").is_fatal());
        assert!(!ModuleLifecycleError::StartFailed("temp").is_fatal());
        assert!(!ModuleLifecycleError::DuplicateModule(ModuleId::new(1)).is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(ModuleLifecycleError::InitFailed("temp").is_retryable());
        assert!(ModuleLifecycleError::StartFailed("temp").is_retryable());
        assert!(ModuleLifecycleError::StopFailed("temp").is_retryable());
        // Non-retryable examples.
        assert!(!ModuleLifecycleError::DependencyCycle(ModuleId::new(1)).is_retryable());
        assert!(!ModuleLifecycleError::ShuttingDown.is_retryable());
        assert!(!ModuleLifecycleError::DuplicateModule(ModuleId::new(1)).is_retryable());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = ModuleLifecycleError::DependencyCycle(ModuleId::new(42));
        let s = format!("{e}");
        assert!(s.contains("dependency cycle"));
        assert!(s.contains("42"));

        let e = ModuleLifecycleError::ShuttingDown;
        assert!(format!("{e}").contains("shutting down"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = ModuleLifecycleError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let retry = ModuleLifecycleError::InitFailed("temp");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());

        let neither = ModuleLifecycleError::DuplicateModule(ModuleId::new(1));
        assert!(!neither.is_fatal());
        assert!(!neither.is_retryable());
    }

    #[test]
    fn dependency_cycle_includes_module_id() {
        let id = ModuleId::new(7);
        let e = ModuleLifecycleError::DependencyCycle(id);
        let _ = format!("{e:?}"); // Silence unused format warning
        assert!(matches!(e, ModuleLifecycleError::DependencyCycle(mid) if mid.raw() == 7));
    }
}

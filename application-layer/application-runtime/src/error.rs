//! [`AppRuntimeError`] — the typed failure surface of the
//! application-runtime crate.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`AppRuntimeError::is_fatal`] / [`AppRuntimeError::is_retryable`]
//! predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `AlreadyRunning`, `NotRunning`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `ModuleFailed`, `CompositionFailed` | Back off + retry |
//! | Capacity | (none yet) | Shed load / wait |

use thiserror::Error;

// ── AppRuntimeError ─────────────────────────────────────────────

/// Typed failure surface for the application-runtime crate.
///
/// Every fallible API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum AppRuntimeError {
    /// The runtime is already running; cannot start again.
    #[error("application runtime is already running")]
    AlreadyRunning,

    /// The runtime is not running; cannot stop or query.
    #[error("application runtime is not running")]
    NotRunning,

    /// A module failed during startup or operation.
    #[error("module failed: {0}")]
    ModuleFailed(&'static str),

    /// Composition of subsystems failed.
    #[error("composition failed: {0}")]
    CompositionFailed(&'static str),

    /// The runtime is shutting down; reject new operations.
    #[error("application runtime is shutting down")]
    ShuttingDown,
}

impl AppRuntimeError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate lifecycle violations (`AlreadyRunning`,
    /// `NotRunning`) or terminal lifecycle states (`ShuttingDown`)
    /// that no amount of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::AlreadyRunning | Self::NotRunning | Self::ShuttingDown
        )
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (module crash,
    /// composition ordering issue).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::ModuleFailed(_) | Self::CompositionFailed(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(AppRuntimeError::AlreadyRunning.is_fatal());
        assert!(AppRuntimeError::NotRunning.is_fatal());
        assert!(AppRuntimeError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!AppRuntimeError::ModuleFailed("auth").is_fatal());
        assert!(!AppRuntimeError::CompositionFailed("cycle").is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(AppRuntimeError::ModuleFailed("auth").is_retryable());
        assert!(AppRuntimeError::CompositionFailed("cycle").is_retryable());
        // Non-retryable examples.
        assert!(!AppRuntimeError::AlreadyRunning.is_retryable());
        assert!(!AppRuntimeError::NotRunning.is_retryable());
        assert!(!AppRuntimeError::ShuttingDown.is_retryable());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = AppRuntimeError::AlreadyRunning;
        assert!(format!("{e}").contains("already running"));

        let e = AppRuntimeError::NotRunning;
        assert!(format!("{e}").contains("not running"));

        let e = AppRuntimeError::ModuleFailed("auth");
        let s = format!("{e}");
        assert!(s.contains("module failed"));
        assert!(s.contains("auth"));

        let e = AppRuntimeError::CompositionFailed("cycle");
        let s = format!("{e}");
        assert!(s.contains("composition failed"));
        assert!(s.contains("cycle"));

        let e = AppRuntimeError::ShuttingDown;
        assert!(format!("{e}").contains("shutting down"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = AppRuntimeError::AlreadyRunning;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let fatal = AppRuntimeError::NotRunning;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let fatal = AppRuntimeError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let retry = AppRuntimeError::ModuleFailed("auth");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());

        let retry2 = AppRuntimeError::CompositionFailed("cycle");
        assert!(!retry2.is_fatal());
        assert!(retry2.is_retryable());
    }
}

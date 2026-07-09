//! [`MiddlewareError`] — the typed failure surface of the middleware
//! pipeline system.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`MiddlewareError::is_fatal`] / [`MiddlewareError::is_retryable`]
//! predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `DuplicateMiddleware`, `InvalidOrder`, `StageMismatch`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `Rejected`, `ChainAborted` | Back off + retry |
//! | Capacity | `NotFound` | Check registration |

use crate::id::MiddlewareId;
use thiserror::Error;

// ── MiddlewareError ─────────────────────────────────────────────

/// Typed failure surface for the middleware pipeline system.
///
/// Every fallible pipeline API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum MiddlewareError {
    /// Middleware rejected the request (short-circuit).
    #[error("middleware rejected: {0}")]
    Rejected(&'static str),

    /// Chain execution was aborted mid-chain.
    #[error("chain aborted: {0}")]
    ChainAborted(&'static str),

    /// A middleware with the given id is already registered.
    #[error("middleware {0:?} is already registered")]
    DuplicateMiddleware(MiddlewareId),

    /// Invalid order value provided.
    #[error("invalid order: {0}")]
    InvalidOrder(&'static str),

    /// Middleware stage mismatch in registration.
    #[error("stage mismatch: expected {expected}, got {got}")]
    StageMismatch {
        /// Expected stage
        expected: &'static str,
        /// Actual stage
        got: &'static str,
    },

    /// Middleware not found in chain.
    #[error("middleware {0:?} not found")]
    NotFound(MiddlewareId),

    /// System is shutting down; reject new operations.
    #[error("pipeline is shutting down")]
    ShuttingDown,
}

impl MiddlewareError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes
    /// or terminal lifecycle states (`ShuttingDown`) that no amount
    /// of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::DuplicateMiddleware(_)
                | Self::InvalidOrder(_)
                | Self::StageMismatch { .. }
                | Self::ShuttingDown
        )
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (rejection due to
    /// rate limiting, chain abort due to transient error).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Rejected(_) | Self::ChainAborted(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(MiddlewareError::DuplicateMiddleware(MiddlewareId::new(1)).is_fatal());
        assert!(MiddlewareError::InvalidOrder("bad").is_fatal());
        assert!(MiddlewareError::ShuttingDown.is_fatal());
        assert!(matches!(
            MiddlewareError::StageMismatch {
                expected: "auth",
                got: "response"
            },
            MiddlewareError::StageMismatch { .. }
        ));
        // Non-fatal examples.
        assert!(!MiddlewareError::Rejected("rate limit").is_fatal());
        assert!(!MiddlewareError::NotFound(MiddlewareId::new(1)).is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(MiddlewareError::Rejected("rate limit").is_retryable());
        assert!(MiddlewareError::ChainAborted("timeout").is_retryable());
        // Non-retryable examples.
        assert!(!MiddlewareError::DuplicateMiddleware(MiddlewareId::new(1)).is_retryable());
        assert!(!MiddlewareError::ShuttingDown.is_retryable());
        assert!(!MiddlewareError::NotFound(MiddlewareId::new(1)).is_retryable());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = MiddlewareError::Rejected("rate limit");
        let s = format!("{e}");
        assert!(s.contains("rejected"));
        assert!(s.contains("rate limit"));

        let e = MiddlewareError::NotFound(MiddlewareId::new(42));
        let s = format!("{e}");
        assert!(s.contains("not found"));
        assert!(s.contains("42"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = MiddlewareError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let retry = MiddlewareError::Rejected("temp");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());

        let neither = MiddlewareError::NotFound(MiddlewareId::new(1));
        assert!(!neither.is_fatal());
        assert!(!neither.is_retryable());
    }

    #[test]
    fn stage_mismatch_includes_details() {
        let e = MiddlewareError::StageMismatch {
            expected: "auth",
            got: "response",
        };
        let _ = format!("{e:?}"); // Silence unused format warning
        assert!(matches!(
            e,
            MiddlewareError::StageMismatch {
                expected: "auth",
                got: "response"
            }
        ));
    }
}

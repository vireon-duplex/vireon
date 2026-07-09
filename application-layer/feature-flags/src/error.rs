//! [`FlagError`] — typed failure surface for the feature flag system.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12 (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`FlagError::is_fatal`] / [`FlagError::is_retryable`] predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `InvalidValue`, `Frozen`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `EvalError` | Back off + retry |
//! | Recoverable | `NotFound` | Check flag exists or create it |

use crate::flag::FlagKey;

// ── FlagError ───────────────────────────────────────────────────

/// Typed failure surface for the feature flag system.
///
/// Every fallible flag API returns one of these variants so that
/// callers can branch on a small, exhaustive set of failure modes
/// instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum FlagError {
    /// A flag key was not found in the registry.
    #[error("flag not found: {0}")]
    NotFound(String),

    /// A flag value is invalid for its type.
    #[error("invalid flag value: {0}")]
    InvalidValue(String),

    /// A rule evaluation error occurred.
    #[error("rule evaluation error: {0}")]
    EvalError(&'static str),

    /// The flag is frozen and cannot be modified.
    #[error("flag is frozen: {0:?}")]
    Frozen(FlagKey),

    /// The flag system is shutting down; reject new operations.
    #[error("flag system is shutting down")]
    ShuttingDown,
}

impl FlagError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate terminal lifecycle states
    /// (`ShuttingDown`) or frozen flags that no amount of retrying
    /// will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::Frozen(_) | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (rule evaluation
    /// temporarily failing due to context issues).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::EvalError(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        let key = FlagKey::new("test").unwrap();
        assert!(FlagError::Frozen(key).is_fatal());
        assert!(FlagError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!FlagError::NotFound("missing".into()).is_fatal());
        assert!(!FlagError::InvalidValue("bad".into()).is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(FlagError::EvalError("temp").is_retryable());
        // Non-retryable examples.
        assert!(!FlagError::NotFound("x".into()).is_retryable());
        assert!(!FlagError::ShuttingDown.is_retryable());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = FlagError::NotFound("my_flag".into());
        let s = format!("{e}");
        assert!(s.contains("flag not found"));
        assert!(s.contains("my_flag"));

        let e = FlagError::EvalError("context");
        assert!(format!("{e}").contains("rule evaluation error"));
    }

    #[test]
    fn variants_are_distinct() {
        let key = FlagKey::new("test").unwrap();
        // Ensure predicates do not overlap on the same variant.
        let fatal = FlagError::Frozen(key);
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let retry = FlagError::EvalError("temp");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());

        let neither = FlagError::NotFound("x".into());
        assert!(!neither.is_fatal());
        assert!(!neither.is_retryable());
    }

    #[test]
    fn invalid_value_error() {
        let e = FlagError::InvalidValue("percentage > 100".into());
        assert!(format!("{e}").contains("invalid flag value"));
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }
}

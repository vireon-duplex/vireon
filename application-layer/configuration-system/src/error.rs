//! [`ConfigError`] — the typed failure surface of the configuration
//! system.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`ConfigError::is_fatal`] / [`ConfigError::is_retryable`]
//! predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `InvalidKey`, `Frozen`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `SourceUnavailable` | Back off + retry |
//! | Parse | `ParseError`, `TypeMismatch` | Fix input; don't retry |

use thiserror::Error;

// ── ConfigError ───────────────────────────────────────────────────

/// Typed failure surface for the configuration system.
///
/// Every fallible config API returns one of these variants so that
/// callers can branch on a small, exhaustive set of failure modes
/// instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ConfigError {
    /// A configuration field is out of range or contradictory.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// A configuration key is invalid (empty, too long, bad chars).
    #[error("invalid configuration key: {0}")]
    InvalidKey(String),

    /// The configuration key was not found in any layer.
    #[error("configuration key not found: {0}")]
    KeyNotFound(String),

    /// Failed to parse a value from its source.
    #[error("parse error: {0}")]
    ParseError(String),

    /// Type mismatch when accessing a value (e.g. reading bool as i64).
    #[error("type mismatch: {0}")]
    TypeMismatch(String),

    /// A configuration source is temporarily unavailable.
    #[error("source unavailable: {0}")]
    SourceUnavailable(&'static str),

    /// The configuration key is frozen and cannot be modified.
    #[error("configuration key is frozen: {0}")]
    Frozen(String),

    /// The configuration system is shutting down.
    #[error("configuration system is shutting down")]
    ShuttingDown,
}

impl ConfigError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`InvalidKey`,
    /// `TypeMismatch`) or terminal lifecycle states (`Frozen`,
    /// `ShuttingDown`) that no amount of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::InvalidConfig(_) | Self::InvalidKey(_) | Self::Frozen(_) | Self::ShuttingDown
        )
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (source temporarily
    /// unreachable).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::SourceUnavailable(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(ConfigError::InvalidConfig("bad".into()).is_fatal());
        assert!(ConfigError::InvalidKey("bad".into()).is_fatal());
        assert!(ConfigError::Frozen("key".into()).is_fatal());
        assert!(ConfigError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!ConfigError::ParseError("bad".into()).is_fatal());
        assert!(!ConfigError::TypeMismatch("bad".into()).is_fatal());
        // Retryable example (not fatal).
        assert!(!ConfigError::SourceUnavailable("network").is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(ConfigError::SourceUnavailable("network").is_retryable());
        // Non-retryable examples.
        assert!(!ConfigError::InvalidKey("bad".into()).is_retryable());
        assert!(!ConfigError::ParseError("bad".into()).is_retryable());
        assert!(!ConfigError::ShuttingDown.is_retryable());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = ConfigError::InvalidKey("empty key".into());
        let s = format!("{e}");
        assert!(s.contains("invalid configuration key"));
        assert!(s.contains("empty key"));

        let e = ConfigError::ShuttingDown;
        assert!(format!("{e}").contains("shutting down"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = ConfigError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let retry = ConfigError::SourceUnavailable("network");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
    }

    #[test]
    fn type_mismatch_is_not_fatal() {
        // Type mismatches are input errors, not system fatal errors.
        let e = ConfigError::TypeMismatch("expected bool".into());
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn parse_error_is_not_fatal() {
        // Parse errors are input errors, not system fatal errors.
        let e = ConfigError::ParseError("invalid number".into());
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }
}

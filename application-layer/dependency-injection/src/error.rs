//! [`DependencyError`] — the typed failure surface of the dependency
//! injection container.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`DependencyError::is_fatal`] / [`DependencyError::is_retryable`]
//! predicates:
//!
//! | Category | Example | Caller action |
//!----------|---------|---------------|
//! | Fatal | `CircularDependency`, `TypeMismatch`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `ResolutionFailed` | Back off + retry |
//! | Transient | `ServiceNotRegistered`, `AlreadyRegistered` | Fix configuration |

use crate::token::TokenId;
use thiserror::Error;

// ── DependencyError ─────────────────────────────────────────────

/// Typed failure surface for the dependency injection container.
///
/// Every fallible container API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum DependencyError {
    /// The requested service is not registered in the container.
    #[error("service not registered: token id {0}")]
    ServiceNotRegistered(TokenId),

    /// A circular dependency was detected in the dependency graph.
    #[error("circular dependency detected at token id {0}")]
    CircularDependency(TokenId),

    /// Type mismatch between requested and registered service.
    #[error("type mismatch: expected {0}")]
    TypeMismatch(&'static str),

    /// A service with this token is already registered.
    #[error("service already registered: token id {0}")]
    AlreadyRegistered(TokenId),

    /// Service resolution failed for the given reason.
    #[error("resolution failed: {0}")]
    ResolutionFailed(&'static str),

    /// Attempted to operate on a closed scope.
    #[error("scope is closed")]
    ScopeClosed,

    /// Container is shutting down; reject new operations.
    #[error("container is shutting down")]
    ShuttingDown,
}

impl DependencyError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`CircularDependency`,
    /// `TypeMismatch`) or terminal lifecycle states (`ShuttingDown`)
    /// that no amount of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::CircularDependency(_) | Self::TypeMismatch(_) | Self::ShuttingDown
        )
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (resolution failing
    /// due to temporary conditions).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::ResolutionFailed(_))
    }

    /// Whether the error reflects a configuration issue.
    ///
    /// Configuration errors signal that the container setup is
    /// incorrect and needs to be fixed before retrying.
    #[must_use]
    pub const fn is_configuration_error(&self) -> bool {
        matches!(
            self,
            Self::ServiceNotRegistered(_) | Self::AlreadyRegistered(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(DependencyError::CircularDependency(TokenId::new(1)).is_fatal());
        assert!(DependencyError::TypeMismatch("test").is_fatal());
        assert!(DependencyError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!DependencyError::ServiceNotRegistered(TokenId::new(1)).is_fatal());
        assert!(!DependencyError::ResolutionFailed("temp").is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(DependencyError::ResolutionFailed("temp").is_retryable());
        // Non-retryable examples.
        assert!(!DependencyError::CircularDependency(TokenId::new(1)).is_retryable());
        assert!(!DependencyError::ServiceNotRegistered(TokenId::new(1)).is_retryable());
        assert!(!DependencyError::ShuttingDown.is_retryable());
    }

    #[test]
    fn configuration_error_classification() {
        assert!(DependencyError::ServiceNotRegistered(TokenId::new(1)).is_configuration_error());
        assert!(DependencyError::AlreadyRegistered(TokenId::new(1)).is_configuration_error());
        // Non-configuration examples.
        assert!(!DependencyError::CircularDependency(TokenId::new(1)).is_configuration_error());
        assert!(!DependencyError::ResolutionFailed("temp").is_configuration_error());
        assert!(!DependencyError::ShuttingDown.is_configuration_error());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = DependencyError::ServiceNotRegistered(TokenId::new(42));
        let s = format!("{e}");
        assert!(s.contains("service not registered"));
        assert!(s.contains("42"));

        let e = DependencyError::CircularDependency(TokenId::new(7));
        assert!(format!("{e}").contains("circular dependency"));

        let e = DependencyError::TypeMismatch("expected Foo");
        assert!(format!("{e}").contains("type mismatch"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = DependencyError::CircularDependency(TokenId::new(1));
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());
        assert!(!fatal.is_configuration_error());

        let retry = DependencyError::ResolutionFailed("temp");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
        assert!(!retry.is_configuration_error());

        let config = DependencyError::ServiceNotRegistered(TokenId::new(1));
        assert!(!config.is_fatal());
        assert!(!config.is_retryable());
        assert!(config.is_configuration_error());
    }

    #[test]
    fn shutting_down_is_fatal() {
        let e = DependencyError::ShuttingDown;
        assert!(e.is_fatal());
        assert!(!e.is_retryable());
        assert!(!e.is_configuration_error());
    }

    #[test]
    fn scope_closed_is_neither_fatal_nor_retryable() {
        let e = DependencyError::ScopeClosed;
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
        assert!(!e.is_configuration_error());
    }
}

//! [`AuthError`] — the typed failure surface of the auth framework.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`AuthError::is_fatal`] / [`AuthError::is_retryable`] /
//! [`AuthError::is_authz`] predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `InvalidToken`, `InvalidCredential`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `AuthenticationFailed`, `RateLimited` | Back off + retry |
//! | AuthZ | `PermissionDenied`, `SessionRevoked` | Authorization failure |

use crate::principal::PrincipalId;
use thiserror::Error;

// ── AuthError ───────────────────────────────────────────────────

/// Typed failure surface for the auth framework.
///
/// Every fallible auth API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum AuthError {
    /// Authentication failed (bad password, invalid cert, etc.).
    #[error("authentication failed: {0}")]
    AuthenticationFailed(&'static str),

    /// Token is invalid (malformed, signature failed, etc.).
    #[error("invalid token: {0}")]
    InvalidToken(&'static str),

    /// Token has expired.
    #[error("token expired")]
    TokenExpired,

    /// Permission denied (authorization failure).
    #[error("permission denied: {0}")]
    PermissionDenied(&'static str),

    /// Principal not found in the system.
    #[error("unknown principal: {0}")]
    UnknownPrincipal(PrincipalId),

    /// Rate limit exceeded.
    #[error("rate limited")]
    RateLimited,

    /// Invalid credential (malformed, wrong type, etc.).
    #[error("invalid credential: {0}")]
    InvalidCredential(&'static str),

    /// Session has been revoked.
    #[error("session revoked")]
    SessionRevoked,

    /// Framework is shutting down; reject new operations.
    #[error("auth framework is shutting down")]
    ShuttingDown,
}

impl AuthError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`InvalidCredential`)
    /// or terminal lifecycle states (`ShuttingDown`) that no amount
    /// of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::InvalidToken(_) | Self::InvalidCredential(_) | Self::ShuttingDown
        )
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (rate limiting,
    /// temporary auth failure).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::AuthenticationFailed(_) | Self::RateLimited)
    }

    /// Whether the error reflects an authorization failure.
    ///
    /// Authorization errors signal that the principal is authenticated
    /// but lacks permission for the requested action.
    #[must_use]
    pub const fn is_authz(&self) -> bool {
        matches!(self, Self::PermissionDenied(_) | Self::SessionRevoked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(AuthError::InvalidToken("bad signature").is_fatal());
        assert!(AuthError::InvalidCredential("malformed").is_fatal());
        assert!(AuthError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!AuthError::AuthenticationFailed("bad password").is_fatal());
        assert!(!AuthError::PermissionDenied("access denied").is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(AuthError::AuthenticationFailed("bad password").is_retryable());
        assert!(AuthError::RateLimited.is_retryable());
        // Non-retryable examples.
        assert!(!AuthError::ShuttingDown.is_retryable());
        assert!(!AuthError::InvalidToken("bad").is_retryable());
    }

    #[test]
    fn authz_classification() {
        assert!(AuthError::PermissionDenied("access denied").is_authz());
        assert!(AuthError::SessionRevoked.is_authz());
        // Non-authz examples.
        assert!(!AuthError::ShuttingDown.is_authz());
        assert!(!AuthError::AuthenticationFailed("bad").is_authz());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = AuthError::AuthenticationFailed("bad password");
        let s = format!("{e}");
        assert!(s.contains("authentication failed"));
        assert!(s.contains("bad password"));

        let e = AuthError::ShuttingDown;
        assert!(format!("{e}").contains("shutting down"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = AuthError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());
        assert!(!fatal.is_authz());

        let retry = AuthError::AuthenticationFailed("bad");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
        assert!(!retry.is_authz());

        let authz = AuthError::PermissionDenied("denied");
        assert!(!authz.is_fatal());
        assert!(!authz.is_retryable());
        assert!(authz.is_authz());
    }
}

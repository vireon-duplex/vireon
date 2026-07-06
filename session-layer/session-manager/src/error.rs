//! [`SessionManagerError`] — unified error surface for the session
//! manager.
//!
//! See `DESIGN.md` Part 13 (failure handling) and Part 14 (security).

use thiserror::Error;

/// Unified session-manager error.
///
/// `#[non_exhaustive]` reserves the right to add new variants in
/// future phases without breaking match arms.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum SessionManagerError {
    /// Configuration validation failed.
    #[error("invalid session-manager configuration: {0}")]
    InvalidConfig(String),

    /// No session with the given id is known to this manager.
    #[error("session not found")]
    SessionNotFound(crate::id::SessionId),

    /// A session with the given id already exists.
    #[error("session already exists")]
    SessionExists(crate::id::SessionId),

    /// Session is closed / draining / destroyed.
    #[error("session is closed")]
    SessionClosed(crate::id::SessionId),

    /// Authentication failed (bad credentials, expired token,
    /// certificate rejected).
    #[error("authentication failed: {0}")]
    AuthenticationFailed(&'static str),

    /// Authorization denied (insufficient permissions, tenant
    /// isolation violation).
    #[error("authorization denied: {0}")]
    AuthorizationDenied(&'static str),

    /// Resource quota exceeded (too many streams, memory, bandwidth).
    #[error("quota exceeded: {0}")]
    QuotaExceeded(&'static str),

    /// The session manager has reached its maximum session count.
    #[error("session limit reached")]
    SessionLimitReached,

    /// Session has no bound connection (e.g. before handshake
    /// completes or after connection close).
    #[error("session has no bound connection")]
    ConnectionNotBound,

    /// Connection migration failed (path challenge failure, CID
    /// rotation conflict, reactor handoff error).
    #[error("migration failed: {0}")]
    MigrationFailed(&'static str),

    /// Slab / table / out-of-memory.
    #[error("resource exhausted: {0}")]
    ResourceExhausted(&'static str),

    /// Manager is shutting down; no new sessions.
    #[error("session-manager is shutting down")]
    ShuttingDown,
}

impl SessionManagerError {
    /// Fatal errors abort the connection or trigger a connection
    /// close. Security violations and resource exhaustion are
    /// always fatal.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::InvalidConfig(_)
                | Self::AuthenticationFailed(_)
                | Self::ResourceExhausted(_)
                | Self::ShuttingDown
        )
    }

    /// Retryable errors are logged and the caller continues.
    /// Examples: `SessionNotFound` (retry with create),
    /// `QuotaExceeded` (back off + retry).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        !self.is_fatal()
    }

    /// Whether the error is security-related (auth / authz).
    /// These should be audited and may trigger rate limiting.
    #[must_use]
    pub const fn is_security(&self) -> bool {
        matches!(
            self,
            Self::AuthenticationFailed(_) | Self::AuthorizationDenied(_)
        )
    }

    /// Whether the error is resource-related (quota / limit /
    /// exhaustion).
    #[must_use]
    pub const fn is_resource(&self) -> bool {
        matches!(
            self,
            Self::QuotaExceeded(_) | Self::SessionLimitReached | Self::ResourceExhausted(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::SessionId;

    #[test]
    fn classify_fatal() {
        assert!(SessionManagerError::ShuttingDown.is_fatal());
        assert!(SessionManagerError::AuthenticationFailed("bad token").is_fatal());
        assert!(SessionManagerError::ResourceExhausted("slab full").is_fatal());
    }

    #[test]
    fn classify_retryable() {
        let id = SessionId::nil();
        assert!(SessionManagerError::SessionNotFound(id).is_retryable());
        assert!(SessionManagerError::QuotaExceeded("streams").is_retryable());
        assert!(SessionManagerError::SessionLimitReached.is_retryable());
    }

    #[test]
    fn security_classification() {
        assert!(SessionManagerError::AuthenticationFailed("x").is_security());
        assert!(SessionManagerError::AuthorizationDenied("no perm").is_security());
        assert!(!SessionManagerError::SessionLimitReached.is_security());
    }

    #[test]
    fn resource_classification() {
        assert!(SessionManagerError::QuotaExceeded("mem").is_resource());
        assert!(SessionManagerError::SessionLimitReached.is_resource());
        assert!(!SessionManagerError::AuthenticationFailed("x").is_resource());
    }

    #[test]
    fn display_includes_context() {
        let e = SessionManagerError::AuthenticationFailed("expired token");
        let s = format!("{e}");
        assert!(s.contains("expired token"));
    }
}

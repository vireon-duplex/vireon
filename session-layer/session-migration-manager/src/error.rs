//! [`MigrationError`] — the typed failure surface of the session
//! migration manager.
//!
//! See `DESIGN.md` Part 9 (failure & rollback) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories mirroring the
//! [`MigrationError::is_fatal`] / [`MigrationError::is_retryable`] /
//! [`MigrationError::is_recoverable`] predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `InvalidConfig`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `SnapshotFailed`, `TransferFailed`, `RestoreFailed` | Back off + retry |
//! | Recoverable | `AlreadyInProgress`, `OrderingViolation` | Rollback + retry |

use session_manager::SessionId;
use session_registry::NodeId;
use thiserror::Error;

// ── MigrationError ──────────────────────────────────────────────

/// Typed failure surface for cross-node session migration.
///
/// Every fallible migration API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum MigrationError {
    /// A configuration field is out of range or contradictory.
    #[error("invalid migration configuration: {0}")]
    InvalidConfig(String),

    /// No migration record exists for the given session.
    #[error("migration not found for session {0}")]
    MigrationNotFound(SessionId),

    /// A migration is already running for this session.
    #[error("migration already in progress for session {0}")]
    AlreadyInProgress(SessionId),

    /// Snapshot capture failed on the source node.
    #[error("snapshot capture failed: {0}")]
    SnapshotFailed(&'static str),

    /// State transfer from source to target failed.
    #[error("state transfer failed: {0}")]
    TransferFailed(&'static str),

    /// State restore on the target node failed.
    #[error("state restore failed: {0}")]
    RestoreFailed(&'static str),

    /// The migration deadline was exceeded.
    #[error("migration deadline exceeded for session {0}")]
    DeadlineExceeded(SessionId),

    /// The source node is not available for migration.
    #[error("source node unavailable: {0:?}")]
    SourceUnavailable(NodeId),

    /// The target node is not available for migration.
    #[error("target node unavailable: {0:?}")]
    TargetUnavailable(NodeId),

    /// An ordering violation occurred during migration.
    #[error("ordering violation during migration: {0}")]
    OrderingViolation(&'static str),

    /// Rollback of a failed migration could not complete.
    #[error("rollback failed: {0}")]
    RollbackFailed(&'static str),

    /// The migration service is shutting down; reject new operations.
    #[error("migration service is shutting down")]
    ShuttingDown,
}

impl MigrationError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`InvalidConfig`)
    /// or terminal lifecycle states (`ShuttingDown`) that no amount
    /// of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::InvalidConfig(_) | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient: a snapshot capture
    /// raced with a concurrent write, a network blip interrupted
    /// transfer, or the target node was momentarily unavailable.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::SnapshotFailed(_)
                | Self::TransferFailed(_)
                | Self::RestoreFailed(_)
                | Self::SourceUnavailable(_)
                | Self::TargetUnavailable(_)
                | Self::DeadlineExceeded(_)
        )
    }

    /// Whether the error is recoverable — rollback + retry.
    ///
    /// Recoverable errors indicate that the migration itself is in
    /// a bad state but the session is still viable; the caller should
    /// abort, roll back, and try again.
    #[must_use]
    pub const fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::AlreadyInProgress(_)
                | Self::OrderingViolation(_)
                | Self::RollbackFailed(_)
                | Self::MigrationNotFound(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(MigrationError::InvalidConfig("bad".into()).is_fatal());
        assert!(MigrationError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!MigrationError::SnapshotFailed("race").is_fatal());
        assert!(!MigrationError::TransferFailed("timeout").is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(MigrationError::SnapshotFailed("race").is_retryable());
        assert!(MigrationError::TransferFailed("net").is_retryable());
        assert!(MigrationError::RestoreFailed("oom").is_retryable());
        assert!(MigrationError::SourceUnavailable(NodeId::new(1)).is_retryable());
        assert!(MigrationError::TargetUnavailable(NodeId::new(2)).is_retryable());
        assert!(MigrationError::DeadlineExceeded(SessionId::nil()).is_retryable());
        // Non-retryable examples.
        assert!(!MigrationError::ShuttingDown.is_retryable());
        assert!(!MigrationError::InvalidConfig("bad".into()).is_retryable());
    }

    #[test]
    fn recoverable_classification() {
        assert!(MigrationError::AlreadyInProgress(SessionId::nil()).is_recoverable());
        assert!(MigrationError::OrderingViolation("seq").is_recoverable());
        assert!(MigrationError::RollbackFailed("cleanup").is_recoverable());
        assert!(MigrationError::MigrationNotFound(SessionId::nil()).is_recoverable());
        // Non-recoverable examples.
        assert!(!MigrationError::ShuttingDown.is_recoverable());
        assert!(!MigrationError::SnapshotFailed("race").is_recoverable());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = MigrationError::SourceUnavailable(NodeId::new(42));
        let s = format!("{e}");
        assert!(s.contains("source node unavailable"));

        let e = MigrationError::DeadlineExceeded(SessionId::from_pair(1, 2));
        assert!(format!("{e}").contains("deadline exceeded"));

        let e = MigrationError::ShuttingDown;
        assert!(format!("{e}").contains("shutting down"));
    }

    #[test]
    fn variants_are_distinct() {
        // Fatal.
        let fatal = MigrationError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());
        assert!(!fatal.is_recoverable());

        // Retryable.
        let retry = MigrationError::TransferFailed("timeout");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
        assert!(!retry.is_recoverable());

        // Recoverable.
        let rec = MigrationError::AlreadyInProgress(SessionId::nil());
        assert!(!rec.is_fatal());
        assert!(!rec.is_retryable());
        assert!(rec.is_recoverable());
    }
}

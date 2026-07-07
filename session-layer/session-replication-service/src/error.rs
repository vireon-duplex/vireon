//! [`ReplicationError`] — the typed failure surface of the
//! replication service.
//!
//! See `DESIGN.md` Part 9 (conflict detection) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`ReplicationError::is_fatal`] / [`ReplicationError::is_retryable`]
//! / [`ReplicationError::is_consensus`] predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `InvalidConfig`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `ReplicaUnreachable`, `QuorumNotReached` | Back off + retry |
//! | Consensus | `ConsensusTimeout`, `ConflictDetected` | Retry or resolve |

use crate::replica::ReplicaId;
use thiserror::Error;

// ── ReplicationError ────────────────────────────────────────────

/// Typed failure surface for the session replication service.
///
/// Every fallible replication API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ReplicationError {
    /// A configuration field is out of range or contradictory.
    #[error("invalid replication configuration: {0}")]
    InvalidConfig(String),

    /// The named replica is not registered with the service.
    #[error("replica not found: {0:?}")]
    ReplicaNotFound(ReplicaId),

    /// The required quorum of acknowledgements was not reached.
    #[error("quorum not reached: {0} of {1} replicas")]
    QuorumNotReached(usize, usize),

    /// Consensus round timed out without a decision.
    #[error("consensus timeout")]
    ConsensusTimeout,

    /// Replication lag exceeded the configured threshold.
    #[error("replication lag exceeds threshold: {0} ns")]
    LagExceeded(u64),

    /// A conflicting write was detected during replication.
    #[error("conflict detected: {0}")]
    ConflictDetected(&'static str),

    /// Conflict resolution algorithm failed to converge.
    #[error("conflict resolution failed: {0}")]
    ConflictResolutionFailed(&'static str),

    /// The requested replication strategy is not available.
    #[error("strategy unavailable: {0}")]
    StrategyUnavailable(&'static str),

    /// The target replica is not reachable.
    #[error("replica unreachable: {0:?}")]
    ReplicaUnreachable(ReplicaId),

    /// Too many conflicts accumulated; manual intervention needed.
    #[error("too many conflicts: {0}")]
    TooManyConflicts(u64),

    /// A write was rejected (e.g. by a fence or leadership check).
    #[error("write rejected: {0}")]
    WriteRejected(&'static str),

    /// The replication service is shutting down; reject new ops.
    #[error("replication service is shutting down")]
    ShuttingDown,
}

impl ReplicationError {
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
    /// Retryable errors are typically transient: a replica is
    /// temporarily unreachable, quorum isn't met, or lag exceeds
    /// the threshold momentarily.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::ReplicaUnreachable(_)
                | Self::QuorumNotReached(_, _)
                | Self::LagExceeded(_)
                | Self::ConsensusTimeout
                | Self::ReplicaNotFound(_)
        )
    }

    /// Whether the error is a consensus-related failure.
    ///
    /// Consensus errors relate to the agreement protocol itself:
    /// timeouts, conflicts, or write rejections.
    #[must_use]
    pub const fn is_consensus(&self) -> bool {
        matches!(
            self,
            Self::ConsensusTimeout
                | Self::ConflictDetected(_)
                | Self::ConflictResolutionFailed(_)
                | Self::WriteRejected(_)
                | Self::QuorumNotReached(_, _)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::ReplicaId;
    use session_registry::node::NodeId;

    #[test]
    fn fatal_classification() {
        assert!(ReplicationError::InvalidConfig("bad".into()).is_fatal());
        assert!(ReplicationError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!ReplicationError::ConsensusTimeout.is_fatal());
        assert!(!ReplicationError::TooManyConflicts(10).is_fatal());
    }

    #[test]
    fn retryable_classification() {
        let rid = ReplicaId::from_node(NodeId::new(1));
        assert!(ReplicationError::ReplicaUnreachable(rid).is_retryable());
        assert!(ReplicationError::QuorumNotReached(2, 3).is_retryable());
        assert!(ReplicationError::LagExceeded(5_000_000).is_retryable());
        assert!(ReplicationError::ConsensusTimeout.is_retryable());
        assert!(ReplicationError::ReplicaNotFound(rid).is_retryable());
        // Non-retryable.
        assert!(!ReplicationError::InvalidConfig("bad".into()).is_retryable());
        assert!(!ReplicationError::ShuttingDown.is_retryable());
        assert!(!ReplicationError::WriteRejected("fenced").is_retryable());
    }

    #[test]
    fn consensus_classification() {
        assert!(ReplicationError::ConsensusTimeout.is_consensus());
        assert!(ReplicationError::ConflictDetected("concurrent").is_consensus());
        assert!(ReplicationError::ConflictResolutionFailed("no converge").is_consensus());
        assert!(ReplicationError::WriteRejected("not leader").is_consensus());
        assert!(ReplicationError::QuorumNotReached(1, 3).is_consensus());
        // Non-consensus.
        assert!(!ReplicationError::InvalidConfig("bad".into()).is_consensus());
        assert!(!ReplicationError::ShuttingDown.is_consensus());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = ReplicationError::QuorumNotReached(2, 5);
        let s = format!("{e}");
        assert!(s.contains("quorum not reached"));
        assert!(s.contains("2 of 5"));

        let e = ReplicationError::LagExceeded(9_999_999);
        let s = format!("{e}");
        assert!(s.contains("9999999 ns"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates classify variants correctly.
        let fatal = ReplicationError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());
        assert!(!fatal.is_consensus());

        let retry = ReplicationError::ConsensusTimeout;
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
        assert!(retry.is_consensus());

        let config = ReplicationError::InvalidConfig("bad".into());
        assert!(config.is_fatal());
        assert!(!config.is_retryable());
        assert!(!config.is_consensus());
    }
}

//! [`ReplicationEvent`] — domain events emitted by the replication
//! service.
//!
//! See `DESIGN.md` Part 12 (observability).
//!
//! # Role
//!
//! The replication service publishes events for every meaningful
//! state transition: replica join/leave, write replication success
//! or failure, conflict detection/resolution, leader changes, and
//! lag alerts. Subscribers (router, migration manager, metrics
//! pipeline) react to these events without polling.
//!
//! # Phase 0
//!
//! Type only — no event bus. Phase 1+ adds a broadcast channel.

use crate::replica::ReplicaId;

/// A domain event emitted by the replication service.
///
/// Events are the **only** push-based notification channel the
/// replication service offers.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum ReplicationEvent {
    /// A replica joined the replication group.
    ReplicaJoined {
        /// The replica that joined.
        replica: ReplicaId,
    },
    /// A replica left the replication group.
    ReplicaLeft {
        /// The replica that left.
        replica: ReplicaId,
    },
    /// A write was successfully replicated to all required replicas.
    WriteReplicated {
        /// The key that was replicated.
        key: String,
        /// Number of replicas that acknowledged.
        ack_count: u8,
    },
    /// A write failed to replicate.
    WriteFailed {
        /// The key that failed.
        key: String,
        /// The error reason (static string).
        reason: &'static str,
    },
    /// A conflict was detected.
    ConflictDetected {
        /// The key with the conflict.
        key: String,
    },
    /// A conflict was resolved.
    ConflictResolved {
        /// The key whose conflict was resolved.
        key: String,
    },
    /// The Raft leader changed.
    LeaderChanged {
        /// The new leader's replica id.
        leader: ReplicaId,
        /// The new term.
        term: u64,
    },
    /// Replication lag exceeded the configured threshold.
    LagExceeded {
        /// Current lag in nanoseconds.
        lag_ns: u64,
        /// Threshold in nanoseconds.
        threshold_ns: u64,
    },
}

impl ReplicationEvent {
    /// Human-readable category name for the event.
    #[must_use]
    pub const fn category(&self) -> &'static str {
        match self {
            Self::ReplicaJoined { .. } | Self::ReplicaLeft { .. } => "membership",
            Self::WriteReplicated { .. } | Self::WriteFailed { .. } => "write",
            Self::ConflictDetected { .. } | Self::ConflictResolved { .. } => "conflict",
            Self::LeaderChanged { .. } => "consensus",
            Self::LagExceeded { .. } => "health",
        }
    }

    /// Whether the event indicates a membership change.
    #[must_use]
    pub const fn is_membership_change(&self) -> bool {
        matches!(self, Self::ReplicaJoined { .. } | Self::ReplicaLeft { .. })
    }

    /// Whether the event is an error or failure condition.
    #[must_use]
    pub const fn is_failure(&self) -> bool {
        matches!(
            self,
            Self::WriteFailed { .. } | Self::ConflictDetected { .. } | Self::LagExceeded { .. }
        )
    }

    /// Whether the event is informational (no action required).
    #[must_use]
    pub const fn is_informational(&self) -> bool {
        matches!(
            self,
            Self::ReplicaJoined { .. }
                | Self::WriteReplicated { .. }
                | Self::ConflictResolved { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use session_registry::node::NodeId;

    fn sample_replica() -> ReplicaId {
        ReplicaId::from_node(NodeId::new(1))
    }

    #[test]
    fn category_membership() {
        let e = ReplicationEvent::ReplicaJoined {
            replica: sample_replica(),
        };
        assert_eq!(e.category(), "membership");
    }

    #[test]
    fn category_write() {
        let e = ReplicationEvent::WriteReplicated {
            key: "k1".into(),
            ack_count: 3,
        };
        assert_eq!(e.category(), "write");
    }

    #[test]
    fn category_conflict() {
        let e = ReplicationEvent::ConflictDetected { key: "k1".into() };
        assert_eq!(e.category(), "conflict");
    }

    #[test]
    fn category_consensus() {
        let e = ReplicationEvent::LeaderChanged {
            leader: sample_replica(),
            term: 1,
        };
        assert_eq!(e.category(), "consensus");
    }

    #[test]
    fn is_failure_classification() {
        let fail = ReplicationEvent::WriteFailed {
            key: "k1".into(),
            reason: "timeout",
        };
        assert!(fail.is_failure());

        let ok = ReplicationEvent::WriteReplicated {
            key: "k1".into(),
            ack_count: 3,
        };
        assert!(!ok.is_failure());
    }

    #[test]
    fn is_informational_classification() {
        let joined = ReplicationEvent::ReplicaJoined {
            replica: sample_replica(),
        };
        assert!(joined.is_informational());

        let failed = ReplicationEvent::WriteFailed {
            key: "k1".into(),
            reason: "timeout",
        };
        assert!(!failed.is_informational());
    }

    #[test]
    fn event_can_be_cloned() {
        let e = ReplicationEvent::LagExceeded {
            lag_ns: 1_000_000,
            threshold_ns: 500_000,
        };
        let cloned = e.clone();
        let _ = format!("{cloned:?}");
    }

    #[test]
    fn lag_exceeded_is_failure_and_not_informational() {
        let e = ReplicationEvent::LagExceeded {
            lag_ns: 6_000_000_000,
            threshold_ns: 5_000_000_000,
        };
        assert!(e.is_failure());
        assert!(!e.is_informational());
    }
}

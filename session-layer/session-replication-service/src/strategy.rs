//! [`ReplicationStrategy`], [`ConsistencyLevel`] — strategy and
//! consistency primitives for the replication service.
//!
//! See `DESIGN.md` Part 2 (strategy comparison) and Part 4
//! (consistency levels).
//!
//! # Strategies
//!
//! Vireon supports four replication strategies, each suited to a
//! different point on the CAP-latency trade-off curve:
//!
//! | Strategy | Latency | Consistency | Availability |
//! |----------|---------|-------------|--------------|
//! | `Gossip` | High | Eventual | Maximum |
//! | `Raft` | Medium | Strong | Medium |
//! | `Crdt` | Low | Eventual | High |
//! | `Sync` | Low | Strong | Low |

// ── ReplicationStrategy ─────────────────────────────────────────

/// The replication algorithm used to propagate metadata changes.
///
/// Each strategy represents a different point on the CAP trade-off
/// curve. The default is `Raft` because it provides the strongest
/// consistency guarantees for session metadata.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ReplicationStrategy {
    /// Infection-style gossip protocol (SWIM-style dissemination).
    /// Highest availability, eventual consistency.
    Gossip,
    /// Raft consensus with leader-based log replication.
    /// Strong consistency, medium availability.
    #[default]
    Raft,
    /// Conflict-free Replicated Data Types (CRDTs).
    /// High availability, eventual consistency, no conflicts.
    Crdt,
    /// Synchronous replication to all replicas before ack.
    /// Strongest consistency, lowest availability.
    Sync,
}

impl ReplicationStrategy {
    /// Whether this strategy provides strong consistency.
    ///
    /// Strong consistency means reads always return the latest
    /// committed write (linearizability). Only `Raft` and `Sync`
    /// qualify.
    #[must_use]
    pub const fn is_strong_consistency(self) -> bool {
        matches!(self, Self::Raft | Self::Sync)
    }

    /// Whether this strategy provides eventual consistency.
    ///
    /// Eventual consistency means replicas converge to the same
    /// state if no new updates arrive, but reads may be stale
    /// temporarily. `Gossip` and `Crdt` qualify.
    #[must_use]
    pub const fn is_eventual_consistency(self) -> bool {
        matches!(self, Self::Gossip | Self::Crdt)
    }

    /// Whether this strategy requires a quorum to operate.
    ///
    /// `Raft` requires a majority quorum for writes and leader
    /// election. `Sync` requires all replicas (quorum = all).
    /// `Gossip` and `Crdt` have no quorum requirement.
    #[must_use]
    pub const fn requires_quorum(self) -> bool {
        matches!(self, Self::Raft | Self::Sync)
    }

    /// Human-readable name for metrics labels and configuration.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gossip => "gossip",
            Self::Raft => "raft",
            Self::Crdt => "crdt",
            Self::Sync => "sync",
        }
    }
}

// ── ConsistencyLevel ────────────────────────────────────────────

/// The consistency level required for a read or write operation.
///
/// Controls how many replicas must acknowledge an operation before
/// it is considered successful.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ConsistencyLevel {
    /// Only one replica needs to acknowledge. Fastest, but may
    /// return stale data.
    One,
    /// A quorum (majority) of replicas must acknowledge. The default
    /// for session metadata replication.
    #[default]
    Quorum,
    /// All replicas must acknowledge. Slowest, but provides the
    /// strongest consistency guarantee.
    All,
    /// A quorum within the local region/distance. Balances latency
    /// and consistency for multi-region deployments.
    LocalQuorum,
}

impl ConsistencyLevel {
    /// Minimum number of acknowledgements required for this
    /// consistency level given `replica_count` total replicas.
    ///
    /// - `One` → 1
    /// - `Quorum` → `(n / 2) + 1`
    /// - `All` → `n`
    /// - `LocalQuorum` → `(n / 2) + 1` (Phase 0 approximation)
    #[must_use]
    pub const fn min_acks(self, replica_count: u8) -> u8 {
        match self {
            Self::One => {
                if replica_count == 0 {
                    0
                } else {
                    1
                }
            }
            Self::Quorum | Self::LocalQuorum => {
                if replica_count == 0 {
                    0
                } else {
                    (replica_count / 2) + 1
                }
            }
            Self::All => replica_count,
        }
    }

    /// Whether reads at this consistency level may return stale
    /// (not-up-to-date) data.
    ///
    /// - `One` → `true` (may read from a lagging replica)
    /// - `Quorum`, `All`, `LocalQuorum` → `false`
    #[must_use]
    pub const fn allows_stale_reads(self) -> bool {
        matches!(self, Self::One)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── ReplicationStrategy tests ───────────────────────────────

    #[test]
    fn default_strategy_is_raft() {
        assert_eq!(ReplicationStrategy::default(), ReplicationStrategy::Raft);
    }

    #[test]
    fn strong_vs_eventual_consistency() {
        assert!(ReplicationStrategy::Raft.is_strong_consistency());
        assert!(ReplicationStrategy::Sync.is_strong_consistency());
        assert!(!ReplicationStrategy::Gossip.is_strong_consistency());
        assert!(!ReplicationStrategy::Crdt.is_strong_consistency());

        assert!(ReplicationStrategy::Gossip.is_eventual_consistency());
        assert!(ReplicationStrategy::Crdt.is_eventual_consistency());
        assert!(!ReplicationStrategy::Raft.is_eventual_consistency());
        assert!(!ReplicationStrategy::Sync.is_eventual_consistency());
    }

    #[test]
    fn requires_quorum_classification() {
        assert!(ReplicationStrategy::Raft.requires_quorum());
        assert!(ReplicationStrategy::Sync.requires_quorum());
        assert!(!ReplicationStrategy::Gossip.requires_quorum());
        assert!(!ReplicationStrategy::Crdt.requires_quorum());
    }

    #[test]
    fn strategy_as_str() {
        assert_eq!(ReplicationStrategy::Gossip.as_str(), "gossip");
        assert_eq!(ReplicationStrategy::Raft.as_str(), "raft");
        assert_eq!(ReplicationStrategy::Crdt.as_str(), "crdt");
        assert_eq!(ReplicationStrategy::Sync.as_str(), "sync");
    }

    // ── ConsistencyLevel tests ──────────────────────────────────

    #[test]
    fn default_consistency_is_quorum() {
        assert_eq!(ConsistencyLevel::default(), ConsistencyLevel::Quorum);
    }

    #[test]
    fn min_acks_for_one() {
        assert_eq!(ConsistencyLevel::One.min_acks(1), 1);
        assert_eq!(ConsistencyLevel::One.min_acks(3), 1);
        assert_eq!(ConsistencyLevel::One.min_acks(5), 1);
        assert_eq!(ConsistencyLevel::One.min_acks(0), 0);
    }

    #[test]
    fn min_acks_for_quorum() {
        // n=1 → 1, n=2 → 2, n=3 → 2, n=4 → 3, n=5 → 3
        assert_eq!(ConsistencyLevel::Quorum.min_acks(1), 1);
        assert_eq!(ConsistencyLevel::Quorum.min_acks(2), 2);
        assert_eq!(ConsistencyLevel::Quorum.min_acks(3), 2);
        assert_eq!(ConsistencyLevel::Quorum.min_acks(4), 3);
        assert_eq!(ConsistencyLevel::Quorum.min_acks(5), 3);
        assert_eq!(ConsistencyLevel::Quorum.min_acks(0), 0);
    }

    #[test]
    fn min_acks_for_all() {
        assert_eq!(ConsistencyLevel::All.min_acks(1), 1);
        assert_eq!(ConsistencyLevel::All.min_acks(3), 3);
        assert_eq!(ConsistencyLevel::All.min_acks(5), 5);
        assert_eq!(ConsistencyLevel::All.min_acks(0), 0);
    }

    #[test]
    fn min_acks_for_local_quorum() {
        // Phase 0: LocalQuorum uses same formula as Quorum.
        assert_eq!(
            ConsistencyLevel::LocalQuorum.min_acks(3),
            ConsistencyLevel::Quorum.min_acks(3)
        );
        assert_eq!(
            ConsistencyLevel::LocalQuorum.min_acks(5),
            ConsistencyLevel::Quorum.min_acks(5)
        );
    }

    #[test]
    fn allows_stale_reads_classification() {
        assert!(ConsistencyLevel::One.allows_stale_reads());
        assert!(!ConsistencyLevel::Quorum.allows_stale_reads());
        assert!(!ConsistencyLevel::All.allows_stale_reads());
        assert!(!ConsistencyLevel::LocalQuorum.allows_stale_reads());
    }
}

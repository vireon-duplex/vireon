//! [`ReplicaLiveness`], [`ReplicaHealth`] — replica liveness tracking
//! for the replication service.
//!
//! See `DESIGN.md` Part 9 (replica liveness & failure detection).
//!
//! # Liveness Model
//!
//! Replica liveness follows a three-state model inspired by SWIM:
//!
//! ```text
//!   Alive ──► Suspect ──► Dead
//!    ▲                     │
//!    └───── (rejoin) ──────┘
//! ```
//!
//! | State | Reads | Writes | Replication |
//! |-------|-------|--------|-------------|
//! | `Alive` | Yes | Yes | Active |
//! | `Suspect` | Yes (stale ok) | No | Catching up |
//! | `Dead` | No | No | Stopped |

use crate::replica::ReplicaId;

// ── ReplicaLiveness ─────────────────────────────────────────────

/// Three-state replica liveness classification.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ReplicaLiveness {
    /// Replica is reachable and accepting operations normally.
    #[default]
    Alive,
    /// Replica missed heartbeats; under suspicion. May recover or
    /// transition to `Dead`.
    Suspect,
    /// Replica is confirmed dead; not accepting any operations.
    Dead,
}

impl ReplicaLiveness {
    /// Whether the replica is considered alive for operational
    /// purposes.
    ///
    /// Only `Alive` returns `true`; `Suspect` and `Dead` return
    /// `false`.
    #[must_use]
    pub const fn is_alive(self) -> bool {
        matches!(self, Self::Alive)
    }

    /// Whether the replica can serve read operations.
    ///
    /// `Alive` and `Suspect` can serve reads (possibly stale);
    /// `Dead` cannot.
    #[must_use]
    pub const fn can_serve_reads(self) -> bool {
        matches!(self, Self::Alive | Self::Suspect)
    }

    /// Whether the replica can serve write operations.
    ///
    /// Only `Alive` can serve writes; `Suspect` and `Dead` cannot.
    #[must_use]
    pub const fn can_serve_writes(self) -> bool {
        matches!(self, Self::Alive)
    }

    /// Human-readable name for metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Alive => "alive",
            Self::Suspect => "suspect",
            Self::Dead => "dead",
        }
    }
}

// ── ReplicaHealth ───────────────────────────────────────────────

/// Point-in-time health snapshot for a single replica.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicaHealth {
    /// The replica identifier.
    pub id: ReplicaId,
    /// Current liveness state.
    pub liveness: ReplicaLiveness,
    /// Replication lag in nanoseconds (0 if fully caught up).
    pub replication_lag_ns: u64,
    /// Nanoseconds since cluster epoch when the replica was last
    /// contacted successfully.
    pub last_seen_ns: u64,
}

impl ReplicaHealth {
    /// Construct a new health snapshot for a replica.
    #[must_use]
    pub const fn new(id: ReplicaId) -> Self {
        Self {
            id,
            liveness: ReplicaLiveness::Alive,
            replication_lag_ns: 0,
            last_seen_ns: 0,
        }
    }

    /// Whether this replica is healthy enough to serve reads.
    #[must_use]
    pub const fn can_serve_reads(&self) -> bool {
        self.liveness.can_serve_reads()
    }

    /// Whether this replica is healthy enough to serve writes.
    #[must_use]
    pub const fn can_serve_writes(&self) -> bool {
        self.liveness.can_serve_writes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use session_registry::node::NodeId;

    #[test]
    fn liveness_default_is_alive() {
        assert_eq!(ReplicaLiveness::default(), ReplicaLiveness::Alive);
    }

    #[test]
    fn liveness_is_alive() {
        assert!(ReplicaLiveness::Alive.is_alive());
        assert!(!ReplicaLiveness::Suspect.is_alive());
        assert!(!ReplicaLiveness::Dead.is_alive());
    }

    #[test]
    fn liveness_can_serve_reads() {
        assert!(ReplicaLiveness::Alive.can_serve_reads());
        assert!(ReplicaLiveness::Suspect.can_serve_reads());
        assert!(!ReplicaLiveness::Dead.can_serve_reads());
    }

    #[test]
    fn liveness_can_serve_writes() {
        assert!(ReplicaLiveness::Alive.can_serve_writes());
        assert!(!ReplicaLiveness::Suspect.can_serve_writes());
        assert!(!ReplicaLiveness::Dead.can_serve_writes());
    }

    #[test]
    fn liveness_as_str() {
        assert_eq!(ReplicaLiveness::Alive.as_str(), "alive");
        assert_eq!(ReplicaLiveness::Suspect.as_str(), "suspect");
        assert_eq!(ReplicaLiveness::Dead.as_str(), "dead");
    }

    #[test]
    fn replica_health_new_defaults_to_alive() {
        let id = ReplicaId::from_node(NodeId::new(1));
        let h = ReplicaHealth::new(id);
        assert_eq!(h.liveness, ReplicaLiveness::Alive);
        assert_eq!(h.replication_lag_ns, 0);
        assert!(h.can_serve_reads());
        assert!(h.can_serve_writes());
    }
}

//! [`ReplicaId`], [`ReplicaInfo`] — replica identity and topology
//! descriptors.
//!
//! See `DESIGN.md` Part 3 (replication topology).
//!
//! # ReplicaId
//!
//! `ReplicaId` is a thin newtype around `NodeId`. It exists to
//! distinguish "the replica of session metadata running on this
//! node" from "the node itself" at the type level. The wrapping is
//! zero-cost (same `u32` underneath).

use session_registry::node::{NodeId, Region, Zone};

// ── ReplicaId ───────────────────────────────────────────────────

/// Identifier for a replica within the replication service.
///
/// Wraps a [`NodeId`] to distinguish replica identity from node
/// identity at the type level. Zero-cost newtype.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ReplicaId(NodeId);

impl ReplicaId {
    /// Construct from a [`NodeId`].
    #[must_use]
    pub const fn from_node(node: NodeId) -> Self {
        Self(node)
    }

    /// Convert back to the underlying [`NodeId`].
    #[must_use]
    pub const fn to_node(self) -> NodeId {
        self.0
    }

    /// Whether this is the nil sentinel (derived from `NodeId::NIL`).
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.0.raw() == 0
    }

    /// The raw `u32` value of the underlying `NodeId`.
    #[must_use]
    pub const fn raw(self) -> u32 {
        self.0.raw()
    }
}

impl From<NodeId> for ReplicaId {
    fn from(node: NodeId) -> Self {
        Self::from_node(node)
    }
}

impl From<ReplicaId> for NodeId {
    fn from(replica: ReplicaId) -> Self {
        replica.to_node()
    }
}

impl std::fmt::Display for ReplicaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "replica#{}", self.0.raw())
    }
}

impl Default for ReplicaId {
    fn default() -> Self {
        Self(NodeId::NIL)
    }
}

// ── ReplicaInfo ─────────────────────────────────────────────────

/// Static + topology info about a replica.
#[derive(Clone, Debug)]
pub struct ReplicaInfo {
    /// The replica identifier.
    pub id: ReplicaId,
    /// Geographic region where this replica resides.
    pub region: Region,
    /// Availability zone within the region.
    pub zone: Zone,
    /// Replication priority (lower = higher priority).
    pub priority: u8,
    /// Whether this replica participates as a voter in consensus
    /// (Raft). Non-voters receive data but don't vote.
    pub is_voter: bool,
}

impl ReplicaInfo {
    /// Construct a new replica info with default values.
    #[must_use]
    pub fn new(id: ReplicaId, region: Region, zone: Zone) -> Self {
        Self {
            id,
            region,
            zone,
            priority: 128,
            is_voter: true,
        }
    }

    /// Builder-style: set the priority.
    #[must_use]
    pub const fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Builder-style: set voter status.
    #[must_use]
    pub const fn with_voter(mut self, is_voter: bool) -> Self {
        self.is_voter = is_voter;
        self
    }

    /// Whether this replica is in the same region as `other`.
    #[must_use]
    pub fn is_same_region(&self, other: &Self) -> bool {
        self.region.raw() == other.region.raw()
    }

    /// Whether this replica is in the same zone as `other`.
    #[must_use]
    pub fn is_same_zone(&self, other: &Self) -> bool {
        self.is_same_region(other) && self.zone.raw() == other.zone.raw()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_id_from_node_roundtrips() {
        let node = NodeId::new(42);
        let replica = ReplicaId::from_node(node);
        assert_eq!(replica.to_node().raw(), 42);
        assert_eq!(replica.raw(), 42);
    }

    #[test]
    fn replica_id_is_nil() {
        assert!(ReplicaId::from_node(NodeId::NIL).is_nil());
        assert!(ReplicaId::default().is_nil());
        assert!(!ReplicaId::from_node(NodeId::new(1)).is_nil());
    }

    #[test]
    fn replica_id_from_nodeid_trait() {
        let node = NodeId::new(7);
        let replica: ReplicaId = node.into();
        assert_eq!(replica.raw(), 7);
        let back: NodeId = replica.into();
        assert_eq!(back.raw(), 7);
    }

    #[test]
    fn replica_id_display() {
        let replica = ReplicaId::from_node(NodeId::new(99));
        let s = format!("{replica}");
        assert!(s.contains("replica#99"));
    }

    #[test]
    fn replica_id_ordering() {
        let a = ReplicaId::from_node(NodeId::new(1));
        let b = ReplicaId::from_node(NodeId::new(2));
        assert!(a < b);
    }

    #[test]
    fn replica_info_builder_and_topology() {
        let r1 = ReplicaInfo::new(
            ReplicaId::from_node(NodeId::new(1)),
            Region::new(1),
            Zone::new(1),
        )
        .with_priority(10)
        .with_voter(false);

        let r2 = ReplicaInfo::new(
            ReplicaId::from_node(NodeId::new(2)),
            Region::new(1),
            Zone::new(2),
        );

        assert_eq!(r1.priority, 10);
        assert!(!r1.is_voter);
        assert!(r1.is_same_region(&r2));
        assert!(!r1.is_same_zone(&r2));
    }
}

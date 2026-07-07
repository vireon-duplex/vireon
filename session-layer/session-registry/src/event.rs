//! [`RegistryEvent`] — domain events emitted by the registry.
//!
//! See `DESIGN.md` Part 12 (observability) and Part 6 (membership).
//!
//! # Role
//!
//! The registry publishes events for every meaningful state
//! transition: node join/leave, ownership gain/loss, lease expiry,
//! rebalance start/stop. Subscribers (router, migration manager,
//! metrics pipeline) react to these events without polling.
//!
//! # Phase 0
//!
//! Type only — no event bus. Phase 1+ adds a broadcast channel.

use crate::node::{NodeId, NodeInfo};
use crate::version::{Epoch, Generation};
use session_manager::SessionId;

/// A domain event emitted by the registry.
///
/// Events are the **only** push-based notification channel the
/// registry offers. All other APIs are pull-based (lookups, stats).
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum RegistryEvent {
    /// A new node joined the cluster.
    NodeJoined {
        /// The node that joined.
        node: NodeInfo,
        /// The cluster epoch at join time.
        epoch: Epoch,
    },
    /// A node left the cluster (graceful).
    NodeLeft {
        /// The node that left.
        node: NodeId,
        /// The cluster epoch at leave time.
        epoch: Epoch,
    },
    /// A node was marked suspect by the failure detector.
    NodeSuspect {
        /// The suspect node.
        node: NodeId,
    },
    /// A node was marked dead (suspicion timed out).
    NodeDead {
        /// The dead node.
        node: NodeId,
    },
    /// This node gained ownership of a session.
    OwnershipGained {
        /// The session now owned.
        session: SessionId,
        /// The new owner (this node).
        node: NodeId,
        /// The new ownership generation.
        generation: Generation,
    },
    /// This node lost ownership of a session.
    OwnershipLost {
        /// The session no longer owned.
        session: SessionId,
        /// The previous owner (this node).
        node: NodeId,
        /// The generation at which ownership was lost.
        generation: Generation,
    },
    /// A lease expired without renewal.
    LeaseExpired {
        /// The session whose lease expired.
        session: SessionId,
        /// The node that held the lease.
        owner: NodeId,
    },
    /// A rebalance pass started.
    RebalanceStarted {
        /// The cluster epoch at which rebalancing started.
        epoch: Epoch,
    },
    /// A rebalance pass completed.
    RebalanceCompleted {
        /// The cluster epoch at which rebalancing completed.
        epoch: Epoch,
        /// Number of sessions moved.
        sessions_moved: u64,
    },
}

impl RegistryEvent {
    /// Human-readable category name for the event.
    #[must_use]
    pub const fn category(&self) -> &'static str {
        match self {
            Self::NodeJoined { .. }
            | Self::NodeLeft { .. }
            | Self::NodeSuspect { .. }
            | Self::NodeDead { .. } => "membership",
            Self::OwnershipGained { .. } | Self::OwnershipLost { .. } => "ownership",
            Self::LeaseExpired { .. } => "lease",
            Self::RebalanceStarted { .. } | Self::RebalanceCompleted { .. } => "rebalance",
        }
    }

    /// Whether the event indicates a topology change.
    #[must_use]
    pub const fn is_topology_change(&self) -> bool {
        matches!(
            self,
            Self::NodeJoined { .. }
                | Self::NodeLeft { .. }
                | Self::NodeDead { .. }
                | Self::RebalanceStarted { .. }
                | Self::RebalanceCompleted { .. }
        )
    }

    /// Whether the event affects ownership of a specific session.
    #[must_use]
    pub const fn affects_session(&self) -> Option<SessionId> {
        match self {
            Self::OwnershipGained { session, .. }
            | Self::OwnershipLost { session, .. }
            | Self::LeaseExpired { session, .. } => Some(*session),
            _ => None,
        }
    }

    /// Whether the event is informational (no action required).
    #[must_use]
    pub const fn is_informational(&self) -> bool {
        matches!(
            self,
            Self::NodeJoined { .. }
                | Self::RebalanceCompleted { .. }
                | Self::OwnershipGained { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{NodeAddress, NodeCapacity, NodeState, Region, Zone};

    fn sample_node(id: u32) -> NodeInfo {
        NodeInfo {
            id: NodeId::new(id),
            region: Region::GLOBAL,
            zone: Zone::DEFAULT,
            address: NodeAddress::loopback(443),
            capacity: NodeCapacity::default(),
            weight: 100,
            state: NodeState::Alive,
            joined_ns: 0,
        }
    }

    #[test]
    fn category_membership() {
        let e = RegistryEvent::NodeJoined {
            node: sample_node(1),
            epoch: Epoch::INITIAL,
        };
        assert_eq!(e.category(), "membership");

        let e = RegistryEvent::NodeLeft {
            node: NodeId::new(1),
            epoch: Epoch::INITIAL,
        };
        assert_eq!(e.category(), "membership");
    }

    #[test]
    fn category_ownership() {
        let e = RegistryEvent::OwnershipGained {
            session: SessionId::from_pair(1, 1),
            node: NodeId::new(1),
            generation: Generation::INITIAL,
        };
        assert_eq!(e.category(), "ownership");
    }

    #[test]
    fn category_lease() {
        let e = RegistryEvent::LeaseExpired {
            session: SessionId::from_pair(1, 1),
            owner: NodeId::new(1),
        };
        assert_eq!(e.category(), "lease");
    }

    #[test]
    fn category_rebalance() {
        let e = RegistryEvent::RebalanceStarted {
            epoch: Epoch::INITIAL,
        };
        assert_eq!(e.category(), "rebalance");
    }

    #[test]
    fn is_topology_change_for_membership_events() {
        let joined = RegistryEvent::NodeJoined {
            node: sample_node(1),
            epoch: Epoch::INITIAL,
        };
        assert!(joined.is_topology_change());

        let left = RegistryEvent::NodeLeft {
            node: NodeId::new(1),
            epoch: Epoch::INITIAL,
        };
        assert!(left.is_topology_change());

        let dead = RegistryEvent::NodeDead {
            node: NodeId::new(1),
        };
        assert!(dead.is_topology_change());

        let suspect = RegistryEvent::NodeSuspect {
            node: NodeId::new(1),
        };
        assert!(!suspect.is_topology_change()); // suspect is not yet topology change
    }

    #[test]
    fn affects_session_for_ownership_events() {
        let sid = SessionId::from_pair(42, 99);
        let gained = RegistryEvent::OwnershipGained {
            session: sid,
            node: NodeId::new(1),
            generation: Generation::INITIAL,
        };
        assert_eq!(gained.affects_session(), Some(sid));

        let expired = RegistryEvent::LeaseExpired {
            session: sid,
            owner: NodeId::new(1),
        };
        assert_eq!(expired.affects_session(), Some(sid));

        let joined = RegistryEvent::NodeJoined {
            node: sample_node(1),
            epoch: Epoch::INITIAL,
        };
        assert_eq!(joined.affects_session(), None);
    }

    #[test]
    fn is_informational_for_benign_events() {
        let joined = RegistryEvent::NodeJoined {
            node: sample_node(1),
            epoch: Epoch::INITIAL,
        };
        assert!(joined.is_informational());

        let dead = RegistryEvent::NodeDead {
            node: NodeId::new(1),
        };
        assert!(!dead.is_informational());
    }

    #[test]
    fn event_can_be_cloned() {
        let e = RegistryEvent::LeaseExpired {
            session: SessionId::from_pair(1, 1),
            owner: NodeId::new(1),
        };
        let cloned = e.clone();
        let _ = format!("{cloned:?}");
    }
}

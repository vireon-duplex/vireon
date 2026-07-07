//! [`MembershipList`] — cluster membership tracker.
//!
//! See `DESIGN.md` Part 6 (membership protocol).
//!
//! # Role
//!
//! Tracks every node the local registry is aware of, along with
//! its `NodeState` (Alive / Suspect / Dead / Left). The list is
//! consulted by the rebalance planner (which sessions must move),
//! the routing layer (which nodes can serve requests), and the
//! health subsystem.
//!
//! # Phase 0
//!
//! Skeleton only. The Phase 1 implementation will use a
//! `Vec<NodeInfo>` (not a `HashMap`) so that iteration is
//! cache-friendly and we can snapshot in O(n) without locks.

use crate::error::RegistryError;
use crate::node::{NodeId, NodeInfo, NodeState, Region};
use session_manager::SessionId;

// Re-export so callers don't have to dig into node::* for the type.
pub use crate::node::NodeInfo as MemberInfo;

/// Cluster membership tracker.
///
/// Phase 0 ships only a typed skeleton.
#[derive(Debug, Default)]
pub struct MembershipList {
    /// Number of members (the only state Phase 0 tracks).
    count: usize,
}

impl MembershipList {
    /// Construct an empty membership list.
    #[must_use]
    pub const fn new() -> Self {
        Self { count: 0 }
    }

    /// Current member count.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.count
    }

    /// Whether the list is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Add a node to the membership list.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn add(&mut self, info: NodeInfo) -> Result<(), RegistryError> {
        // TODO(phase-1): insert into Vec + dedupe by NodeId.
        let _ = (info, SessionId::nil());
        self.count = self.count.saturating_add(1);
        Err(RegistryError::ShuttingDown)
    }

    /// Remove a node from the list.
    ///
    /// # Errors
    /// Always returns [`RegistryError::NodeNotFound`] in Phase 0.
    pub fn remove(&mut self, id: NodeId) -> Result<NodeInfo, RegistryError> {
        // TODO(phase-1): find + remove + return removed.
        Err(RegistryError::NodeNotFound(id))
    }

    /// Mark a node as suspect (failure detector reported it).
    ///
    /// # Errors
    /// Always returns [`RegistryError::NodeNotFound`] in Phase 0.
    pub fn mark_suspect(&mut self, id: NodeId) -> Result<(), RegistryError> {
        // TODO(phase-1): transition Alive → Suspect.
        Err(RegistryError::NodeNotFound(id))
    }

    /// Mark a node as dead (after suspicion timeout).
    ///
    /// # Errors
    /// Always returns [`RegistryError::NodeNotFound`] in Phase 0.
    pub fn mark_dead(&mut self, id: NodeId) -> Result<(), RegistryError> {
        // TODO(phase-1): transition Suspect → Dead.
        Err(RegistryError::NodeNotFound(id))
    }

    /// Look up a node by id.
    ///
    /// # Errors
    /// Always returns [`RegistryError::NodeNotFound`] in Phase 0.
    pub fn get(&self, id: NodeId) -> Result<&NodeInfo, RegistryError> {
        // TODO(phase-1): linear scan or HashMap.
        Err(RegistryError::NodeNotFound(id))
    }

    /// List all alive node ids.
    ///
    /// # Errors
    /// Always returns an empty Vec in Phase 0.
    pub fn alive_nodes(&self) -> Vec<NodeId> {
        // TODO(phase-1): filter by state == Alive.
        Vec::new()
    }

    /// List nodes in a specific region.
    ///
    /// # Errors
    /// Always returns an empty Vec in Phase 0.
    pub fn nodes_in_region(&self, _region: Region) -> Vec<NodeId> {
        // TODO(phase-1): filter by region.
        Vec::new()
    }

    /// Count nodes in a given state.
    #[must_use]
    pub fn count_in_state(&self, _state: NodeState) -> usize {
        // TODO(phase-1): real count.
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{NodeAddress, NodeCapacity, NodeId, NodeInfo, NodeState, Region, Zone};

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
    fn new_membership_is_empty() {
        let m = MembershipList::new();
        assert!(m.is_empty());
        assert_eq!(m.len(), 0);
    }

    #[test]
    fn add_phase0_stub() {
        let mut m = MembershipList::new();
        assert!(m.add(sample_node(1)).is_err());
    }

    #[test]
    fn remove_phase0_stub() {
        let mut m = MembershipList::new();
        assert!(matches!(
            m.remove(NodeId::new(1)),
            Err(RegistryError::NodeNotFound(_))
        ));
    }

    #[test]
    fn mark_suspect_phase0_stub() {
        let mut m = MembershipList::new();
        assert!(matches!(
            m.mark_suspect(NodeId::new(1)),
            Err(RegistryError::NodeNotFound(_))
        ));
    }

    #[test]
    fn mark_dead_phase0_stub() {
        let mut m = MembershipList::new();
        assert!(matches!(
            m.mark_dead(NodeId::new(1)),
            Err(RegistryError::NodeNotFound(_))
        ));
    }

    #[test]
    fn get_phase0_stub() {
        let m = MembershipList::new();
        assert!(matches!(
            m.get(NodeId::new(1)),
            Err(RegistryError::NodeNotFound(_))
        ));
    }

    #[test]
    fn alive_nodes_empty_in_phase0() {
        let m = MembershipList::new();
        assert!(m.alive_nodes().is_empty());
    }

    #[test]
    fn nodes_in_region_empty_in_phase0() {
        let m = MembershipList::new();
        assert!(m.nodes_in_region(Region::GLOBAL).is_empty());
    }

    #[test]
    fn count_in_state_zero_in_phase0() {
        let m = MembershipList::new();
        assert_eq!(m.count_in_state(NodeState::Alive), 0);
    }
}

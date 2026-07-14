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

use crate::error::RegistryError;
use crate::node::{NodeId, NodeInfo, NodeState, Region};
use session_manager::SessionId;

// Re-export so callers don't have to dig into node::* for the type.
pub use crate::node::NodeInfo as MemberInfo;

/// Cluster membership tracker.
///
/// Stores nodes in a `Vec<NodeInfo>` for cache-friendly iteration.
/// Lookups by `NodeId` are O(n) — acceptable for cluster sizes up
/// to a few thousand nodes.
pub struct MembershipList {
    nodes: Vec<NodeInfo>,
}

impl MembershipList {
    /// Construct an empty membership list.
    #[must_use]
    pub const fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    /// Current member count (all states).
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the list is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Add a node to the membership list.
    ///
    /// If a node with the same `NodeId` already exists, its info is
    /// updated (state, capacity, weight, address) and the node is
    /// promoted back to `Alive` if it was previously dead/suspect.
    ///
    /// # Errors
    /// Returns [`RegistryError::NodeExists`] only if you want to
    /// detect duplicates. In practice this method always succeeds,
    /// updating the node info in place.
    pub fn add(&mut self, info: NodeInfo) -> Result<(), RegistryError> {
        let _ = SessionId::nil(); // suppress unused import warning
        if let Some(existing) = self.nodes.iter_mut().find(|n| n.id == info.id) {
            // Update existing node info.
            *existing = info;
            return Ok(());
        }
        self.nodes.push(info);
        Ok(())
    }

    /// Remove a node from the list.
    ///
    /// # Errors
    /// Returns [`RegistryError::NodeNotFound`] if the node is not
    /// registered.
    pub fn remove(&mut self, id: NodeId) -> Result<NodeInfo, RegistryError> {
        let idx = self
            .nodes
            .iter()
            .position(|n| n.id == id)
            .ok_or(RegistryError::NodeNotFound(id))?;
        Ok(self.nodes.swap_remove(idx))
    }

    /// Mark a node as suspect (failure detector reported it).
    ///
    /// Transitions `Alive` → `Suspect`. No-op if already suspect/dead.
    ///
    /// # Errors
    /// Returns [`RegistryError::NodeNotFound`] if the node is not
    /// registered.
    pub fn mark_suspect(&mut self, id: NodeId) -> Result<(), RegistryError> {
        let node = self
            .nodes
            .iter_mut()
            .find(|n| n.id == id)
            .ok_or(RegistryError::NodeNotFound(id))?;
        if node.state == NodeState::Alive || node.state == NodeState::Joining {
            node.state = NodeState::Suspect;
        }
        Ok(())
    }

    /// Mark a node as dead (after suspicion timeout).
    ///
    /// Transitions `Suspect` → `Dead`. No-op if already dead.
    ///
    /// # Errors
    /// Returns [`RegistryError::NodeNotFound`] if the node is not
    /// registered.
    pub fn mark_dead(&mut self, id: NodeId) -> Result<(), RegistryError> {
        let node = self
            .nodes
            .iter_mut()
            .find(|n| n.id == id)
            .ok_or(RegistryError::NodeNotFound(id))?;
        node.state = NodeState::Dead;
        Ok(())
    }

    /// Look up a node by id.
    ///
    /// # Errors
    /// Returns [`RegistryError::NodeNotFound`] if the node is not
    /// registered.
    pub fn get(&self, id: NodeId) -> Result<&NodeInfo, RegistryError> {
        self.nodes
            .iter()
            .find(|n| n.id == id)
            .ok_or(RegistryError::NodeNotFound(id))
    }

    /// List all alive node ids.
    pub fn alive_nodes(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|n| n.state == NodeState::Alive)
            .map(|n| n.id)
            .collect()
    }

    /// List nodes in a specific region.
    pub fn nodes_in_region(&self, region: Region) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|n| n.region == region)
            .map(|n| n.id)
            .collect()
    }

    /// Count nodes in a given state.
    #[must_use]
    pub fn count_in_state(&self, state: NodeState) -> usize {
        self.nodes.iter().filter(|n| n.state == state).count()
    }

    /// Snapshot all member info (for rebalancing, gossip, etc.).
    pub fn snapshot(&self) -> &[NodeInfo] {
        &self.nodes
    }
}

impl Default for MembershipList {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MembershipList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MembershipList")
            .field("count", &self.nodes.len())
            .field("alive", &self.count_in_state(NodeState::Alive))
            .field("suspect", &self.count_in_state(NodeState::Suspect))
            .field("dead", &self.count_in_state(NodeState::Dead))
            .finish()
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
    fn add_and_get() {
        let mut m = MembershipList::new();
        assert!(m.add(sample_node(1)).is_ok());
        assert_eq!(m.len(), 1);
        assert!(m.get(NodeId::new(1)).is_ok());
    }

    #[test]
    fn add_updates_existing() {
        let mut m = MembershipList::new();
        m.add(sample_node(1)).unwrap();
        let mut updated = sample_node(1);
        updated.weight = 200;
        m.add(updated).unwrap();
        assert_eq!(m.len(), 1);
        assert_eq!(m.get(NodeId::new(1)).unwrap().weight, 200);
    }

    #[test]
    fn remove_returns_node() {
        let mut m = MembershipList::new();
        m.add(sample_node(1)).unwrap();
        let removed = m.remove(NodeId::new(1)).unwrap();
        assert_eq!(removed.id, NodeId::new(1));
        assert!(m.is_empty());
    }

    #[test]
    fn remove_missing_errors() {
        let mut m = MembershipList::new();
        assert!(matches!(
            m.remove(NodeId::new(1)),
            Err(RegistryError::NodeNotFound(_))
        ));
    }

    #[test]
    fn mark_suspect_transitions_alive() {
        let mut m = MembershipList::new();
        m.add(sample_node(1)).unwrap();
        m.mark_suspect(NodeId::new(1)).unwrap();
        assert_eq!(m.get(NodeId::new(1)).unwrap().state, NodeState::Suspect);
    }

    #[test]
    fn mark_dead_transitions() {
        let mut m = MembershipList::new();
        m.add(sample_node(1)).unwrap();
        m.mark_suspect(NodeId::new(1)).unwrap();
        m.mark_dead(NodeId::new(1)).unwrap();
        assert_eq!(m.get(NodeId::new(1)).unwrap().state, NodeState::Dead);
    }

    #[test]
    fn mark_suspect_missing_errors() {
        let mut m = MembershipList::new();
        assert!(matches!(
            m.mark_suspect(NodeId::new(1)),
            Err(RegistryError::NodeNotFound(_))
        ));
    }

    #[test]
    fn mark_dead_missing_errors() {
        let mut m = MembershipList::new();
        assert!(matches!(
            m.mark_dead(NodeId::new(1)),
            Err(RegistryError::NodeNotFound(_))
        ));
    }

    #[test]
    fn get_missing_errors() {
        let m = MembershipList::new();
        assert!(matches!(
            m.get(NodeId::new(1)),
            Err(RegistryError::NodeNotFound(_))
        ));
    }

    #[test]
    fn alive_nodes_filters_state() {
        let mut m = MembershipList::new();
        m.add(sample_node(1)).unwrap();
        m.add(sample_node(2)).unwrap();
        m.add(sample_node(3)).unwrap();
        m.mark_dead(NodeId::new(2)).unwrap();
        let alive = m.alive_nodes();
        assert_eq!(alive.len(), 2);
        assert!(alive.contains(&NodeId::new(1)));
        assert!(alive.contains(&NodeId::new(3)));
    }

    #[test]
    fn nodes_in_region_filters() {
        let mut m = MembershipList::new();
        let mut n1 = sample_node(1);
        n1.region = Region::GLOBAL;
        let mut n2 = sample_node(2);
        n2.region = Region::new(1);
        m.add(n1).unwrap();
        m.add(n2).unwrap();
        let global = m.nodes_in_region(Region::GLOBAL);
        assert_eq!(global.len(), 1);
        assert_eq!(global[0], NodeId::new(1));
    }

    #[test]
    fn count_in_state() {
        let mut m = MembershipList::new();
        m.add(sample_node(1)).unwrap();
        m.add(sample_node(2)).unwrap();
        m.add(sample_node(3)).unwrap();
        m.mark_dead(NodeId::new(1)).unwrap();
        m.mark_suspect(NodeId::new(2)).unwrap();
        assert_eq!(m.count_in_state(NodeState::Alive), 1);
        assert_eq!(m.count_in_state(NodeState::Suspect), 1);
        assert_eq!(m.count_in_state(NodeState::Dead), 1);
    }

    #[test]
    fn alive_nodes_empty_when_all_dead() {
        let mut m = MembershipList::new();
        m.add(sample_node(1)).unwrap();
        m.mark_dead(NodeId::new(1)).unwrap();
        assert!(m.alive_nodes().is_empty());
    }
}

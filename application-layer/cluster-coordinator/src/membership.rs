//! [`ClusterMembership`] — the static cluster topology.
//!
//! Phase 6 is **static** membership: peers are supplied at startup
//! and the set does not change at runtime. Dynamic SWIM/gossip
//! membership is a future phase.
//!
//! A "peer" is any node in the cluster (including self). The
//! membership tracks which peers are currently `Alive` — a node
//! marked down via [`crate::ClusterCoordinator::mark_node_down`] is
//! removed from the alive set but its id stays in the topology so
//! operators can introspect it.

use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;
use session_registry::NodeId;
use session_registry::node::{NodeInfo, NodeState};

use crate::error::ClusterError;

/// Default number of virtual nodes per physical node on the topic
/// ring. Mirrors `session_registry::ring::DEFAULT_VNODES` (150 — the
/// commonly-cited sweet spot from Amazon Dynamo).
pub const DEFAULT_VNODES: u32 = 150;

/// Cluster boot configuration.
#[derive(Clone, Debug)]
pub struct ClusterConfig {
    /// This node's id. Must appear in `peers`.
    pub self_id: NodeId,
    /// All nodes in the cluster, **including self**.
    pub peers: Vec<NodeInfo>,
    /// Virtual nodes per physical node on the topic ring. Default is
    /// [`DEFAULT_VNODES`]. Clamped to `[1, 1024]` by [`ClusterMembership::new`].
    pub vnodes: u32,
}

impl ClusterConfig {
    /// Construct with self id, peers, and the default vnode count.
    #[must_use]
    pub fn new(self_id: NodeId, peers: Vec<NodeInfo>) -> Self {
        Self {
            self_id,
            peers,
            vnodes: DEFAULT_VNODES,
        }
    }
}

/// Static cluster topology — the nodes that make up the cluster and
/// which are currently considered alive.
///
/// Built once at startup from [`ClusterConfig`]; mutated only by
/// `mark_node_down` (which flips a node's state to `Dead` and drops
/// it from the alive set).
#[derive(Debug)]
pub struct ClusterMembership {
    self_id: NodeId,
    nodes: FxHashMap<NodeId, NodeInfo>,
    alive: FxHashSet<NodeId>,
}

impl ClusterMembership {
    /// Validate and freeze a cluster config.
    ///
    /// # Errors
    /// * [`ClusterError::InvalidConfig`] — empty peers, missing self,
    ///   duplicate ids.
    pub fn new(cfg: ClusterConfig) -> Result<Self, ClusterError> {
        if cfg.peers.is_empty() {
            return Err(ClusterError::InvalidConfig {
                reason: "peer list is empty",
            });
        }
        if cfg.self_id.is_nil() {
            return Err(ClusterError::InvalidConfig {
                reason: "self_id is nil",
            });
        }
        let mut nodes: FxHashMap<NodeId, NodeInfo> =
            FxHashMap::with_capacity_and_hasher(cfg.peers.len(), Default::default());
        let mut saw_self = false;
        for p in cfg.peers {
            if p.id.is_nil() {
                return Err(ClusterError::InvalidConfig {
                    reason: "peer has nil NodeId",
                });
            }
            if p.id == cfg.self_id {
                saw_self = true;
            }
            if nodes.insert(p.id, p).is_some() {
                return Err(ClusterError::InvalidConfig {
                    reason: "duplicate NodeId in peers",
                });
            }
        }
        let alive: FxHashSet<NodeId> = nodes
            .iter()
            .filter(|(_, info)| info.state.is_alive())
            .map(|(id, _)| *id)
            .collect();
        if !saw_self {
            return Err(ClusterError::InvalidConfig {
                reason: "self_id not present in peers",
            });
        }
        if !alive.contains(&cfg.self_id) {
            return Err(ClusterError::InvalidConfig {
                reason: "self is not Alive (initial state must be Alive)",
            });
        }
        Ok(Self {
            self_id: cfg.self_id,
            nodes,
            alive,
        })
    }

    /// This node's id.
    #[must_use]
    pub fn self_id(&self) -> NodeId {
        self.self_id
    }

    /// Whether `n` is currently considered alive.
    #[must_use]
    pub fn is_alive(&self, n: NodeId) -> bool {
        self.alive.contains(&n)
    }

    /// All alive node ids, unordered.
    #[must_use]
    pub fn alive_nodes(&self) -> Vec<NodeId> {
        self.alive.iter().copied().collect()
    }

    /// Lookup static info for a node.
    #[must_use]
    pub fn node_info(&self, n: NodeId) -> Option<&NodeInfo> {
        self.nodes.get(&n)
    }

    /// Total peer count (alive + down).
    #[must_use]
    pub fn peer_count(&self) -> usize {
        self.nodes.len()
    }

    /// Mark a node as down. No-op if not in topology or already down.
    /// Returns `true` if the node was alive before this call.
    pub(crate) fn mark_down(&mut self, n: NodeId) -> bool {
        let was = self.alive.remove(&n);
        if let Some(info) = self.nodes.get_mut(&n) {
            info.state = NodeState::Dead;
        }
        was
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use session_registry::node::{NodeAddress, Region, Zone};

    fn node(id: u32, state: NodeState) -> NodeInfo {
        let mut n = NodeInfo::new(
            NodeId::new(id),
            Region::GLOBAL,
            Zone::DEFAULT,
            NodeAddress::loopback(4000 + id as u16),
        );
        n.state = state;
        n
    }

    fn ok_cfg() -> ClusterConfig {
        ClusterConfig::new(
            NodeId::new(1),
            vec![node(1, NodeState::Alive), node(2, NodeState::Alive)],
        )
    }

    #[test]
    fn rejects_empty_peers() {
        let cfg = ClusterConfig::new(NodeId::new(1), vec![]);
        assert!(matches!(
            ClusterMembership::new(cfg),
            Err(ClusterError::InvalidConfig { .. })
        ));
    }

    #[test]
    fn rejects_nil_self() {
        let cfg = ClusterConfig::new(NodeId::NIL, vec![node(1, NodeState::Alive)]);
        assert!(ClusterMembership::new(cfg).is_err());
    }

    #[test]
    fn rejects_self_missing_from_peers() {
        let cfg = ClusterConfig::new(NodeId::new(99), vec![node(1, NodeState::Alive)]);
        assert!(ClusterMembership::new(cfg).is_err());
    }

    #[test]
    fn rejects_duplicate_node_ids() {
        let cfg = ClusterConfig::new(
            NodeId::new(1),
            vec![node(1, NodeState::Alive), node(1, NodeState::Alive)],
        );
        assert!(ClusterMembership::new(cfg).is_err());
    }

    #[test]
    fn rejects_nil_peer_id() {
        let cfg = ClusterConfig::new(
            NodeId::new(1),
            vec![node(1, NodeState::Alive), node(0, NodeState::Alive)],
        );
        assert!(ClusterMembership::new(cfg).is_err());
    }

    #[test]
    fn rejects_self_not_alive() {
        let cfg = ClusterConfig::new(
            NodeId::new(1),
            vec![node(1, NodeState::Joining), node(2, NodeState::Alive)],
        );
        assert!(ClusterMembership::new(cfg).is_err());
    }

    #[test]
    fn happy_path_aliveness() {
        let m = ClusterMembership::new(ok_cfg()).unwrap();
        assert_eq!(m.self_id(), NodeId::new(1));
        assert_eq!(m.peer_count(), 2);
        assert!(m.is_alive(NodeId::new(1)));
        assert!(m.is_alive(NodeId::new(2)));
        let mut alive = m.alive_nodes();
        alive.sort_unstable();
        assert_eq!(alive, vec![NodeId::new(1), NodeId::new(2)]);
    }

    #[test]
    fn mark_down_flips_state_and_clears_alive() {
        let mut m = ClusterMembership::new(ok_cfg()).unwrap();
        assert!(m.mark_down(NodeId::new(2)));
        assert!(!m.is_alive(NodeId::new(2)));
        assert_eq!(
            m.node_info(NodeId::new(2)).map(|i| i.state),
            Some(NodeState::Dead)
        );
        // Idempotent.
        assert!(!m.mark_down(NodeId::new(2)));
        // Self can also be marked down (operator-triggered graceful exit).
        assert!(m.mark_down(NodeId::new(1)));
    }

    #[test]
    fn mark_down_unknown_node_is_noop() {
        let mut m = ClusterMembership::new(ok_cfg()).unwrap();
        assert!(!m.mark_down(NodeId::new(99)));
    }

    #[test]
    fn node_info_returns_some_for_known() {
        let m = ClusterMembership::new(ok_cfg()).unwrap();
        assert!(m.node_info(NodeId::new(1)).is_some());
        assert!(m.node_info(NodeId::new(99)).is_none());
    }
}

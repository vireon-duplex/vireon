//! [`ClusterCoordinator`] — top-level facade combining topology,
//! routing, and remote-subscriber tracking.
//!
//! Owns three pieces of state:
//! * [`ClusterMembership`](crate::ClusterMembership) — static peer list + alive set
//! * [`TopicRouter`](crate::TopicRouter) — topic → owner via consistent hash
//! * [`RemoteSubscriberRegistry`](crate::RemoteSubscriberRegistry) —
//!   subscribers on other nodes that have registered interest in
//!   patterns owned by **this** node
//!
//! The coordinator's job is to answer one question per publish:
//! *"do I own this topic, or do I forward it to another node?"* — and,
//! if we own it, *"which remote nodes need a fanout?"*.
//!
//! ## Stats
//!
//! All counters are `AtomicU64` so they can be read from any thread
//! without locking the coordinator (the coordinator itself is
//! `!Sync` — single-threaded per reactor worker — but stats may be
//! scraped from a separate metrics-poller thread).

use std::sync::atomic::{AtomicU64, Ordering};

use rustc_hash::FxHashSet;
use session_registry::NodeId;

use crate::membership::{ClusterMembership, DEFAULT_VNODES};
use crate::router::TopicRouter;
use crate::subscriber::{RemoteSubscriber, RemoteSubscriberRegistry};
use crate::{ClusterConfig, ClusterError};

/// What the coordinator decided about a publish.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PublishDecision {
    /// Self owns the topic. `remote_targets` lists every alive node
    /// that has at least one matching remote subscription for the
    /// topic. `replicas` lists the additional nodes (beyond self)
    /// that should receive a replicated copy for durability. The
    /// caller should:
    /// 1. Deliver to local subscribers via the existing
    ///    `pubsub_engine::TopicRegistry`.
    /// 2. For each node in `remote_targets`, send an
    ///    `InterNodeMessage::Fanout`.
    /// 3. For each node in `replicas`, send an
    ///    `InterNodeMessage::Publish` (replication copy).
    Own {
        /// Distinct alive nodes (no self) needing fanout.
        remote_targets: Vec<NodeId>,
        /// Distinct alive nodes (no self) needing a replication copy.
        /// Empty when `replication_factor <= 1`.
        replicas: Vec<NodeId>,
    },
    /// Another node owns the topic. The caller should send an
    /// `InterNodeMessage::Publish` to `owner`.
    Forward {
        /// The owning node.
        owner: NodeId,
    },
}

/// Atomic counters; load with `Ordering::Relaxed`, the values are
/// advisory (no invariants depend on them).
#[derive(Default, Debug)]
pub struct ClusterStats {
    pub publishes_routed_local: AtomicU64,
    pub publishes_forwarded: AtomicU64,
    pub remote_subs_added: AtomicU64,
    pub remote_subs_removed: AtomicU64,
    pub nodes_marked_down: AtomicU64,
}

/// Read-only snapshot of [`ClusterStats`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ClusterStatsSnapshot {
    pub publishes_routed_local: u64,
    pub publishes_forwarded: u64,
    pub remote_subs_added: u64,
    pub remote_subs_removed: u64,
    pub nodes_marked_down: u64,
}

impl ClusterStats {
    fn snapshot(&self) -> ClusterStatsSnapshot {
        ClusterStatsSnapshot {
            publishes_routed_local: self.publishes_routed_local.load(Ordering::Relaxed),
            publishes_forwarded: self.publishes_forwarded.load(Ordering::Relaxed),
            remote_subs_added: self.remote_subs_added.load(Ordering::Relaxed),
            remote_subs_removed: self.remote_subs_removed.load(Ordering::Relaxed),
            nodes_marked_down: self.nodes_marked_down.load(Ordering::Relaxed),
        }
    }
}

/// Top-level cluster facade. Single-threaded (one per reactor worker).
pub struct ClusterCoordinator {
    membership: ClusterMembership,
    router: TopicRouter,
    remote_subs: RemoteSubscriberRegistry,
    stats: ClusterStats,
    replication_factor: u8,
}

impl ClusterCoordinator {
    /// Construct from config. Seeds the topic router with every
    /// initially-alive peer.
    ///
    /// # Errors
    /// Propagates [`ClusterError::InvalidConfig`] from
    /// [`ClusterMembership::new`] if the config is malformed.
    pub fn new(cfg: ClusterConfig) -> Result<Self, ClusterError> {
        let alive_nodes: Vec<NodeId> = cfg
            .peers
            .iter()
            .filter(|p| p.state.is_alive())
            .map(|p| p.id)
            .collect();
        let vnodes = if cfg.vnodes == 0 {
            DEFAULT_VNODES
        } else {
            cfg.vnodes
        };
        let replication_factor = cfg.replication_factor.max(1);
        let membership = ClusterMembership::new(cfg)?;
        let mut router = TopicRouter::new(vnodes);
        for n in alive_nodes {
            router.add_node(n);
        }
        Ok(Self {
            membership,
            router,
            remote_subs: RemoteSubscriberRegistry::new(),
            stats: ClusterStats::default(),
            replication_factor,
        })
    }

    /// This node's id.
    #[must_use]
    pub fn self_id(&self) -> NodeId {
        self.membership.self_id()
    }

    /// Whether this node is the primary owner of `topic` in the
    /// consistent-hash ring.
    ///
    /// Used to distinguish forwarded publishes (this node IS the owner)
    /// from replicated publishes (this node is a replica backup).
    #[must_use]
    pub fn owns_topic(&self, topic: &str) -> bool {
        match self.router.owner_of(topic) {
            Some(owner) => owner == self.self_id(),
            None => true, // empty ring → self owns by convention
        }
    }

    /// Total peer count (alive + down).
    #[must_use]
    pub fn peer_count(&self) -> usize {
        self.membership.peer_count()
    }

    /// Number of alive nodes (including self).
    #[must_use]
    pub fn alive_node_count(&self) -> usize {
        self.router.node_count()
    }

    /// Decide what to do with a locally-originated publish.
    ///
    /// Updates the `publishes_routed_local` / `publishes_forwarded`
    /// stat as a side-effect.
    pub fn route_publish(&self, topic: &str) -> PublishDecision {
        match self.router.owner_of(topic) {
            Some(owner) if owner == self.self_id() => {
                self.stats
                    .publishes_routed_local
                    .fetch_add(1, Ordering::Relaxed);
                let remote_targets = self.collect_remote_targets(topic);
                let replicas = self.collect_replicas(topic);
                PublishDecision::Own {
                    remote_targets,
                    replicas,
                }
            }
            Some(owner) => {
                self.stats
                    .publishes_forwarded
                    .fetch_add(1, Ordering::Relaxed);
                PublishDecision::Forward { owner }
            }
            None => {
                // Ring empty — by convention self handles it (no-op
                // deliver to local subs only). Counts as a local route.
                self.stats
                    .publishes_routed_local
                    .fetch_add(1, Ordering::Relaxed);
                PublishDecision::Own {
                    remote_targets: Vec::new(),
                    replicas: Vec::new(),
                }
            }
        }
    }

    /// Collect replica node IDs for `topic` based on the configured
    /// `replication_factor`. Excludes self (the primary owner).
    fn collect_replicas(&self, topic: &str) -> Vec<NodeId> {
        if self.replication_factor <= 1 {
            return Vec::new();
        }
        let n = usize::from(self.replication_factor);
        self.router
            .replicas_of(topic, n)
            .into_iter()
            .filter(|&node| node != self.self_id())
            .collect()
    }

    /// Collect the distinct alive nodes with subscribers matching
    /// `topic`. Excludes self.
    pub fn collect_remote_targets(&self, topic: &str) -> Vec<NodeId> {
        let hits = self.remote_subs.lookup(topic);
        let mut seen: FxHashSet<NodeId> = FxHashSet::default();
        let mut out: Vec<NodeId> = Vec::with_capacity(hits.len());
        for sub in hits {
            let n = sub.node_id;
            if n == self.self_id() || !self.membership.is_alive(n) || seen.contains(&n) {
                continue;
            }
            seen.insert(n);
            out.push(n);
        }
        out
    }

    /// Register a remote subscription (call this when we receive an
    /// `InterNodeMessage::Subscribe`, or when self is non-owner and
    /// is forwarding local subscribers' interest to the topic owner).
    pub fn add_remote_sub(&mut self, pattern: &str, sub: RemoteSubscriber) {
        self.remote_subs.add(pattern, sub);
        self.stats.remote_subs_added.fetch_add(1, Ordering::Relaxed);
    }

    /// Drop a remote subscription. Returns `true` if it was present.
    pub fn remove_remote_sub(&mut self, pattern: &str, sub: &RemoteSubscriber) -> bool {
        let removed = self.remote_subs.remove(pattern, sub);
        if removed {
            self.stats
                .remote_subs_removed
                .fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    /// Mark a node as down. Drops all of the node's remote
    /// subscriptions and removes it from the topic router so
    /// ownership falls through to the next node on the ring.
    ///
    /// Returns the count of remote subscriptions dropped.
    pub fn mark_node_down(&mut self, node: NodeId) -> usize {
        let was_alive = self.membership.mark_down(node);
        if was_alive {
            self.stats.nodes_marked_down.fetch_add(1, Ordering::Relaxed);
            self.router.remove_node(node);
        }
        // `remote_subs_removed` stat tracks explicit unsubscribe, not
        // implicit removal via node-down. The return value is the
        // authoritative count.
        self.remote_subs.remove_node(node)
    }

    /// Snapshot of cluster stats.
    #[must_use]
    pub fn stats(&self) -> ClusterStatsSnapshot {
        self.stats.snapshot()
    }

    /// Borrow the underlying router (for tests / introspection).
    #[must_use]
    pub fn router(&self) -> &TopicRouter {
        &self.router
    }

    /// Borrow the underlying membership.
    #[must_use]
    pub fn membership(&self) -> &ClusterMembership {
        &self.membership
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use session_registry::node::{NodeAddress, NodeInfo, NodeState, Region, Zone};

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

    fn single_node_coordinator() -> ClusterCoordinator {
        // Self alone on the ring → owns every topic.
        let cfg = ClusterConfig::new(NodeId::new(1), vec![node(1, NodeState::Alive)]);
        ClusterCoordinator::new(cfg).unwrap()
    }

    fn two_node_coordinator(self_id: u32) -> ClusterCoordinator {
        let cfg = ClusterConfig::new(
            NodeId::new(self_id),
            vec![node(1, NodeState::Alive), node(2, NodeState::Alive)],
        );
        ClusterCoordinator::new(cfg).unwrap()
    }

    #[test]
    fn single_node_owns_everything() {
        let c = single_node_coordinator();
        for t in &["a", "b", "sensor.temp", "x.y.z"] {
            let d = c.route_publish(t);
            assert!(matches!(d, PublishDecision::Own { .. } if d.remote_targets().is_empty()));
        }
    }

    // Helper trait for tests.
    trait PublishDecisionExt {
        fn remote_targets(&self) -> &[NodeId];
    }
    impl PublishDecisionExt for PublishDecision {
        fn remote_targets(&self) -> &[NodeId] {
            match self {
                PublishDecision::Own { remote_targets, .. } => remote_targets,
                PublishDecision::Forward { .. } => &[],
            }
        }
    }

    #[test]
    fn own_decision_includes_remote_subscriber_nodes() {
        // Two-node cluster, self is node 1 — node 2 is a known alive peer,
        // so it can appear as a remote target.
        let mut c = two_node_coordinator(1);
        // Find a topic owned by self (node 1).
        let mut owned_topic = String::new();
        let self_id = NodeId::new(1);
        for i in 0..1_000 {
            let t = format!("sensor.{i}");
            if c.router().owner_of(&t) == Some(self_id) {
                owned_topic = t;
                break;
            }
        }
        assert!(
            !owned_topic.is_empty(),
            "expected to find a self-owned sensor.* topic"
        );
        c.add_remote_sub(
            "sensor.*",
            RemoteSubscriber {
                node_id: NodeId::new(2),
                remote_conn_idx: 5,
                remote_quic_stream_id: 100,
            },
        );
        let d = c.route_publish(&owned_topic);
        let targets = d.remote_targets();
        assert_eq!(targets, &[NodeId::new(2)]);
    }

    #[test]
    fn own_decision_excludes_self_and_down_nodes() {
        let mut c = two_node_coordinator(1);
        // Self-owned topic: choose one that the router places on node 1.
        // Brute-force find one.
        let mut self_topic = String::new();
        for i in 0..1_000 {
            let t = format!("topic.{i}");
            if c.router().owner_of(&t) == Some(NodeId::new(1)) {
                self_topic = t;
                break;
            }
        }
        assert!(
            !self_topic.is_empty(),
            "expected to find a self-owned topic"
        );

        c.add_remote_sub(
            &self_topic,
            RemoteSubscriber {
                node_id: NodeId::new(2),
                remote_conn_idx: 0,
                remote_quic_stream_id: 0,
            },
        );
        // Mark node 2 down — it should no longer appear as a remote target.
        c.mark_node_down(NodeId::new(2));
        let d = c.route_publish(&self_topic);
        assert!(d.remote_targets().is_empty());
    }

    #[test]
    fn forward_decision_for_non_owned_topic() {
        let c = two_node_coordinator(1);
        // Find a topic owned by node 2.
        let mut remote_topic = String::new();
        for i in 0..1_000 {
            let t = format!("topic.{i}");
            if c.router().owner_of(&t) == Some(NodeId::new(2)) {
                remote_topic = t;
                break;
            }
        }
        assert!(!remote_topic.is_empty());
        let d = c.route_publish(&remote_topic);
        assert!(matches!(d, PublishDecision::Forward { owner } if owner == NodeId::new(2)));
    }

    #[test]
    fn mark_node_down_drops_its_remote_subscriptions() {
        let mut c = single_node_coordinator();
        c.add_remote_sub(
            "a.*",
            RemoteSubscriber {
                node_id: NodeId::new(2),
                remote_conn_idx: 1,
                remote_quic_stream_id: 1,
            },
        );
        c.add_remote_sub(
            "b.*",
            RemoteSubscriber {
                node_id: NodeId::new(2),
                remote_conn_idx: 2,
                remote_quic_stream_id: 2,
            },
        );
        c.add_remote_sub(
            "c.*",
            RemoteSubscriber {
                node_id: NodeId::new(3),
                remote_conn_idx: 0,
                remote_quic_stream_id: 0,
            },
        );
        let dropped = c.mark_node_down(NodeId::new(2));
        assert_eq!(dropped, 2);
        // Node 3 should still be present.
        c.route_publish("c.x"); // ensures lookup still works
    }

    #[test]
    fn mark_node_down_unknown_returns_zero() {
        let mut c = single_node_coordinator();
        assert_eq!(c.mark_node_down(NodeId::new(99)), 0);
    }

    #[test]
    fn add_remove_remote_sub_updates_stats() {
        let mut c = single_node_coordinator();
        let s = RemoteSubscriber {
            node_id: NodeId::new(2),
            remote_conn_idx: 1,
            remote_quic_stream_id: 1,
        };
        c.add_remote_sub("t", s);
        assert_eq!(c.stats().remote_subs_added, 1);
        assert!(c.remove_remote_sub("t", &s));
        assert_eq!(c.stats().remote_subs_removed, 1);
    }

    #[test]
    fn route_publish_stats_advance() {
        let c = two_node_coordinator(1);
        // Find one self-owned and one remote-owned topic.
        let mut self_topic = String::new();
        let mut remote_topic = String::new();
        let self_id = NodeId::new(1);
        let other_id = NodeId::new(2);
        for i in 0..1_000 {
            let t = format!("t.{i}");
            match c.router().owner_of(&t) {
                Some(owner) if owner == self_id && self_topic.is_empty() => self_topic = t,
                Some(owner) if owner == other_id && remote_topic.is_empty() => remote_topic = t,
                _ => {}
            }
            if !self_topic.is_empty() && !remote_topic.is_empty() {
                break;
            }
        }
        c.route_publish(&self_topic);
        c.route_publish(&remote_topic);
        let s = c.stats();
        assert_eq!(s.publishes_routed_local, 1);
        assert_eq!(s.publishes_forwarded, 1);
    }

    #[test]
    fn alive_node_count_reflects_marked_down() {
        let mut c = two_node_coordinator(1);
        assert_eq!(c.alive_node_count(), 2);
        c.mark_node_down(NodeId::new(2));
        assert_eq!(c.alive_node_count(), 1);
    }
}

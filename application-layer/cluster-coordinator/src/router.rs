//! [`TopicRouter`] — topic → owner node via consistent hashing.
//!
//! This is the *routing* equivalent of
//! `session_registry::ring::ConsistentHash` (which is
//! `SessionId`-keyed). We re-implement for arbitrary topic strings
//! because the existing ring's lookup API requires a `&SessionId` and
//! exposes `raw_hi`/`raw_lo` methods that don't generalise cleanly.
//!
//! ## Algorithm
//!
//! Identical to Dynamo / Cassandra / Redis-cluster:
//! 1. For each physical node, scatter `vnodes` virtual nodes onto the
//!    ring at `hash(node_id, vnode_index)`.
//! 2. Keep the ring sorted by hash for `O(log n)` lookups via binary
//!    search.
//! 3. To find the owner of a topic, hash the topic bytes and find the
//!    first ring entry with hash ≥ topic hash (wrapping around to 0
//!    if needed).
//!
//! Adding or removing a single node moves only `~(1/n)` of topics —
//! the consistent-hash property that makes rolling restarts cheap.

use std::hash::{Hash, Hasher};

use rustc_hash::FxHashSet;
use rustc_hash::FxHasher;
use session_registry::NodeId;

/// Minimum/maximum vnodes per physical node.
pub const MIN_VNODES: u32 = 1;
pub const MAX_VNODES: u32 = 1_024;

/// One position on the ring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RingEntry {
    hash: u64,
    node: NodeId,
}

impl RingEntry {
    const fn new(hash: u64, node: NodeId) -> Self {
        Self { hash, node }
    }
}

impl PartialOrd for RingEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RingEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by hash, then by node id for stable ordering at
        // hash collisions.
        self.hash
            .cmp(&other.hash)
            .then(self.node.raw().cmp(&other.node.raw()))
    }
}

/// Topic-keyed consistent-hash ring.
///
/// Tracks alive nodes only — call [`TopicRouter::remove_node`] when a
/// node goes down so ownership falls through to the next node on the
/// ring.
pub struct TopicRouter {
    /// Sorted ring entries.
    ring: Vec<RingEntry>,
    /// Virtual nodes per physical node.
    vnodes: u32,
    /// Distinct alive node ids currently on the ring.
    nodes: Vec<NodeId>,
    /// O(1) dedup set for add/remove.
    node_set: FxHashSet<NodeId>,
}

impl Default for TopicRouter {
    fn default() -> Self {
        Self::new(crate::membership::DEFAULT_VNODES)
    }
}

impl TopicRouter {
    /// Construct an empty ring with `vnodes` virtual nodes per
    /// physical node. `vnodes` is clamped to `[MIN_VNODES, MAX_VNODES]`.
    #[must_use]
    pub fn new(vnodes: u32) -> Self {
        let vnodes = vnodes.clamp(MIN_VNODES, MAX_VNODES);
        Self {
            ring: Vec::new(),
            vnodes,
            nodes: Vec::new(),
            node_set: FxHashSet::default(),
        }
    }

    /// Add a physical node. Places `self.vnodes` ring entries. No-op
    /// if the node is already on the ring or is `NodeId::NIL`.
    pub fn add_node(&mut self, node: NodeId) {
        if node.is_nil() || !self.node_set.insert(node) {
            return;
        }
        for i in 0..self.vnodes {
            let entry = RingEntry::new(hash_pair(node, i), node);
            let pos = self.ring.partition_point(|e| e < &entry);
            self.ring.insert(pos, entry);
        }
        self.nodes.push(node);
    }

    /// Remove a physical node. Removes all of its virtual nodes.
    /// No-op if the node isn't on the ring.
    pub fn remove_node(&mut self, node: NodeId) {
        if !self.node_set.remove(&node) {
            return;
        }
        if let Some(i) = self.nodes.iter().position(|n| *n == node) {
            self.nodes.swap_remove(i);
        }
        self.ring.retain(|e| e.node != node);
    }

    /// Owner of `topic` — the first alive node whose ring position is
    /// at or after `hash(topic)`. Returns `None` if the ring is empty.
    #[must_use]
    pub fn owner_of(&self, topic: &str) -> Option<NodeId> {
        if self.ring.is_empty() {
            return None;
        }
        let h = hash_topic(topic);
        let idx = self.ring.partition_point(|e| e.hash < h);
        let entry = if idx >= self.ring.len() {
            // Wrap around.
            self.ring[0]
        } else {
            self.ring[idx]
        };
        Some(entry.node)
    }

    /// First `n` distinct alive nodes responsible for `topic`
    /// (owner + replicas for future replication). Returns fewer if
    /// the ring has fewer distinct nodes.
    #[must_use]
    pub fn replicas_of(&self, topic: &str, n: usize) -> Vec<NodeId> {
        if self.ring.is_empty() || n == 0 {
            return Vec::new();
        }
        let h = hash_topic(topic);
        let start = self.ring.partition_point(|e| e.hash < h);
        let mut out = Vec::with_capacity(n);
        let len = self.ring.len();
        for i in 0..len {
            let entry = self.ring[(start + i) % len];
            if !out.contains(&entry.node) {
                out.push(entry.node);
                if out.len() == n {
                    break;
                }
            }
        }
        out
    }

    /// Distinct alive node count.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the ring has zero nodes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }
}

// ── Hashing helpers ─────────────────────────────────────────────

fn hash_pair(node: NodeId, vnode: u32) -> u64 {
    let mut hasher = FxHasher::default();
    node.raw().hash(&mut hasher);
    vnode.hash(&mut hasher);
    hasher.finish()
}

fn hash_topic(topic: &str) -> u64 {
    let mut hasher = FxHasher::default();
    topic.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn empty_router_owner_is_none() {
        let r = TopicRouter::default();
        assert!(r.is_empty());
        assert!(r.owner_of("any.topic").is_none());
    }

    #[test]
    fn vnodes_clamped() {
        assert_eq!(TopicRouter::new(0).vnodes, MIN_VNODES);
        assert_eq!(TopicRouter::new(u32::MAX).vnodes, MAX_VNODES);
    }

    #[test]
    fn single_node_owns_everything() {
        let mut r = TopicRouter::new(20);
        r.add_node(NodeId::new(1));
        for t in &["a", "b", "sensor.temp", "alerts", "long.topic.name"] {
            assert_eq!(r.owner_of(t), Some(NodeId::new(1)));
        }
    }

    #[test]
    fn add_node_populates_ring() {
        let mut r = TopicRouter::new(10);
        r.add_node(NodeId::new(1));
        assert_eq!(r.node_count(), 1);
        // 10 vnodes per node.
        assert_eq!(r.ring.len(), 10);
    }

    #[test]
    fn add_nil_node_is_noop() {
        let mut r = TopicRouter::default();
        r.add_node(NodeId::NIL);
        assert_eq!(r.node_count(), 0);
    }

    #[test]
    fn add_duplicate_node_is_noop() {
        let mut r = TopicRouter::new(5);
        r.add_node(NodeId::new(1));
        r.add_node(NodeId::new(1));
        assert_eq!(r.node_count(), 1);
        assert_eq!(r.ring.len(), 5);
    }

    #[test]
    fn remove_node_clears_its_vnodes() {
        let mut r = TopicRouter::new(10);
        r.add_node(NodeId::new(1));
        r.add_node(NodeId::new(2));
        assert_eq!(r.ring.len(), 20);
        r.remove_node(NodeId::new(1));
        assert_eq!(r.node_count(), 1);
        assert_eq!(r.ring.len(), 10);
        for e in &r.ring {
            assert_eq!(e.node, NodeId::new(2));
        }
    }

    #[test]
    fn remove_missing_node_is_noop() {
        let mut r = TopicRouter::new(5);
        r.add_node(NodeId::new(1));
        r.remove_node(NodeId::new(99));
        assert_eq!(r.node_count(), 1);
    }

    #[test]
    fn ownership_is_deterministic_across_runs() {
        let mut r1 = TopicRouter::default();
        let mut r2 = TopicRouter::default();
        for n in 1..=5 {
            r1.add_node(NodeId::new(n));
            r2.add_node(NodeId::new(n));
        }
        for t in &["alpha", "beta", "gamma", "delta", "epsilon"] {
            assert_eq!(r1.owner_of(t), r2.owner_of(t));
        }
    }

    #[test]
    fn adding_node_minimally_disturbs_ownership() {
        // With 5 nodes and 200 vnodes, adding a 6th should move ~1/6
        // of topics. Band matches session-registry's analogous test
        // ([50, 350] for 1000 samples) — FxHash distribution quality
        // varies, so allow generous slack.
        let mut r = TopicRouter::new(200);
        for n in 1..=5 {
            r.add_node(NodeId::new(n));
        }
        let topics: Vec<String> = (0..1000).map(|i| format!("topic.{i}")).collect();
        let before: Vec<Option<NodeId>> = topics.iter().map(|t| r.owner_of(t)).collect();
        r.add_node(NodeId::new(6));
        let moved = topics
            .iter()
            .zip(before.iter())
            .filter(|(t, prev)| r.owner_of(t) != **prev)
            .count();
        // Expect ~1/6 ≈ 166. Allow [50, 350].
        assert!(moved > 50 && moved < 350, "moved = {moved} (expected ~166)",);
    }

    #[test]
    fn owner_is_none_when_all_nodes_removed() {
        let mut r = TopicRouter::new(10);
        r.add_node(NodeId::new(1));
        r.add_node(NodeId::new(2));
        r.remove_node(NodeId::new(1));
        r.remove_node(NodeId::new(2));
        assert!(r.owner_of("t").is_none());
    }

    #[test]
    fn replicas_returns_n_distinct_nodes() {
        let mut r = TopicRouter::default();
        for n in 1..=5 {
            r.add_node(NodeId::new(n));
        }
        let replicas = r.replicas_of("sensor.temp", 3);
        assert_eq!(replicas.len(), 3);
        let mut dedup = replicas.clone();
        dedup.sort_unstable();
        dedup.dedup();
        assert_eq!(dedup.len(), 3);
    }

    #[test]
    fn replicas_returns_fewer_when_ring_smaller() {
        let mut r = TopicRouter::default();
        r.add_node(NodeId::new(1));
        r.add_node(NodeId::new(2));
        let replicas = r.replicas_of("t", 5);
        assert_eq!(replicas.len(), 2);
    }

    #[test]
    fn replicas_zero_returns_empty() {
        let mut r = TopicRouter::default();
        r.add_node(NodeId::new(1));
        assert!(r.replicas_of("t", 0).is_empty());
    }

    #[test]
    fn replicas_owner_is_first_element() {
        let mut r = TopicRouter::default();
        for n in 1..=5 {
            r.add_node(NodeId::new(n));
        }
        let topic = "sensor.temp";
        let owner = r.owner_of(topic);
        let replicas = r.replicas_of(topic, 3);
        assert_eq!(replicas.first().copied(), owner);
    }
}

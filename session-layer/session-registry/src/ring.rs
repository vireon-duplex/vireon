//! [`ConsistentHash`] ring — session-to-node assignment via virtual
//! nodes.
//!
//! See `DESIGN.md` Part 4 (consistent hash ring) and Part 8
//! (rebalancing).
//!
//! # Why consistent hashing?
//!
//! A naive modulo hash (`hash(session) % node_count`) redistributes
//! ~100% of sessions whenever a node joins or leaves. Consistent
//! hashing with virtual nodes (vnodes) reduces this to
//! `~(1 / node_count)` of sessions — essential at hyperscale.
//!
//! # Algorithm
//!
//! 1. For each physical node, place `vnodes` (default 150) entries
//!    on the ring at `hash(node_id, vnode_index)`.
//! 2. Sort the ring by hash value.
//! 3. To find the owner of a key, binary-search for the first ring
//!    entry with `hash >= hash(key)` (wrap around to 0 if needed).
//!
//! # Hash function
//!
//! Uses FxHash (rustc-hash) — fast, non-cryptographic, well-distributed.
//! Phase 1+ may switch to a keyed hash (SipHash-2-4) to prevent
//! hash-flooding attacks.

use crate::node::NodeId;
use profiler::profile;
use session_manager::SessionId;

/// Default number of virtual nodes per physical node. 150 is the
/// commonly-cited sweet spot (from Amazon Dynamo, ~10% deviation
/// at 10 nodes).
pub const DEFAULT_VNODES: u32 = 150;

/// Minimum vnodes per physical node (below this, distribution
/// quality degrades sharply).
pub const MIN_VNODES: u32 = 1;

/// Maximum vnodes per physical node. Beyond this, memory use grows
/// without improving distribution quality.
pub const MAX_VNODES: u32 = 1_024;

/// A single entry on the hash ring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingEntry {
    /// Hash value (sort key).
    pub hash: u64,
    /// The node owning this ring position.
    pub node_id: NodeId,
}

impl RingEntry {
    /// Construct a new ring entry.
    #[must_use]
    pub const fn new(hash: u64, node_id: NodeId) -> Self {
        Self { hash, node_id }
    }
}

impl PartialOrd for RingEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RingEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by hash, then by node_id for stable ordering.
        self.hash
            .cmp(&other.hash)
            .then(self.node_id.raw().cmp(&other.node_id.raw()))
    }
}

// ── FxHash (simplified) ─────────────────────────────────────────

/// A simple, fast, non-cryptographic hash function based on FxHash.
/// Used internally for ring placement and key lookup.
///
/// Note: this is NOT a cryptographic hash. Phase 1+ should switch
/// to a keyed hash to prevent hash-flooding attacks.
const FX_HASH_SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;
const FX_HASH_MUL: u64 = 0x517cc1b727220a95;

#[allow(dead_code)]
fn fxhash_u64(value: u64) -> u64 {
    let mut hash = FX_HASH_SEED;
    hash = (hash ^ value).wrapping_mul(FX_HASH_MUL);
    hash
}

fn fxhash_pair(a: u64, b: u64) -> u64 {
    let mut hash = FX_HASH_SEED;
    hash = (hash ^ a).wrapping_mul(FX_HASH_MUL);
    hash = (hash ^ b).wrapping_mul(FX_HASH_MUL);
    hash
}

// ── ConsistentHash ──────────────────────────────────────────────

/// Consistent-hash ring for session-to-node assignment.
///
/// Uses virtual nodes for uniform distribution. The ring is kept
/// sorted by hash for O(log n) lookups via binary search.
#[derive(Debug)]
pub struct ConsistentHash {
    /// Sorted ring entries (by hash, then node_id).
    ring: Vec<RingEntry>,
    /// Virtual nodes per physical node.
    vnodes: u32,
    /// Distinct physical node ids currently on the ring.
    /// (Kept as a sorted Vec for fast add/remove.)
    nodes: Vec<NodeId>,
}

impl Default for ConsistentHash {
    fn default() -> Self {
        Self::new(DEFAULT_VNODES)
    }
}

impl ConsistentHash {
    /// Construct an empty ring with the given vnode count.
    ///
    /// # Panics
    /// Never in Phase 0; bad inputs are clamped to
    /// `[MIN_VNODES, MAX_VNODES]`.
    #[must_use]
    pub fn new(vnodes: u32) -> Self {
        let vnodes = vnodes.clamp(MIN_VNODES, MAX_VNODES);
        Self {
            ring: Vec::new(),
            vnodes,
            nodes: Vec::new(),
        }
    }

    /// Number of physical nodes on the ring.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Number of virtual nodes (ring entries).
    #[must_use]
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Whether the ring is empty (no physical nodes).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    /// Add a physical node to the ring. Places `self.vnodes` virtual
    /// nodes at hashed positions. No-op if the node is already on
    /// the ring.
    #[profile]
    pub fn add_node(&mut self, node: NodeId) {
        if node.is_nil() || self.nodes.contains(&node) {
            return;
        }
        for i in 0..self.vnodes {
            let hash = fxhash_pair(node.raw() as u64, i as u64);
            self.ring.push(RingEntry::new(hash, node));
        }
        self.nodes.push(node);
        self.ring.sort_unstable();
    }

    /// Remove a physical node from the ring. Removes all of its
    /// virtual nodes. No-op if the node isn't on the ring.
    #[profile]
    pub fn remove_node(&mut self, node: NodeId) {
        let idx = match self.nodes.iter().position(|n| *n == node) {
            Some(i) => i,
            None => return,
        };
        self.nodes.swap_remove(idx);
        self.ring.retain(|e| e.node_id != node);
        // swap_remove breaks sort order relative to original indices,
        // but ring sort is unaffected since we retain + sort below.
        // Actually: retain preserves relative order, so ring is still
        // sorted. No re-sort needed.
    }

    /// Find the owner of a session id.
    ///
    /// Returns the first node whose ring position is at or after
    /// `hash(session_id)`. Wraps around to the first ring entry if
    /// `hash(session_id)` exceeds all ring entries.
    #[must_use]
    #[profile]
    pub fn lookup(&self, key: &SessionId) -> Option<NodeId> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = fxhash_pair(key.raw_hi(), key.raw_lo());
        let idx = self.ring.partition_point(|e| e.hash < hash);
        let entry = if idx >= self.ring.len() {
            // Wrap around.
            self.ring[0]
        } else {
            self.ring[idx]
        };
        Some(entry.node_id)
    }

    /// Find the next `n` distinct nodes responsible for a session
    /// (for replication / fallback). Returns fewer if the ring has
    /// fewer distinct nodes.
    #[must_use]
    #[profile]
    pub fn lookup_n(&self, key: &SessionId, n: usize) -> Vec<NodeId> {
        if self.ring.is_empty() || n == 0 {
            return Vec::new();
        }
        let hash = fxhash_pair(key.raw_hi(), key.raw_lo());
        let start = self.ring.partition_point(|e| e.hash < hash);
        let mut result = Vec::with_capacity(n);
        let len = self.ring.len();
        for i in 0..len {
            let entry = self.ring[(start + i) % len];
            if !result.contains(&entry.node_id) {
                result.push(entry.node_id);
                if result.len() == n {
                    break;
                }
            }
        }
        result
    }

    /// All physical nodes currently on the ring (unspecified order).
    #[must_use]
    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    /// The configured vnode count.
    #[must_use]
    pub const fn vnodes(&self) -> u32 {
        self.vnodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_ring_lookups_return_none() {
        let r = ConsistentHash::default();
        assert!(r.is_empty());
        assert_eq!(r.lookup(&SessionId::from_pair(1, 1)), None);
        assert_eq!(
            r.lookup_n(&SessionId::from_pair(1, 1), 3),
            Vec::<NodeId>::new()
        );
    }

    #[test]
    fn add_node_populates_ring() {
        let mut r = ConsistentHash::new(10);
        r.add_node(NodeId::new(1));
        assert_eq!(r.node_count(), 1);
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn add_nil_node_is_noop() {
        let mut r = ConsistentHash::default();
        r.add_node(NodeId::NIL);
        assert_eq!(r.node_count(), 0);
    }

    #[test]
    fn add_duplicate_node_is_noop() {
        let mut r = ConsistentHash::new(10);
        r.add_node(NodeId::new(1));
        r.add_node(NodeId::new(1));
        assert_eq!(r.node_count(), 1);
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn remove_node_clears_vnodes() {
        let mut r = ConsistentHash::new(10);
        r.add_node(NodeId::new(1));
        r.add_node(NodeId::new(2));
        assert_eq!(r.len(), 20);
        r.remove_node(NodeId::new(1));
        assert_eq!(r.node_count(), 1);
        assert_eq!(r.len(), 10);
        // Remaining entries all belong to node 2.
        for e in &r.ring {
            assert_eq!(e.node_id, NodeId::new(2));
        }
    }

    #[test]
    fn remove_missing_node_is_noop() {
        let mut r = ConsistentHash::new(10);
        r.add_node(NodeId::new(1));
        r.remove_node(NodeId::new(99));
        assert_eq!(r.node_count(), 1);
    }

    #[test]
    fn lookup_returns_a_node() {
        let mut r = ConsistentHash::default();
        r.add_node(NodeId::new(1));
        r.add_node(NodeId::new(2));
        r.add_node(NodeId::new(3));
        let owner = r.lookup(&SessionId::from_pair(42, 99));
        assert!(owner.is_some());
        let owner = owner.unwrap();
        // Must be one of the registered nodes.
        assert!(owner == NodeId::new(1) || owner == NodeId::new(2) || owner == NodeId::new(3));
    }

    #[test]
    fn lookup_is_deterministic() {
        let mut r1 = ConsistentHash::default();
        let mut r2 = ConsistentHash::default();
        for n in 1..=5 {
            r1.add_node(NodeId::new(n));
            r2.add_node(NodeId::new(n));
        }
        let id = SessionId::from_pair(123, 456);
        assert_eq!(r1.lookup(&id), r2.lookup(&id));
    }

    #[test]
    fn lookup_n_returns_distinct_nodes() {
        let mut r = ConsistentHash::default();
        for n in 1..=5 {
            r.add_node(NodeId::new(n));
        }
        let owners = r.lookup_n(&SessionId::from_pair(1, 1), 3);
        assert_eq!(owners.len(), 3);
        // All distinct.
        let mut sorted = owners.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 3);
    }

    #[test]
    fn lookup_n_handles_fewer_nodes_than_requested() {
        let mut r = ConsistentHash::default();
        r.add_node(NodeId::new(1));
        r.add_node(NodeId::new(2));
        let owners = r.lookup_n(&SessionId::from_pair(1, 1), 5);
        assert_eq!(owners.len(), 2);
    }

    #[test]
    fn lookup_n_zero_returns_empty() {
        let mut r = ConsistentHash::default();
        r.add_node(NodeId::new(1));
        assert_eq!(r.lookup_n(&SessionId::from_pair(1, 1), 0).len(), 0);
    }

    #[test]
    fn adding_node_minimally_disturbs_ownership() {
        // With 5 nodes and 200 vnodes each, adding a 6th node should
        // cause ~1/6 of sessions to move.
        let mut r = ConsistentHash::new(200);
        for n in 1..=5 {
            r.add_node(NodeId::new(n));
        }
        // Sample 1000 sessions.
        let mut before = Vec::with_capacity(1000);
        for i in 0..1000 {
            before.push(r.lookup(&SessionId::from_pair(i, i * 7)));
        }
        // Add a 6th node.
        r.add_node(NodeId::new(6));
        let mut moved = 0usize;
        for (i, before_owner) in before.iter().enumerate() {
            let after_owner = r.lookup(&SessionId::from_pair(i as u64, (i as u64) * 7));
            if before_owner != &after_owner {
                moved += 1;
            }
        }
        // Expect ~1/6 = ~166. Allow a generous range [50, 350].
        assert!(moved > 50 && moved < 350, "moved = {moved} (expected ~166)");
    }

    #[test]
    fn vnodes_clamped_to_valid_range() {
        let r_zero = ConsistentHash::new(0);
        assert_eq!(r_zero.vnodes(), MIN_VNODES);
        let r_huge = ConsistentHash::new(u32::MAX);
        assert_eq!(r_huge.vnodes(), MAX_VNODES);
    }

    #[test]
    fn ring_entry_ordering() {
        let a = RingEntry::new(100, NodeId::new(1));
        let b = RingEntry::new(200, NodeId::new(2));
        let c = RingEntry::new(100, NodeId::new(3));
        assert!(a < b);
        assert!(a < c); // Same hash, lower node_id wins.
    }

    #[test]
    fn fxhash_is_deterministic() {
        let h1 = fxhash_pair(1, 2);
        let h2 = fxhash_pair(1, 2);
        assert_eq!(h1, h2);
        let h3 = fxhash_pair(2, 1);
        assert_ne!(h1, h3);
    }

    #[test]
    fn fxhash_u64_distributes() {
        // Hashes for sequential inputs should be spread out.
        let mut hashes = Vec::with_capacity(100);
        for i in 0..100u64 {
            hashes.push(fxhash_u64(i));
        }
        hashes.sort_unstable();
        hashes.dedup();
        // All 100 hashes should be distinct.
        assert_eq!(hashes.len(), 100);
    }

    #[test]
    fn nodes_accessor() {
        let mut r = ConsistentHash::default();
        r.add_node(NodeId::new(5));
        r.add_node(NodeId::new(3));
        r.add_node(NodeId::new(7));
        assert_eq!(r.nodes().len(), 3);
        assert!(r.nodes().contains(&NodeId::new(5)));
    }
}

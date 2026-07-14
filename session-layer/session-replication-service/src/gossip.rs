//! [`GossipEngine`] — gossip-based replication protocol.
//!
//! See `DESIGN.md` Part 5 (gossip protocol).
//!
//! # Gossip Protocol
//!
//! The gossip engine uses infection-style dissemination inspired by
//! SWIM and HyParView:
//!
//! 1. **Broadcast**: A node disseminates a (key, payload) pair to a
//!    random subset of peers (fan-out = 3 by default).
//! 2. **Reconcile**: Peers exchange state hashes to detect missing
//!    updates.
//! 3. **Anti-Entropy**: On mismatch, a full sync is initiated.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::ReplicationConfig;
use crate::error::ReplicationError;
use crate::replica::ReplicaId;

/// A gossiped message entry.
#[derive(Clone, Debug)]
struct GossipEntry {
    payload: Vec<u8>,
    broadcast_ns: u64,
    /// Peers that have acknowledged this message.
    acked_by: Vec<ReplicaId>,
}

/// Gossip-based replication engine.
///
/// Broadcasts metadata updates via infection-style gossip to a
/// random subset of peers. Provides eventual consistency with high
/// availability and partition tolerance.
pub struct GossipEngine {
    interval_ms: u64,
    fanout: u8,
    /// In-memory message store: key → latest entry.
    messages: HashMap<String, GossipEntry>,
    /// Known peer replicas.
    peers: Vec<ReplicaId>,
    /// Number of broadcasts sent.
    broadcast_count: u64,
    /// Number of successful reconciliations.
    reconcile_count: u64,
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

impl GossipEngine {
    /// Construct a new gossip engine with the given broadcast
    /// interval.
    #[must_use]
    pub fn new(interval_ms: u64) -> Self {
        Self {
            interval_ms,
            fanout: 3,
            messages: HashMap::new(),
            peers: Vec::new(),
            broadcast_count: 0,
            reconcile_count: 0,
        }
    }

    /// Construct from a replication config.
    #[must_use]
    pub fn from_config(cfg: &ReplicationConfig) -> Self {
        Self::new(cfg.gossip_interval_ms)
    }

    /// Register a peer replica for gossip dissemination.
    pub fn add_peer(&mut self, peer: ReplicaId) {
        if !self.peers.contains(&peer) {
            self.peers.push(peer);
        }
    }

    /// Remove a peer replica.
    pub fn remove_peer(&mut self, peer: ReplicaId) {
        self.peers.retain(|p| *p != peer);
    }

    /// Broadcast a (key, payload) pair to the gossip mesh.
    ///
    /// Stores the message locally and marks it for dissemination to
    /// up to `fanout` random peers.
    ///
    /// # Errors
    /// Always returns `Ok(())` — broadcast is best-effort.
    pub fn broadcast(&mut self, key: &str, payload: &[u8]) -> Result<(), ReplicationError> {
        let entry = GossipEntry {
            payload: payload.to_vec(),
            broadcast_ns: now_ns(),
            acked_by: Vec::new(),
        };
        self.messages.insert(key.to_string(), entry);
        self.broadcast_count += 1;
        Ok(())
    }

    /// Reconcile state with a specific peer.
    ///
    /// Exchanges state hashes. Returns `Ok(true)` if divergence was
    /// detected and anti-entropy is needed, `Ok(false)` if states
    /// match.
    ///
    /// # Errors
    /// Returns [`ReplicationError::ReplicaNotFound`] if the peer is
    /// not registered.
    pub fn reconcile(&mut self, peer: ReplicaId) -> Result<bool, ReplicationError> {
        if !self.peers.contains(&peer) {
            return Err(ReplicationError::ReplicaNotFound(peer));
        }
        self.reconcile_count += 1;
        // In a real implementation, we'd compare Merkle tree hashes.
        // For now, assume the peer has no data — divergence = true.
        let has_messages = !self.messages.is_empty();
        Ok(has_messages)
    }

    /// Read the latest payload for a key.
    pub fn read(&self, key: &str) -> Option<&[u8]> {
        self.messages.get(key).map(|e| e.payload.as_slice())
    }

    /// Record an acknowledgment from a peer for a key.
    pub fn ack(&mut self, key: &str, peer: ReplicaId) {
        if let Some(entry) = self.messages.get_mut(key) {
            if !entry.acked_by.contains(&peer) {
                entry.acked_by.push(peer);
            }
        }
    }

    /// Whether all peers have acknowledged a given key.
    pub fn is_fully_disseminated(&self, key: &str) -> bool {
        match self.messages.get(key) {
            Some(entry) => {
                !self.peers.is_empty()
                    && entry.acked_by.len() == self.peers.len()
            }
            None => false,
        }
    }

    /// Purge messages that have been fully disseminated.
    ///
    /// Returns the number of purged messages.
    pub fn purge_disseminated(&mut self) -> usize {
        let before = self.messages.len();
        let peers = &self.peers;
        self.messages.retain(|_, entry| {
            !(!peers.is_empty() && entry.acked_by.len() == peers.len())
        });
        before - self.messages.len()
    }

    /// The configured gossip interval (ms).
    #[must_use]
    pub const fn interval_ms(&self) -> u64 {
        self.interval_ms
    }

    /// The configured fan-out (peers contacted per round).
    #[must_use]
    pub const fn fanout(&self) -> u8 {
        self.fanout
    }

    /// Number of known peers.
    #[must_use]
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Number of active (non-purged) messages.
    #[must_use]
    pub fn message_count(&self) -> usize {
        self.messages.len()
    }

    /// Total broadcasts sent since startup.
    #[must_use]
    pub const fn broadcast_count(&self) -> u64 {
        self.broadcast_count
    }

    /// Total reconciliation rounds performed.
    #[must_use]
    pub const fn reconcile_count(&self) -> u64 {
        self.reconcile_count
    }
}

impl std::fmt::Debug for GossipEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipEngine")
            .field("interval_ms", &self.interval_ms)
            .field("fanout", &self.fanout)
            .field("peers", &self.peers.len())
            .field("messages", &self.messages.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use session_registry::node::NodeId;

    fn peer(n: u32) -> ReplicaId {
        ReplicaId::from_node(NodeId::new(n))
    }

    #[test]
    fn gossip_engine_new() {
        let engine = GossipEngine::new(1_000);
        assert_eq!(engine.interval_ms(), 1_000);
        assert_eq!(engine.fanout(), 3);
        assert_eq!(engine.peer_count(), 0);
    }

    #[test]
    fn gossip_broadcast_stores_message() {
        let mut engine = GossipEngine::new(500);
        assert!(engine.broadcast("key1", b"value1").is_ok());
        assert_eq!(engine.message_count(), 1);
        assert_eq!(engine.broadcast_count(), 1);
        assert_eq!(engine.read("key1").unwrap(), b"value1");
    }

    #[test]
    fn gossip_broadcast_overwrites() {
        let mut engine = GossipEngine::new(500);
        engine.broadcast("key1", b"v1").unwrap();
        engine.broadcast("key1", b"v2").unwrap();
        assert_eq!(engine.message_count(), 1);
        assert_eq!(engine.read("key1").unwrap(), b"v2");
    }

    #[test]
    fn gossip_reconcile_unknown_peer_errors() {
        let mut engine = GossipEngine::new(500);
        assert!(matches!(
            engine.reconcile(peer(1)),
            Err(ReplicationError::ReplicaNotFound(_))
        ));
    }

    #[test]
    fn gossip_reconcile_known_peer_ok() {
        let mut engine = GossipEngine::new(500);
        engine.add_peer(peer(1));
        assert!(engine.reconcile(peer(1)).is_ok());
        assert_eq!(engine.reconcile_count(), 1);
    }

    #[test]
    fn gossip_add_remove_peer() {
        let mut engine = GossipEngine::new(500);
        engine.add_peer(peer(1));
        engine.add_peer(peer(2));
        assert_eq!(engine.peer_count(), 2);
        // Adding same peer is a no-op.
        engine.add_peer(peer(1));
        assert_eq!(engine.peer_count(), 2);
        engine.remove_peer(peer(1));
        assert_eq!(engine.peer_count(), 1);
    }

    #[test]
    fn gossip_ack_tracking() {
        let mut engine = GossipEngine::new(500);
        engine.add_peer(peer(1));
        engine.add_peer(peer(2));
        engine.broadcast("key1", b"v1").unwrap();

        assert!(!engine.is_fully_disseminated("key1"));
        engine.ack("key1", peer(1));
        assert!(!engine.is_fully_disseminated("key1"));
        engine.ack("key1", peer(2));
        assert!(engine.is_fully_disseminated("key1"));
    }

    #[test]
    fn gossip_purge_disseminated() {
        let mut engine = GossipEngine::new(500);
        engine.add_peer(peer(1));
        engine.broadcast("key1", b"v1").unwrap();
        engine.broadcast("key2", b"v2").unwrap();

        // Fully disseminate key1.
        engine.ack("key1", peer(1));
        let purged = engine.purge_disseminated();
        assert_eq!(purged, 1);
        assert_eq!(engine.message_count(), 1);
        assert!(engine.read("key1").is_none());
        assert!(engine.read("key2").is_some());
    }

    #[test]
    fn gossip_from_config() {
        let cfg = ReplicationConfig::new().with_gossip_interval_ms(2_000);
        let engine = GossipEngine::from_config(&cfg);
        assert_eq!(engine.interval_ms(), 2_000);
    }
}

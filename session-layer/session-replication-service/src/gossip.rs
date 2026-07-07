//! [`GossipEngine`] — Phase 1+ skeleton for the gossip-based
//! replication protocol.
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
//! 2. **Reconcile**: Periodically, peers exchange Merkle tree
//!    hashes to detect missing updates.
//! 3. **Anti-Entropy**: On mismatch, a full sync is initiated.
//!
//! # Phase 0
//!
//! Type + stub only. Phase 1+ adds real networking.

use crate::config::ReplicationConfig;
use crate::error::ReplicationError;
use crate::replica::ReplicaId;

/// Phase 1+ gossip-based replication engine.
///
/// Broadcasts metadata updates via infection-style gossip to a
/// random subset of peers. Provides eventual consistency with high
/// availability and partition tolerance.
#[allow(dead_code)]
pub struct GossipEngine {
    interval_ms: u64,
    fanout: u8,
}

impl GossipEngine {
    /// Construct a new gossip engine with the given broadcast
    /// interval.
    #[must_use]
    pub fn new(interval_ms: u64) -> Self {
        Self {
            interval_ms,
            fanout: 3,
        }
    }

    /// Construct from a replication config.
    #[must_use]
    pub fn from_config(cfg: &ReplicationConfig) -> Self {
        Self::new(cfg.gossip_interval_ms)
    }

    /// Broadcast a (key, payload) pair to the gossip mesh.
    ///
    /// # Errors
    /// Always returns `Ok(())` in Phase 0; Phase 1+ will return
    /// `Err` on network failure.
    pub fn broadcast(&self, key: &str, payload: &[u8]) -> Result<(), ReplicationError> {
        // Phase 0: stub.
        let _ = (key, payload);
        Ok(())
    }

    /// Reconcile state with a specific peer.
    ///
    /// Exchanges Merkle tree hashes and initiates anti-entropy if
    /// divergence is detected.
    ///
    /// # Errors
    /// Always returns `Ok(())` in Phase 0.
    pub fn reconcile(&self, peer: ReplicaId) -> Result<(), ReplicationError> {
        // Phase 0: stub.
        let _ = peer;
        Ok(())
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gossip_engine_new() {
        let engine = GossipEngine::new(1_000);
        assert_eq!(engine.interval_ms(), 1_000);
        assert_eq!(engine.fanout(), 3);
    }

    #[test]
    fn gossip_broadcast_is_stub_ok() {
        let engine = GossipEngine::new(500);
        assert!(engine.broadcast("key1", b"value1").is_ok());
    }

    #[test]
    fn gossip_reconcile_is_stub_ok() {
        let engine = GossipEngine::new(500);
        let peer = ReplicaId::from_node(session_registry::node::NodeId::new(1));
        assert!(engine.reconcile(peer).is_ok());
    }

    #[test]
    fn gossip_from_config() {
        let cfg = ReplicationConfig::new().with_gossip_interval_ms(2_000);
        let engine = GossipEngine::from_config(&cfg);
        assert_eq!(engine.interval_ms(), 2_000);
    }
}

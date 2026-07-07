//! [`SessionDirectory`] — the main registry facade.
//!
//! See `DESIGN.md` Part 1 (architecture), Part 9 (session locator),
//! Part 5 (lease & quorum).
//!
//! # Role
//!
//! `SessionDirectory` is the single entry point that callers use to
//! resolve a `SessionId` to its owner node, register new sessions,
//! renew leases, and react to topology changes. It composes:
//!
//! - a [`ConsistentHash`] ring (placement),
//! - an [`OwnershipTable`] (per-session records),
//! - a [`MembershipList`] (node liveness),
//! - a [`RegistryStats`] (observability).
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a
//! stub error so that callers can code against the API contract
//! today. Phase 1 fills in the real logic.

use crate::config::RegistryConfig;
use crate::error::RegistryError;
use crate::health::HealthReport;
use crate::lease::LeaseInfo;
use crate::locator::SessionLocator;
use crate::node::{NodeId, NodeInfo};
use crate::ownership::OwnershipTable;
use crate::ring::ConsistentHash;
use crate::stats::RegistryStats;
use session_manager::SessionId;

/// The distributed session directory facade.
///
/// Composes the ring, ownership table, membership list, and stats.
/// Phase 0 ships a typed skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct SessionDirectory {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: RegistryConfig,
    /// Consistent-hash ring (real impl in Phase 0).
    #[allow(dead_code)]
    ring: ConsistentHash,
    /// Per-session ownership records (Phase 1 will populate).
    #[allow(dead_code)]
    ownership: OwnershipTable,
    /// Atomic observability counters.
    stats: RegistryStats,
}

impl SessionDirectory {
    /// Construct a directory from validated configuration.
    ///
    /// # Errors
    /// Returns [`RegistryError::InvalidConfig`] if the config fails
    /// validation.
    pub fn new(cfg: RegistryConfig) -> Result<Self, RegistryError> {
        cfg.validate()?;
        Ok(Self {
            cfg,
            ring: ConsistentHash::new(150),
            ownership: OwnershipTable::new(),
            stats: RegistryStats::new(),
        })
    }

    /// Register a new session on the ring.
    ///
    /// Resolves the owning node via the consistent-hash ring and
    /// installs an ownership entry + lease.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn register_session(&mut self, _id: SessionId) -> Result<SessionLocator, RegistryError> {
        // TODO(phase-1): ring lookup + ownership insert + lease grant.
        Err(RegistryError::ShuttingDown)
    }

    /// Resolve a session to its current locator.
    ///
    /// This is the **hot path** — must be O(1) in steady state
    /// (cache hit). Phase 1 will add the cache layer.
    ///
    /// # Errors
    /// Always returns [`RegistryError::SessionNotFound`] in Phase 0.
    pub fn lookup(&self, id: SessionId) -> Result<SessionLocator, RegistryError> {
        // TODO(phase-1): cache → ownership table → ring.
        self.stats.record_lookup(false);
        Err(RegistryError::SessionNotFound(id))
    }

    /// Unregister a session (graceful close).
    ///
    /// # Errors
    /// Always returns [`RegistryError::SessionNotFound`] in Phase 0.
    pub fn unregister(&mut self, id: SessionId) -> Result<(), RegistryError> {
        // TODO(phase-1): ownership remove + lease revoke.
        Err(RegistryError::SessionNotFound(id))
    }

    /// Add a node to the cluster.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn add_node(&mut self, _info: NodeInfo) -> Result<(), RegistryError> {
        // TODO(phase-1): membership add + ring add + epoch bump.
        Err(RegistryError::ShuttingDown)
    }

    /// Remove a node from the cluster (graceful leave).
    ///
    /// # Errors
    /// Always returns [`RegistryError::NodeNotFound`] in Phase 0.
    pub fn remove_node(&mut self, id: NodeId) -> Result<(), RegistryError> {
        // TODO(phase-1): membership remove + ring remove + rebalance.
        Err(RegistryError::NodeNotFound(id))
    }

    /// Renew a session's lease.
    ///
    /// # Errors
    /// Always returns [`RegistryError::SessionNotFound`] in Phase 0.
    pub fn renew_lease(&mut self, id: SessionId, _now_ns: u64) -> Result<LeaseInfo, RegistryError> {
        // TODO(phase-1): load entry + verify owner + extend expiry.
        Err(RegistryError::SessionNotFound(id))
    }

    /// Snapshot the directory's health.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    /// Reference to the underlying stats (for testing / metrics).
    #[must_use]
    pub fn stats(&self) -> &RegistryStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;
    use crate::node::{Region, Zone};

    fn make_dir() -> SessionDirectory {
        let cfg = Preset::SingleRegion.to_config(NodeId::new(1)).unwrap();
        SessionDirectory::new(cfg).unwrap()
    }

    #[test]
    fn directory_constructs_from_valid_config() {
        let dir = make_dir();
        let h = dir.health();
        assert!(h.is_healthy());
    }

    #[test]
    fn directory_rejects_invalid_config() {
        let mut cfg = Preset::SingleRegion.to_config(NodeId::new(1)).unwrap();
        cfg.ring_vnodes = 0; // invalid
        assert!(SessionDirectory::new(cfg).is_err());
    }

    #[test]
    fn register_session_phase0_stub() {
        let mut dir = make_dir();
        assert!(dir.register_session(SessionId::from_pair(1, 1)).is_err());
    }

    #[test]
    fn lookup_phase0_stub() {
        let dir = make_dir();
        let res = dir.lookup(SessionId::from_pair(1, 1));
        assert!(matches!(res, Err(RegistryError::SessionNotFound(_))));
    }

    #[test]
    fn unregister_phase0_stub() {
        let mut dir = make_dir();
        assert!(dir.unregister(SessionId::from_pair(1, 1)).is_err());
    }

    #[test]
    fn renew_lease_phase0_stub() {
        let mut dir = make_dir();
        assert!(dir.renew_lease(SessionId::from_pair(1, 1), 0).is_err());
    }

    #[test]
    fn add_node_phase0_stub() {
        let mut dir = make_dir();
        let info = NodeInfo {
            id: NodeId::new(2),
            region: Region::GLOBAL,
            zone: Zone::DEFAULT,
            address: crate::node::NodeAddress::loopback(443),
            capacity: crate::node::NodeCapacity::default(),
            weight: 100,
            state: crate::node::NodeState::Alive,
            joined_ns: 0,
        };
        assert!(dir.add_node(info).is_err());
    }

    #[test]
    fn remove_node_phase0_stub() {
        let mut dir = make_dir();
        assert!(dir.remove_node(NodeId::new(2)).is_err());
    }
}

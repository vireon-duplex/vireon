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

use std::collections::HashMap;

use crate::config::RegistryConfig;
use crate::error::RegistryError;
use crate::health::HealthReport;
use crate::lease::LeaseInfo;
use crate::locator::SessionLocator;
use crate::membership::MembershipList;
use crate::node::{NodeId, NodeInfo};
use crate::ownership::{OwnershipEntry, OwnershipTable};
use crate::ring::ConsistentHash;
use crate::stats::RegistryStats;
use crate::version::{Epoch, Generation, MonotonicVersion};
use connection_shard::ConnectionId;
use session_manager::SessionId;

/// The distributed session directory facade.
///
/// Composes the ring, ownership table, membership list, and stats.
pub struct SessionDirectory {
    cfg: RegistryConfig,
    ring: ConsistentHash,
    ownership: OwnershipTable,
    membership: MembershipList,
    /// Lookup cache for the hot path.
    cache: HashMap<SessionId, SessionLocator>,
    /// Current cluster epoch (bumped on topology change).
    epoch: Epoch,
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
            ring: ConsistentHash::new(cfg.ring_vnodes),
            ownership: OwnershipTable::new(),
            membership: MembershipList::new(),
            cache: HashMap::new(),
            epoch: Epoch::INITIAL,
            stats: RegistryStats::new(),
            cfg,
        })
    }

    /// Register a new session on the ring.
    ///
    /// Resolves the owning node via the consistent-hash ring and
    /// installs an ownership entry + lease.
    ///
    /// # Errors
    /// - [`RegistryError::RingEmpty`] if no nodes are registered.
    /// - [`RegistryError::SessionExists`] if the session is already
    ///   registered.
    pub fn register_session(&mut self, id: SessionId) -> Result<SessionLocator, RegistryError> {
        let owner = self.ring.lookup(&id).ok_or(RegistryError::RingEmpty)?;

        let now_ns = 0u64; // caller provides timing in real deployments
        let lease = LeaseInfo::new(owner, Generation::INITIAL, now_ns, self.cfg.lease_ttl_ms);

        let entry =
            OwnershipEntry::new(id, owner, Generation::INITIAL, self.epoch, lease.expiry_ns);
        self.ownership.insert(entry)?;

        let locator = SessionLocator::new(
            id,
            owner,
            ConnectionId::from_raw(0),
            0,
            MonotonicVersion::new(1),
            lease,
        );

        // Update cache.
        self.cache.insert(id, locator);
        Ok(locator)
    }

    /// Resolve a session to its current locator.
    ///
    /// Checks the cache first (hot path), then falls back to the
    /// ownership table.
    ///
    /// # Errors
    /// Returns [`RegistryError::SessionNotFound`] if the session is
    /// not registered.
    pub fn lookup(&self, id: SessionId) -> Result<SessionLocator, RegistryError> {
        // Cache hit.
        if let Some(locator) = self.cache.get(&id) {
            self.stats.record_lookup(true);
            return Ok(*locator);
        }

        // Cache miss → ownership table.
        let entry = self.ownership.get(id)?;
        let lease = LeaseInfo::new(entry.node_id, entry.generation, 0, self.cfg.lease_ttl_ms);
        let locator = SessionLocator::new(
            id,
            entry.node_id,
            ConnectionId::from_raw(0),
            0,
            MonotonicVersion::new(1),
            lease,
        );

        self.stats.record_lookup(false);
        Ok(locator)
    }

    /// Unregister a session (graceful close).
    ///
    /// # Errors
    /// Returns [`RegistryError::SessionNotFound`] if the session is
    /// not registered.
    pub fn unregister(&mut self, id: SessionId) -> Result<(), RegistryError> {
        self.ownership.remove(id)?;
        self.cache.remove(&id);
        Ok(())
    }

    /// Add a node to the cluster.
    ///
    /// Adds to the membership list and the consistent-hash ring.
    /// Bumps the cluster epoch.
    ///
    /// # Errors
    /// Returns [`RegistryError::NodeExists`] if the node is already
    /// registered.
    pub fn add_node(&mut self, info: NodeInfo) -> Result<(), RegistryError> {
        if info.id.is_nil() {
            return Err(RegistryError::InvalidConfig(
                "node id must not be nil".into(),
            ));
        }
        let node_id = info.id;
        self.membership.add(info)?;
        self.ring.add_node(node_id);
        self.stats.record_node_added();
        self.epoch = Epoch::new(self.epoch.raw() + 1);
        // Invalidate cache — topology changed.
        self.cache.clear();
        Ok(())
    }

    /// Remove a node from the cluster (graceful leave).
    ///
    /// # Errors
    /// Returns [`RegistryError::NodeNotFound`] if the node is not
    /// registered.
    pub fn remove_node(&mut self, id: NodeId) -> Result<(), RegistryError> {
        self.membership.remove(id)?;
        self.ring.remove_node(id);
        self.epoch = Epoch::new(self.epoch.raw() + 1);
        // Invalidate cache — topology changed.
        self.cache.clear();
        Ok(())
    }

    /// Renew a session's lease.
    ///
    /// # Errors
    /// Returns [`RegistryError::SessionNotFound`] if the session is
    /// not registered.
    pub fn renew_lease(&mut self, id: SessionId, now_ns: u64) -> Result<LeaseInfo, RegistryError> {
        let entry = self.ownership.get(id)?;
        let renewed_lease = LeaseInfo::new(
            entry.node_id,
            entry.generation,
            now_ns,
            self.cfg.lease_ttl_ms,
        );

        // Update the ownership entry with the new expiry.
        let updated = OwnershipEntry::new(
            id,
            entry.node_id,
            entry.generation,
            entry.epoch,
            renewed_lease.expiry_ns,
        );
        // Remove + re-insert with updated lease.
        let _ = self.ownership.remove(id);
        self.ownership.insert(updated)?;

        // Update cache.
        if let Some(locator) = self.cache.get_mut(&id) {
            locator.lease = renewed_lease;
        }

        self.stats.record_lease_renewed();
        Ok(renewed_lease)
    }

    /// Current number of registered sessions.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.ownership.len()
    }

    /// Current number of nodes on the ring.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.ring.node_count()
    }

    /// Current cluster epoch.
    #[must_use]
    pub const fn epoch(&self) -> Epoch {
        self.epoch
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

impl std::fmt::Debug for SessionDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionDirectory")
            .field("self_node", &self.cfg.self_node_id)
            .field("nodes", &self.ring.node_count())
            .field("sessions", &self.ownership.len())
            .field("epoch", &self.epoch.raw())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;
    use crate::node::{NodeAddress, NodeCapacity, NodeState, Region, Zone};

    fn make_dir() -> SessionDirectory {
        let cfg = Preset::SingleRegion.to_config(NodeId::new(1)).unwrap();
        SessionDirectory::new(cfg).unwrap()
    }

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
    fn register_on_empty_ring_errors() {
        let mut dir = make_dir();
        // No nodes added yet.
        assert!(matches!(
            dir.register_session(SessionId::from_pair(1, 1)),
            Err(RegistryError::RingEmpty)
        ));
    }

    #[test]
    fn add_node_then_register_succeeds() {
        let mut dir = make_dir();
        dir.add_node(sample_node(1)).unwrap();
        dir.add_node(sample_node(2)).unwrap();

        let locator = dir.register_session(SessionId::from_pair(1, 1)).unwrap();
        assert!(locator.is_valid());
        assert_eq!(dir.session_count(), 1);
    }

    #[test]
    fn lookup_returns_registered_locator() {
        let mut dir = make_dir();
        dir.add_node(sample_node(1)).unwrap();
        let id = SessionId::from_pair(42, 99);
        let registered = dir.register_session(id).unwrap();

        let looked_up = dir.lookup(id).unwrap();
        assert_eq!(looked_up.session_id, id);
        assert_eq!(looked_up.owner_node, registered.owner_node);
    }

    #[test]
    fn lookup_missing_session_errors() {
        let dir = make_dir();
        assert!(matches!(
            dir.lookup(SessionId::from_pair(1, 1)),
            Err(RegistryError::SessionNotFound(_))
        ));
    }

    #[test]
    fn unregister_removes_session() {
        let mut dir = make_dir();
        dir.add_node(sample_node(1)).unwrap();
        let id = SessionId::from_pair(1, 1);
        dir.register_session(id).unwrap();
        assert_eq!(dir.session_count(), 1);

        dir.unregister(id).unwrap();
        assert_eq!(dir.session_count(), 0);
        assert!(dir.lookup(id).is_err());
    }

    #[test]
    fn renew_lease_extends_expiry() {
        let mut dir = make_dir();
        dir.add_node(sample_node(1)).unwrap();
        let id = SessionId::from_pair(1, 1);
        let original = dir.register_session(id).unwrap();

        let renewed = dir.renew_lease(id, 5_000_000_000).unwrap();
        assert!(renewed.expiry_ns > original.lease.expiry_ns);
    }

    #[test]
    fn add_remove_node_bumps_epoch() {
        let mut dir = make_dir();
        let initial_epoch = dir.epoch().raw();

        dir.add_node(sample_node(1)).unwrap();
        assert!(dir.epoch().raw() > initial_epoch);

        let before_remove = dir.epoch().raw();
        dir.remove_node(NodeId::new(1)).unwrap();
        assert!(dir.epoch().raw() > before_remove);
    }

    #[test]
    fn add_node_increases_node_count() {
        let mut dir = make_dir();
        assert_eq!(dir.node_count(), 0);
        dir.add_node(sample_node(1)).unwrap();
        assert_eq!(dir.node_count(), 1);
        dir.add_node(sample_node(2)).unwrap();
        assert_eq!(dir.node_count(), 2);
    }

    #[test]
    fn remove_node_missing_errors() {
        let mut dir = make_dir();
        assert!(matches!(
            dir.remove_node(NodeId::new(99)),
            Err(RegistryError::NodeNotFound(_))
        ));
    }

    #[test]
    fn multiple_sessions_register_to_ring_owners() {
        let mut dir = make_dir();
        for i in 1..=3 {
            dir.add_node(sample_node(i)).unwrap();
        }

        let mut owners = std::collections::HashSet::new();
        for i in 0..20 {
            let locator = dir
                .register_session(SessionId::from_pair(i, i * 7))
                .unwrap();
            owners.insert(locator.owner_node);
        }
        // With 3 nodes and 20 sessions, we expect at least 2 distinct owners.
        assert!(owners.len() >= 2);
    }

    #[test]
    fn debug_formatter_works() {
        let dir = make_dir();
        let dbg = format!("{dir:?}");
        assert!(dbg.contains("SessionDirectory"));
    }
}

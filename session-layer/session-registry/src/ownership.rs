//! [`OwnershipEntry`], [`OwnershipTable`] — session-to-node
//! ownership records.
//!
//! See `DESIGN.md` Part 3 (session ownership) and Part 7 (ownership
//! transfer).

use crate::error::RegistryError;
use crate::node::NodeId;
use crate::version::{Epoch, Generation};
use session_manager::SessionId;

// ── OwnershipEntry ──────────────────────────────────────────────

/// Ownership record for a single session.
///
/// The registry stores one of these per active session. It records
/// who owns the session, at which generation/epoch, and when the
/// lease expires.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OwnershipEntry {
    /// The session this entry owns.
    pub session_id: SessionId,
    /// The owning node.
    pub node_id: NodeId,
    /// Ownership generation (incremented on each transfer).
    pub generation: Generation,
    /// Cluster epoch at which this entry was written.
    pub epoch: Epoch,
    /// When the ownership lease expires (nanoseconds since cluster
    /// epoch).
    pub lease_expiry_ns: u64,
}

impl OwnershipEntry {
    /// Construct a new ownership entry.
    #[must_use]
    pub const fn new(
        session_id: SessionId,
        node_id: NodeId,
        generation: Generation,
        epoch: Epoch,
        lease_expiry_ns: u64,
    ) -> Self {
        Self {
            session_id,
            node_id,
            generation,
            epoch,
            lease_expiry_ns,
        }
    }

    /// Whether the lease has expired at `now_ns`.
    #[must_use]
    pub const fn is_expired(self, now_ns: u64) -> bool {
        self.lease_expiry_ns <= now_ns
    }

    /// Whether `node` is the current owner.
    #[must_use]
    pub const fn owns(self, node: NodeId) -> bool {
        self.node_id.raw() == node.raw()
    }

    /// Whether this entry is stamped with a more recent generation
    /// than `other` (i.e. it supersedes `other`).
    #[must_use]
    pub const fn supersedes(self, other: Self) -> bool {
        self.generation.is_newer_than(other.generation)
    }
}

// ── OwnershipTable (Phase 1 skeleton) ───────────────────────────

/// Ownership table skeleton. Phase 1 will populate this with an
/// `FxHashMap<SessionId, OwnershipEntry>` and lock-free operations.
// TODO(phase-1): real ownership table with HashMap + epoch GC.
#[derive(Debug, Default)]
pub struct OwnershipTable {
    /// Number of entries (the only state Phase 0 tracks).
    count: usize,
}

impl OwnershipTable {
    /// Construct an empty table.
    #[must_use]
    pub const fn new() -> Self {
        Self { count: 0 }
    }

    /// Current entry count.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.count
    }

    /// Whether the table is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Phase-1 `get` placeholder.
    ///
    /// # Errors
    /// Always returns [`RegistryError::SessionNotFound`] in Phase 0.
    pub fn get(&self, id: SessionId) -> Result<OwnershipEntry, RegistryError> {
        // TODO(phase-1): index lookup
        Err(RegistryError::SessionNotFound(id))
    }

    /// Phase-1 `insert` placeholder.
    ///
    /// # Errors
    /// Always returns [`RegistryError::ResourceExhausted`] in Phase 0.
    pub fn insert(&mut self, _entry: OwnershipEntry) -> Result<(), RegistryError> {
        // TODO(phase-1): install entry
        self.count = self.count.saturating_add(1);
        Err(RegistryError::CapacityExceeded(
            "ownership table phase-1 stub",
        ))
    }

    /// Phase-1 `remove` placeholder.
    ///
    /// # Errors
    /// Always returns [`RegistryError::SessionNotFound`] in Phase 0.
    pub fn remove(&mut self, id: SessionId) -> Result<(), RegistryError> {
        // TODO(phase-1): remove entry
        Err(RegistryError::SessionNotFound(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry_for(node: u32, generation: u64) -> OwnershipEntry {
        OwnershipEntry::new(
            SessionId::from_pair(1, 1),
            NodeId::new(node),
            Generation::new(generation),
            Epoch::INITIAL,
            30_000_000_000,
        )
    }

    #[test]
    fn entry_owns_check() {
        let e = entry_for(5, 1);
        assert!(e.owns(NodeId::new(5)));
        assert!(!e.owns(NodeId::new(6)));
    }

    #[test]
    fn entry_expired_check() {
        let e = OwnershipEntry::new(
            SessionId::from_pair(1, 1),
            NodeId::new(1),
            Generation::INITIAL,
            Epoch::INITIAL,
            10_000_000_000,
        );
        assert!(e.is_expired(11_000_000_000));
        assert!(!e.is_expired(5_000_000_000));
    }

    #[test]
    fn entry_supersedes() {
        let old = entry_for(1, 1);
        let new = entry_for(2, 2);
        assert!(new.supersedes(old));
        assert!(!old.supersedes(new));
    }

    #[test]
    fn table_default_empty() {
        let t = OwnershipTable::default();
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn table_get_phase0_stub() {
        let t = OwnershipTable::new();
        assert!(matches!(
            t.get(SessionId::nil()),
            Err(RegistryError::SessionNotFound(_))
        ));
    }

    #[test]
    fn table_insert_phase0_stub() {
        let mut t = OwnershipTable::new();
        let e = entry_for(1, 1);
        assert!(t.insert(e).is_err());
    }

    #[test]
    fn table_remove_phase0_stub() {
        let mut t = OwnershipTable::new();
        assert!(t.remove(SessionId::nil()).is_err());
    }
}

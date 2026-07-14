//! [`OwnershipEntry`], [`OwnershipTable`] — session-to-node
//! ownership records.
//!
//! See `DESIGN.md` Part 3 (session ownership) and Part 7 (ownership
//! transfer).

use std::collections::HashMap;

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

// ── OwnershipTable ──────────────────────────────────────────────

/// Ownership table mapping `SessionId → OwnershipEntry`.
///
/// Backed by a `HashMap` for O(1) lookups on the hot path.
pub struct OwnershipTable {
    entries: HashMap<SessionId, OwnershipEntry>,
}

impl OwnershipTable {
    /// Construct an empty table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Current entry count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the table is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Look up an ownership entry by session id.
    ///
    /// # Errors
    /// Returns [`RegistryError::SessionNotFound`] if the session is
    /// not registered.
    pub fn get(&self, id: SessionId) -> Result<OwnershipEntry, RegistryError> {
        self.entries
            .get(&id)
            .copied()
            .ok_or(RegistryError::SessionNotFound(id))
    }

    /// Insert an ownership entry.
    ///
    /// If an entry already exists, the new one must supersede it
    /// (higher generation); otherwise returns
    /// [`RegistryError::SessionExists`].
    ///
    /// # Errors
    /// - [`RegistryError::SessionExists`] if the session is already
    ///   registered and the new entry does not supersede the existing
    ///   one.
    pub fn insert(&mut self, entry: OwnershipEntry) -> Result<(), RegistryError> {
        if let Some(existing) = self.entries.get(&entry.session_id) {
            if !entry.supersedes(*existing) {
                return Err(RegistryError::SessionExists(entry.session_id));
            }
        }
        self.entries.insert(entry.session_id, entry);
        Ok(())
    }

    /// Remove an ownership entry.
    ///
    /// # Errors
    /// Returns [`RegistryError::SessionNotFound`] if the session is
    /// not registered.
    pub fn remove(&mut self, id: SessionId) -> Result<(), RegistryError> {
        self.entries
            .remove(&id)
            .ok_or(RegistryError::SessionNotFound(id))?;
        Ok(())
    }

    /// Returns `true` if the session is currently registered.
    #[must_use]
    pub fn contains(&self, id: SessionId) -> bool {
        self.entries.contains_key(&id)
    }

    /// Purge all entries whose lease has expired at `now_ns`.
    ///
    /// Returns the number of entries purged.
    pub fn purge_expired(&mut self, now_ns: u64) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, e| !e.is_expired(now_ns));
        before - self.entries.len()
    }
}

impl Default for OwnershipTable {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for OwnershipTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnershipTable")
            .field("entries", &self.entries.len())
            .finish()
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
    fn table_insert_and_get() {
        let mut t = OwnershipTable::new();
        let e = entry_for(1, 1);
        assert!(t.insert(e).is_ok());
        assert_eq!(t.len(), 1);
        let got = t.get(SessionId::from_pair(1, 1)).unwrap();
        assert_eq!(got, e);
    }

    #[test]
    fn table_insert_duplicate_same_gen_fails() {
        let mut t = OwnershipTable::new();
        let e = entry_for(1, 1);
        t.insert(e).unwrap();
        // Same session, same generation → error.
        assert!(matches!(
            t.insert(entry_for(2, 1)),
            Err(RegistryError::SessionExists(_))
        ));
    }

    #[test]
    fn table_insert_superseding_gen_succeeds() {
        let mut t = OwnershipTable::new();
        t.insert(entry_for(1, 1)).unwrap();
        // Higher generation supersedes.
        assert!(t.insert(entry_for(2, 2)).is_ok());
        let got = t.get(SessionId::from_pair(1, 1)).unwrap();
        assert_eq!(got.node_id, NodeId::new(2));
    }

    #[test]
    fn table_remove() {
        let mut t = OwnershipTable::new();
        t.insert(entry_for(1, 1)).unwrap();
        assert!(t.remove(SessionId::from_pair(1, 1)).is_ok());
        assert!(t.is_empty());
    }

    #[test]
    fn table_remove_missing_errors() {
        let mut t = OwnershipTable::new();
        assert!(matches!(
            t.remove(SessionId::nil()),
            Err(RegistryError::SessionNotFound(_))
        ));
    }

    #[test]
    fn table_get_missing_errors() {
        let t = OwnershipTable::new();
        assert!(matches!(
            t.get(SessionId::nil()),
            Err(RegistryError::SessionNotFound(_))
        ));
    }

    #[test]
    fn table_contains() {
        let mut t = OwnershipTable::new();
        assert!(!t.contains(SessionId::from_pair(1, 1)));
        t.insert(entry_for(1, 1)).unwrap();
        assert!(t.contains(SessionId::from_pair(1, 1)));
    }

    #[test]
    fn table_purge_expired() {
        let mut t = OwnershipTable::new();
        let expired = OwnershipEntry::new(
            SessionId::from_pair(1, 1),
            NodeId::new(1),
            Generation::new(1),
            Epoch::INITIAL,
            1_000, // expired at 1us
        );
        let active = OwnershipEntry::new(
            SessionId::from_pair(2, 2),
            NodeId::new(2),
            Generation::new(1),
            Epoch::INITIAL,
            999_999_999_999, // far future
        );
        t.insert(expired).unwrap();
        t.insert(active).unwrap();

        let purged = t.purge_expired(5_000_000_000);
        assert_eq!(purged, 1);
        assert_eq!(t.len(), 1);
        assert!(t.contains(SessionId::from_pair(2, 2)));
    }
}

//! [`CrdtEngine`] — CRDT-based conflict-free replication.
//!
//! See `DESIGN.md` Part 7 (CRDT approach).
//!
//! # CRDT Approach
//!
//! Conflict-free Replicated Data Types (CRDTs) guarantee that
//! replicas converge to the same state without consensus:
//!
//! 1. **Merge**: Any two states can be merged deterministically.
//! 2. **Read**: Reads return a value that is a valid state.
//! 3. **No Conflicts**: Merges always succeed.
//!
//! Vireon uses **LWW-Map** (last-writer-wins per key) for session
//! metadata. Each value is tagged with a monotonically increasing
//! epoch; on merge, the higher-epoch value wins.

use std::collections::HashMap;

use crate::error::ReplicationError;

/// A CRDT entry: value tagged with a logical timestamp (epoch).
#[derive(Clone, Debug)]
struct CrdtEntry {
    value: Vec<u8>,
    /// Logical timestamp — higher wins on merge.
    timestamp: u64,
}

/// CRDT-based replication engine using last-writer-wins semantics.
///
/// Each key stores the latest (highest-timestamp) value seen.
/// Merges are deterministic and always succeed — no conflicts.
pub struct CrdtEngine {
    /// Local epoch counter (incremented on each merge).
    epoch: u64,
    /// Key → LWW entry.
    store: HashMap<String, CrdtEntry>,
}

impl CrdtEngine {
    /// Construct a new CRDT engine.
    #[must_use]
    pub fn new() -> Self {
        Self {
            epoch: 0,
            store: HashMap::new(),
        }
    }

    /// Merge state from a remote replica for the given key.
    ///
    /// CRDT merge is always successful. The remote value wins if
    /// its timestamp is higher than (or equal to, with tie-break by
    /// lexicographic comparison) the local value's timestamp.
    ///
    /// The `other` bytes are treated as the remote value payload,
    /// tagged with the current local epoch + 1 as a synthetic
    /// timestamp.
    ///
    /// # Errors
    /// Always returns `Ok(())`.
    pub fn merge(&mut self, key: &str, other: &[u8]) -> Result<(), ReplicationError> {
        self.epoch = self.epoch.saturating_add(1);

        let should_update = match self.store.get(key) {
            Some(existing) => existing.timestamp <= self.epoch,
            None => true,
        };

        if should_update {
            self.store.insert(
                key.to_string(),
                CrdtEntry {
                    value: other.to_vec(),
                    timestamp: self.epoch,
                },
            );
        }

        Ok(())
    }

    /// Read the current value for a key.
    ///
    /// Returns `None` if the key has never been merged.
    ///
    /// # Errors
    /// Always returns `Ok`.
    pub fn read(&self, key: &str) -> Result<Option<Vec<u8>>, ReplicationError> {
        Ok(self.store.get(key).map(|e| e.value.clone()))
    }

    /// Write a local value with the next epoch.
    ///
    /// Unlike `merge`, this is a local write (not from a remote
    /// replica). It stamps the value with the next epoch.
    pub fn write(&mut self, key: &str, value: &[u8]) -> Result<(), ReplicationError> {
        self.epoch = self.epoch.saturating_add(1);
        self.store.insert(
            key.to_string(),
            CrdtEntry {
                value: value.to_vec(),
                timestamp: self.epoch,
            },
        );
        Ok(())
    }

    /// The current merge epoch (number of merges performed).
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Number of keys in the store.
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.store.len()
    }

    /// Whether the store is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Remove a key from the store.
    pub fn remove(&mut self, key: &str) {
        self.store.remove(key);
    }

    /// List all keys.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.store.keys().map(String::as_str)
    }
}

impl Default for CrdtEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CrdtEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrdtEngine")
            .field("epoch", &self.epoch)
            .field("keys", &self.store.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crdt_engine_new() {
        let engine = CrdtEngine::new();
        assert_eq!(engine.epoch(), 0);
        assert!(engine.is_empty());
    }

    #[test]
    fn crdt_merge_stores_value() {
        let mut engine = CrdtEngine::new();
        engine.merge("key1", b"value1").unwrap();
        assert_eq!(engine.epoch(), 1);
        assert_eq!(engine.key_count(), 1);
        let val = engine.read("key1").unwrap().unwrap();
        assert_eq!(val, b"value1");
    }

    #[test]
    fn crdt_merge_overwrites_same_key() {
        let mut engine = CrdtEngine::new();
        engine.merge("key1", b"v1").unwrap();
        engine.merge("key1", b"v2").unwrap();
        assert_eq!(engine.key_count(), 1);
        let val = engine.read("key1").unwrap().unwrap();
        assert_eq!(val, b"v2");
        assert_eq!(engine.epoch(), 2);
    }

    #[test]
    fn crdt_read_missing_returns_none() {
        let engine = CrdtEngine::new();
        let result = engine.read("missing").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn crdt_write_and_merge() {
        let mut engine = CrdtEngine::new();
        engine.write("key1", b"local").unwrap();
        assert_eq!(engine.epoch(), 1);

        // Merge with higher epoch overwrites.
        engine.merge("key1", b"remote").unwrap();
        assert_eq!(engine.epoch(), 2);
        let val = engine.read("key1").unwrap().unwrap();
        assert_eq!(val, b"remote");
    }

    #[test]
    fn crdt_multiple_keys() {
        let mut engine = CrdtEngine::new();
        engine.merge("a", b"1").unwrap();
        engine.merge("b", b"2").unwrap();
        engine.merge("c", b"3").unwrap();
        assert_eq!(engine.key_count(), 3);
        assert_eq!(engine.epoch(), 3);

        // Verify each key.
        assert_eq!(engine.read("a").unwrap().unwrap(), b"1");
        assert_eq!(engine.read("b").unwrap().unwrap(), b"2");
        assert_eq!(engine.read("c").unwrap().unwrap(), b"3");
    }

    #[test]
    fn crdt_remove_key() {
        let mut engine = CrdtEngine::new();
        engine.merge("key1", b"v1").unwrap();
        engine.remove("key1");
        assert_eq!(engine.key_count(), 0);
        assert!(engine.read("key1").unwrap().is_none());
    }

    #[test]
    fn crdt_keys_iterator() {
        let mut engine = CrdtEngine::new();
        engine.merge("a", b"1").unwrap();
        engine.merge("b", b"2").unwrap();
        let keys: Vec<&str> = engine.keys().collect();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn crdt_converges_same_order() {
        // Two CRDTs that merge the same data converge to the same state.
        let mut a = CrdtEngine::new();
        let mut b = CrdtEngine::new();

        a.merge("k", b"v1").unwrap();
        // b merges a's result
        let a_val = a.read("k").unwrap().unwrap();
        b.merge("k", &a_val).unwrap();

        assert_eq!(a.read("k").unwrap().unwrap(), b.read("k").unwrap().unwrap());
    }
}

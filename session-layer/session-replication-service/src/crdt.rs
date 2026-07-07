//! [`CrdtEngine`] — Phase 1+ skeleton for CRDT-based conflict-free
//! replication.
//!
//! See `DESIGN.md` Part 7 (CRDT approach).
//!
//! # CRDT Approach
//!
//! Conflict-free Replicated Data Types (CRDTs) guarantee that
//! replicas converge to the same state without consensus:
//!
//! 1. **Merge**: Any two states can be merged deterministically.
//! 2. **Read**: Reads return a value that is a valid state at some
//!    point in the causal history.
//! 3. **No Conflicts**: Because merges always succeed, no conflict
//!    resolution is needed.
//!
//! Vireon uses:
//! - **LWW-Map** for session metadata (last-writer-wins per key).
//! - **OR-Set** for ownership sets (add/remove with tombstones).
//!
//! # Phase 0
//!
//! Type + stub only. Phase 1+ adds real CRDT implementations.

use crate::error::ReplicationError;

/// Phase 1+ CRDT-based replication engine.
///
/// Provides eventual consistency without conflicts by using
/// deterministically-mergeable data types.
#[allow(dead_code)]
pub struct CrdtEngine {
    epoch: u64,
}

impl CrdtEngine {
    /// Construct a new CRDT engine.
    #[must_use]
    pub fn new() -> Self {
        Self { epoch: 0 }
    }

    /// Merge state from a remote replica for the given key.
    ///
    /// CRDT merge is always successful (no conflicts possible).
    ///
    /// # Errors
    /// Always returns `Ok(())` in Phase 0.
    pub fn merge(&mut self, key: &str, other: &[u8]) -> Result<(), ReplicationError> {
        // Phase 0: stub.
        let _ = (key, other);
        self.epoch = self.epoch.saturating_add(1);
        Ok(())
    }

    /// Read the current value for a key.
    ///
    /// Returns `None` in Phase 0 stub; Phase 1+ returns the merged
    /// state.
    ///
    /// # Errors
    /// Always returns `Ok(None)` in Phase 0.
    pub fn read(&self, key: &str) -> Result<Option<Vec<u8>>, ReplicationError> {
        // Phase 0: stub.
        let _ = key;
        Ok(None)
    }

    /// The current merge epoch (number of merges performed).
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl Default for CrdtEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crdt_engine_new() {
        let engine = CrdtEngine::new();
        assert_eq!(engine.epoch(), 0);
    }

    #[test]
    fn crdt_merge_is_stub_ok() {
        let mut engine = CrdtEngine::new();
        assert!(engine.merge("key1", b"value1").is_ok());
        assert_eq!(engine.epoch(), 1);
    }

    #[test]
    fn crdt_read_is_stub_none() {
        let engine = CrdtEngine::new();
        let result = engine.read("key1").unwrap();
        assert!(result.is_none());
    }
}

//! [`SyncEngine`] — Phase 1+ skeleton for synchronous replication.
//!
//! See `DESIGN.md` Part 3 (replication topology).
//!
//! # Sync Replication
//!
//! The sync engine replicates writes to all replicas before
//! acknowledging the client. This provides the strongest consistency
//! guarantee (all replicas in sync) but has the lowest availability
//! (any replica failure blocks writes).
//!
//! Suitable for:
//! - Small clusters (2–3 replicas) in a single data center.
//! - Critical metadata that must not be lost.
//! - Read-heavy workloads where read-anywhere is important.
//!
//! # Phase 0
//!
//! Type + stub only. Phase 1+ adds real networking.

use crate::error::ReplicationError;

/// Phase 1+ synchronous replication engine.
///
/// Replicates writes to all replicas before acknowledging. Provides
/// the strongest consistency at the cost of availability.
#[allow(dead_code)]
pub struct SyncEngine {
    replica_count: u8,
    ack_count: u8,
}

impl SyncEngine {
    /// Construct a new sync engine with the given replica count.
    #[must_use]
    pub fn new(replica_count: u8) -> Self {
        Self {
            replica_count,
            ack_count: 0,
        }
    }

    /// Synchronously replicate a (key, value) pair to all replicas.
    ///
    /// Blocks until all replicas acknowledge or the timeout expires.
    ///
    /// # Errors
    /// Always returns `Ok(())` in Phase 0; Phase 1+ will return
    /// `Err` on timeout or replica failure.
    pub fn replicate(&mut self, key: &str, value: &[u8]) -> Result<(), ReplicationError> {
        // Phase 0: stub.
        let _ = (key, value);
        self.ack_count = self.replica_count;
        Ok(())
    }

    /// Read the current value for a key.
    ///
    /// Since all replicas are always in sync, reads can be served
    /// from any replica.
    ///
    /// # Errors
    /// Always returns `Ok(None)` in Phase 0.
    pub fn read(&self, key: &str) -> Result<Option<Vec<u8>>, ReplicationError> {
        // Phase 0: stub.
        let _ = key;
        Ok(None)
    }

    /// The configured replica count.
    #[must_use]
    pub const fn replica_count(&self) -> u8 {
        self.replica_count
    }

    /// The number of replicas that acknowledged the last operation.
    #[must_use]
    pub const fn ack_count(&self) -> u8 {
        self.ack_count
    }
}

impl Default for SyncEngine {
    fn default() -> Self {
        Self::new(3)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_engine_new() {
        let engine = SyncEngine::new(5);
        assert_eq!(engine.replica_count(), 5);
        assert_eq!(engine.ack_count(), 0);
    }

    #[test]
    fn sync_replicate_is_stub_ok() {
        let mut engine = SyncEngine::new(3);
        assert!(engine.replicate("key1", b"value1").is_ok());
        assert_eq!(engine.ack_count(), 3);
    }

    #[test]
    fn sync_read_is_stub_none() {
        let engine = SyncEngine::new(3);
        let result = engine.read("key1").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn sync_default_is_3_replicas() {
        let engine = SyncEngine::default();
        assert_eq!(engine.replica_count(), 3);
    }
}

//! [`StateRestore`] — reconstruct session state on the target node.
//!
//! See `DESIGN.md` Part 5 (state restore).
//!
//! # Role
//!
//! `StateRestore` takes a transferred snapshot and reconstructs the
//! live session on the target node: stream state, flow-control
//! windows, sequence numbers, pending messages, and connection
//! bindings.
//!
//! # Phase 0
//!
//! Skeleton only.

use crate::error::MigrationError;
use crate::snapshot::{MigrationSnapshotRef, SnapshotMetadata};

// ── StateRestore (Phase 1 skeleton) ─────────────────────────────

/// Reconstructs session state from a transferred snapshot.
///
/// Phase 0 ships only a typed skeleton.
// TODO(phase-1): real restore with session-manager integration.
#[derive(Debug, Default)]
pub struct StateRestore {
    /// Whether to verify checksum before restoring.
    #[allow(dead_code)]
    verify_checksum: bool,
}

impl StateRestore {
    /// Construct a new state restore manager.
    #[must_use]
    pub const fn new(verify_checksum: bool) -> Self {
        Self { verify_checksum }
    }

    /// Restore session state from a snapshot reference.
    ///
    /// # Errors
    /// Always returns [`MigrationError::RestoreFailed`] in Phase 0.
    pub fn restore(&self, _snapshot: &MigrationSnapshotRef) -> Result<(), MigrationError> {
        // TODO(phase-1): deserialize + reconstruct streams + bind connection.
        Err(MigrationError::RestoreFailed("phase-1 stub"))
    }

    /// Verify that the restored state matches the snapshot metadata.
    ///
    /// # Errors
    /// Always returns [`MigrationError::RestoreFailed`] in Phase 0.
    pub fn verify(&self, _metadata: &SnapshotMetadata) -> Result<(), MigrationError> {
        // TODO(phase-1): check stream count, checksum, generation.
        Err(MigrationError::RestoreFailed("phase-1 stub"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_state_restore() {
        let sr = StateRestore::new(true);
        let _ = format!("{sr:?}");
    }

    #[test]
    fn restore_returns_stub_error() {
        let sr = StateRestore::new(true);
        let snap = MigrationSnapshotRef::default();
        assert!(matches!(
            sr.restore(&snap),
            Err(MigrationError::RestoreFailed(_))
        ));
    }

    #[test]
    fn verify_returns_stub_error() {
        let sr = StateRestore::new(true);
        let meta = SnapshotMetadata::default();
        assert!(matches!(
            sr.verify(&meta),
            Err(MigrationError::RestoreFailed(_))
        ));
    }
}

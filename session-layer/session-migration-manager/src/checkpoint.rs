//! [`CheckpointManager`] — periodic checkpointing for crash recovery.
//!
//! See `DESIGN.md` Part 10 (checkpoint & crash recovery).
//!
//! # Role
//!
//! `CheckpointManager` periodically saves migration progress so that
//! if the coordinator crashes, the migration can be recovered and
//! resumed from the last checkpoint rather than starting over.
//!
//! # Phase 0
//!
//! Skeleton only.

use crate::error::MigrationError;
use session_manager::SessionId;

// ── CheckpointManager (Phase 1 skeleton) ────────────────────────

/// Periodic checkpoint manager for migration crash recovery.
///
/// Phase 0 ships only a typed skeleton.
// TODO(phase-1): real checkpoint with persistent storage.
#[derive(Debug, Default)]
pub struct CheckpointManager {
    /// Interval between checkpoints in milliseconds.
    #[allow(dead_code)]
    interval_ms: u64,
}

impl CheckpointManager {
    /// Construct a new checkpoint manager with the given interval.
    #[must_use]
    pub const fn new(interval_ms: u64) -> Self {
        Self { interval_ms }
    }

    /// Take a checkpoint of the migration state.
    ///
    /// Returns the checkpoint sequence number.
    ///
    /// # Errors
    /// Always returns [`MigrationError::ShuttingDown`] in Phase 0.
    pub fn checkpoint(&self, _id: SessionId) -> Result<u64, MigrationError> {
        // TODO(phase-1): persist migration record + phase to storage.
        Err(MigrationError::ShuttingDown)
    }

    /// Recover migration state from the last checkpoint.
    ///
    /// # Errors
    /// Always returns [`MigrationError::MigrationNotFound`] in Phase 0.
    pub fn recover(&self, id: SessionId) -> Result<u64, MigrationError> {
        // TODO(phase-1): load last checkpoint from storage.
        Err(MigrationError::MigrationNotFound(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_checkpoint_manager() {
        let cm = CheckpointManager::new(5_000);
        let _ = format!("{cm:?}");
    }

    #[test]
    fn checkpoint_returns_stub_error() {
        let cm = CheckpointManager::new(5_000);
        assert!(matches!(
            cm.checkpoint(SessionId::nil()),
            Err(MigrationError::ShuttingDown)
        ));
    }

    #[test]
    fn recover_returns_stub_error() {
        let cm = CheckpointManager::new(5_000);
        assert!(matches!(
            cm.recover(SessionId::nil()),
            Err(MigrationError::MigrationNotFound(_))
        ));
    }
}

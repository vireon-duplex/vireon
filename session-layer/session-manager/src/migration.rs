//! [`MigrationManager`] — coordinates QUIC connection migration.
//!
//! See `DESIGN.md` Part 9 (connection migration).
//!
//! # Migration lifecycle
//!
//! QUIC connection migration (RFC 9000 §9) lets a peer change its
//! 4-tuple (src/dst IP + port) without tearing down the connection.
//! Vireon's session layer sits above this: when the transport layer
//! reports a path change, the migration manager tracks the session-
//! level state machine around quiche's internal migration.
//!
//! # Phase 3
//!
//! Functional coordinator: tracks per-session migration metadata
//! (start, complete, abort). Actual PATH_CHALLENGE/RESPONSE and CID
//! rotation are handled by quiche internally; this module records
//! the session-layer bookkeeping so the server can query whether a
//! session is mid-migration.

use rustc_hash::FxHashMap;

use crate::error::SessionManagerError;
use crate::id::SessionId;
use connection_shard::ConnectionId;

/// State of an in-progress migration.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MigrationPhase {
    /// Migration not started.
    #[default]
    Idle,
    /// PATH_CHALLENGE sent; awaiting PATH_RESPONSE.
    ChallengeSent,
    /// Path validated; rotating CID.
    RotatingCid,
    /// New CID active; handing off reactor core.
    HandoffCore,
    /// Migration complete; session is Active again.
    Complete,
    /// Migration failed; session reverted to previous path.
    Failed,
}

/// Per-session migration metadata. Captured when migration starts
/// and discarded when it completes.
#[derive(Clone, Debug)]
pub struct MigrationMetadata {
    /// Old connection id (pre-migration).
    pub old_cid: ConnectionId,
    /// New connection id (post-migration).
    pub new_cid: ConnectionId,
    /// Old reactor core.
    pub old_core: u8,
    /// New reactor core.
    pub new_core: u8,
    /// Current migration phase.
    pub phase: MigrationPhase,
    /// Number of retries (path challenge failures trigger retries).
    pub retries: u8,
}

impl MigrationMetadata {
    /// Whether the migration is in progress.
    #[must_use]
    pub const fn in_progress(&self) -> bool {
        matches!(
            self.phase,
            MigrationPhase::ChallengeSent
                | MigrationPhase::RotatingCid
                | MigrationPhase::HandoffCore
        )
    }

    /// Whether the migration has finished (success or failure).
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self.phase,
            MigrationPhase::Complete | MigrationPhase::Failed
        )
    }
}

/// Migration manager — tracks in-flight migrations for all sessions
/// on this reactor.
pub struct MigrationManager {
    in_progress: FxHashMap<SessionId, MigrationMetadata>,
}

impl Default for MigrationManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MigrationManager {
    /// Construct a new manager.
    #[must_use]
    pub fn new() -> Self {
        Self {
            in_progress: FxHashMap::default(),
        }
    }

    /// Starts a migration for `id`, recording the old and new
    /// connection ids. The metadata's phase is set to
    /// [`MigrationPhase::ChallengeSent`].
    ///
    /// # Errors
    /// Returns [`SessionManagerError::MigrationFailed`] if a migration
    /// for `id` is already in progress.
    pub fn start_migration(
        &mut self,
        id: SessionId,
        old_cid: ConnectionId,
        new_cid: ConnectionId,
    ) -> Result<MigrationMetadata, SessionManagerError> {
        if self.in_progress.contains_key(&id) {
            return Err(SessionManagerError::MigrationFailed(
                "migration already in progress for this session",
            ));
        }
        let meta = MigrationMetadata {
            old_cid,
            new_cid,
            old_core: 0,
            new_core: 0,
            phase: MigrationPhase::ChallengeSent,
            retries: 0,
        };
        self.in_progress.insert(id, meta.clone());
        Ok(meta)
    }

    /// Completes a migration for `id`. Removes the in-progress entry
    /// and returns [`MigrationPhase::Complete`].
    ///
    /// # Errors
    /// Returns [`SessionManagerError::MigrationFailed`] if no migration
    /// is in progress for `id`.
    pub fn complete_migration(
        &mut self,
        id: SessionId,
    ) -> Result<MigrationPhase, SessionManagerError> {
        if self.in_progress.remove(&id).is_none() {
            return Err(SessionManagerError::MigrationFailed(
                "no migration in progress for this session",
            ));
        }
        Ok(MigrationPhase::Complete)
    }

    /// Aborts a migration for `id`. Removes the in-progress entry.
    /// No-op if no migration is in progress.
    pub fn abort_migration(&mut self, id: SessionId) {
        self.in_progress.remove(&id);
    }

    /// Whether a migration is in progress for `id`.
    #[must_use]
    pub fn is_migrating(&self, id: SessionId) -> bool {
        self.in_progress.contains_key(&id)
    }

    /// Returns the metadata for an in-progress migration, if any.
    #[must_use]
    pub fn metadata(&self, id: SessionId) -> Option<&MigrationMetadata> {
        self.in_progress.get(&id)
    }

    /// Number of in-progress migrations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.in_progress.len()
    }

    /// Whether there are no in-progress migrations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.in_progress.is_empty()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn cid(raw: u64) -> ConnectionId {
        ConnectionId::from_raw(raw)
    }

    #[test]
    fn phase_idle_default() {
        assert_eq!(MigrationPhase::default(), MigrationPhase::Idle);
    }

    #[test]
    fn metadata_in_progress() {
        let m = MigrationMetadata {
            old_cid: cid(1),
            new_cid: cid(2),
            old_core: 0,
            new_core: 1,
            phase: MigrationPhase::ChallengeSent,
            retries: 0,
        };
        assert!(m.in_progress());
        assert!(!m.is_terminal());
    }

    #[test]
    fn metadata_terminal() {
        let m = MigrationMetadata {
            old_cid: cid(1),
            new_cid: cid(2),
            old_core: 0,
            new_core: 1,
            phase: MigrationPhase::Complete,
            retries: 0,
        };
        assert!(!m.in_progress());
        assert!(m.is_terminal());
    }

    #[test]
    fn start_migration_records_metadata() {
        let mut mgr = MigrationManager::new();
        let id = SessionId::from_pair(1, 1);
        let meta = mgr.start_migration(id, cid(1), cid(2)).unwrap();
        assert_eq!(meta.phase, MigrationPhase::ChallengeSent);
        assert!(mgr.is_migrating(id));
        assert_eq!(mgr.len(), 1);
    }

    #[test]
    fn start_migration_rejects_duplicate() {
        let mut mgr = MigrationManager::new();
        let id = SessionId::from_pair(1, 1);
        mgr.start_migration(id, cid(1), cid(2)).unwrap();
        assert!(mgr.start_migration(id, cid(3), cid(4)).is_err());
    }

    #[test]
    fn complete_migration_clears_state() {
        let mut mgr = MigrationManager::new();
        let id = SessionId::from_pair(1, 1);
        mgr.start_migration(id, cid(1), cid(2)).unwrap();
        let phase = mgr.complete_migration(id).unwrap();
        assert_eq!(phase, MigrationPhase::Complete);
        assert!(!mgr.is_migrating(id));
        assert!(mgr.is_empty());
    }

    #[test]
    fn complete_migration_unknown_returns_error() {
        let mut mgr = MigrationManager::new();
        assert!(mgr.complete_migration(SessionId::nil()).is_err());
    }

    #[test]
    fn abort_migration_clears_state() {
        let mut mgr = MigrationManager::new();
        let id = SessionId::from_pair(1, 1);
        mgr.start_migration(id, cid(1), cid(2)).unwrap();
        mgr.abort_migration(id);
        assert!(!mgr.is_migrating(id));
    }

    #[test]
    fn abort_migration_unknown_is_noop() {
        let mut mgr = MigrationManager::new();
        mgr.abort_migration(SessionId::nil()); // should not panic
    }

    #[test]
    fn metadata_lookup() {
        let mut mgr = MigrationManager::new();
        let id = SessionId::from_pair(1, 1);
        mgr.start_migration(id, cid(10), cid(20)).unwrap();
        let meta = mgr.metadata(id).unwrap();
        assert_eq!(meta.old_cid, cid(10));
        assert_eq!(meta.new_cid, cid(20));
    }
}

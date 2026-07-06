//! [`MigrationManager`] — coordinates QUIC connection migration.
//!
//! See `DESIGN.md` Part 9 (connection migration).
//!
//! # Migration lifecycle
//!
//! QUIC connection migration (RFC 9000 §9) lets a peer change its
//! 4-tuple (src/dst IP + port) without tearing down the connection.
//! Vireon's session layer sits above this: when the transport layer
//! reports a path change, the migration manager:
//!
//! 1. **Validates** — runs `PATH_CHALLENGE` / `PATH_RESPONSE`
//!    (RFC 9000 §8.2).
//! 2. **Rotates CID** — switches to a new connection id to prevent
//!    linkability (RFC 9000 §7.5 / QUIC-LB RFC 9276).
//! 3. **Hands off reactor** — if the new path prefers a different
//!    core (NUMA locality), the session moves between reactor
//!    threads.
//! 4. **Resumes** — transitions the session back to `Active`.
//!
//! # Phase 0 status
//!
//! Phase 0 ships the metadata shape + type signature. Functional
//! migration lands in Phase 3+.

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

/// Migration manager. Owns in-flight migration state for all
/// sessions on this reactor.
///
/// Phase 0 holds no state; Phase 3 will add the per-session map.
// TODO(phase-3): per-session migration map + path challenge state.
#[derive(Clone, Debug, Default)]
pub struct MigrationManager;

impl MigrationManager {
    /// Construct a new manager.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Phase-3 `start_migration` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::MigrationFailed`] in
    /// Phase 0.
    pub fn start_migration(
        &mut self,
        _id: SessionId,
        _old_cid: ConnectionId,
        _new_cid: ConnectionId,
    ) -> Result<MigrationMetadata, SessionManagerError> {
        // TODO(phase-3): send PATH_CHALLENGE, install metadata.
        Err(SessionManagerError::MigrationFailed(
            "migration manager phase-3 stub",
        ))
    }

    /// Phase-3 `complete_migration` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::MigrationFailed`] in
    /// Phase 0.
    pub fn complete_migration(
        &mut self,
        _id: SessionId,
    ) -> Result<MigrationPhase, SessionManagerError> {
        // TODO(phase-3): rotate CID, handoff core, transition session.
        Err(SessionManagerError::MigrationFailed(
            "migration manager phase-3 stub",
        ))
    }

    /// Phase-3 `abort_migration` placeholder.
    pub fn abort_migration(&mut self, _id: SessionId) {
        // TODO(phase-3): revert path, clear metadata.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_idle_default() {
        assert_eq!(MigrationPhase::default(), MigrationPhase::Idle);
    }

    #[test]
    fn metadata_in_progress() {
        let m = MigrationMetadata {
            old_cid: ConnectionId::from_raw(1),
            new_cid: ConnectionId::from_raw(2),
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
            old_cid: ConnectionId::from_raw(1),
            new_cid: ConnectionId::from_raw(2),
            old_core: 0,
            new_core: 1,
            phase: MigrationPhase::Complete,
            retries: 0,
        };
        assert!(!m.in_progress());
        assert!(m.is_terminal());
    }

    #[test]
    fn start_migration_phase0_stub() {
        let mut m = MigrationManager::new();
        assert!(
            m.start_migration(
                SessionId::nil(),
                ConnectionId::from_raw(1),
                ConnectionId::from_raw(2),
            )
            .is_err()
        );
    }
}

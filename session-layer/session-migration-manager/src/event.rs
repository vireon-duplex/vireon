//! [`MigrationEvent`] — domain events emitted by the migration
//! coordinator.
//!
//! See `DESIGN.md` Part 12 (observability) and Part 2 (lifecycle).
//!
//! # Role
//!
//! The coordinator publishes events for every meaningful state
//! transition during migration. Subscribers (router, replication
//! service, metrics pipeline) react to these events without polling.
//!
//! # Phase 0
//!
//! Type only — no event bus. Phase 1+ adds a broadcast channel.

use crate::phase::MigrationPhase;
use crate::snapshot::SnapshotMetadata;
use session_manager::SessionId;
use session_registry::{Generation, NodeId};

/// A domain event emitted by the migration coordinator.
///
/// Events are the **only** push-based notification channel the
/// coordinator offers.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum MigrationEvent {
    /// A migration has been initiated.
    Initiated {
        /// The session being migrated.
        session: SessionId,
        /// The source node.
        source: NodeId,
        /// The target node.
        target: NodeId,
        /// The generation at initiation.
        generation: Generation,
    },
    /// A snapshot was captured on the source node.
    SnapshotCaptured {
        /// The session being migrated.
        session: SessionId,
        /// Snapshot metadata.
        metadata: SnapshotMetadata,
    },
    /// The state was transferred to the target node.
    Transferred {
        /// The session being migrated.
        session: SessionId,
        /// The transfer handle.
        handle: u64,
        /// Bytes transferred.
        bytes: u64,
    },
    /// The state was restored on the target node.
    Restored {
        /// The session being migrated.
        session: SessionId,
        /// The target node.
        target: NodeId,
    },
    /// The migration was finalized successfully.
    Finalized {
        /// The session that was migrated.
        session: SessionId,
        /// The new owning node.
        owner: NodeId,
        /// The new ownership generation.
        generation: Generation,
    },
    /// The migration was aborted (operator or policy).
    Aborted {
        /// The session whose migration was aborted.
        session: SessionId,
        /// The reason for abort.
        reason: &'static str,
    },
    /// The migration failed.
    Failed {
        /// The session whose migration failed.
        session: SessionId,
        /// The phase at which the failure occurred.
        phase: MigrationPhase,
        /// The error message.
        reason: &'static str,
    },
}

impl MigrationEvent {
    /// Human-readable category name for the event.
    #[must_use]
    pub const fn category(&self) -> &'static str {
        match self {
            Self::Initiated { .. } => "initiated",
            Self::SnapshotCaptured { .. } => "snapshot",
            Self::Transferred { .. } => "transfer",
            Self::Restored { .. } => "restore",
            Self::Finalized { .. } => "finalize",
            Self::Aborted { .. } => "abort",
            Self::Failed { .. } => "fail",
        }
    }

    /// Whether the event indicates a terminal migration outcome.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Finalized { .. } | Self::Aborted { .. } | Self::Failed { .. }
        )
    }

    /// Whether the event indicates a successful migration.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Finalized { .. })
    }

    /// The session this event pertains to.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        match self {
            Self::Initiated { session, .. }
            | Self::SnapshotCaptured { session, .. }
            | Self::Transferred { session, .. }
            | Self::Restored { session, .. }
            | Self::Finalized { session, .. }
            | Self::Aborted { session, .. }
            | Self::Failed { session, .. } => *session,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn category_labels() {
        let e = MigrationEvent::Initiated {
            session: SessionId::nil(),
            source: NodeId::new(1),
            target: NodeId::new(2),
            generation: Generation::INITIAL,
        };
        assert_eq!(e.category(), "initiated");

        let e = MigrationEvent::Finalized {
            session: SessionId::nil(),
            owner: NodeId::new(2),
            generation: Generation::INITIAL,
        };
        assert_eq!(e.category(), "finalize");
    }

    #[test]
    fn terminal_events() {
        let finalized = MigrationEvent::Finalized {
            session: SessionId::nil(),
            owner: NodeId::new(2),
            generation: Generation::INITIAL,
        };
        assert!(finalized.is_terminal());
        assert!(finalized.is_success());

        let aborted = MigrationEvent::Aborted {
            session: SessionId::nil(),
            reason: "operator",
        };
        assert!(aborted.is_terminal());
        assert!(!aborted.is_success());

        let failed = MigrationEvent::Failed {
            session: SessionId::nil(),
            phase: MigrationPhase::Transferred,
            reason: "timeout",
        };
        assert!(failed.is_terminal());
        assert!(!failed.is_success());

        let initiated = MigrationEvent::Initiated {
            session: SessionId::nil(),
            source: NodeId::new(1),
            target: NodeId::new(2),
            generation: Generation::INITIAL,
        };
        assert!(!initiated.is_terminal());
    }

    #[test]
    fn session_accessor() {
        let sid = SessionId::from_pair(42, 99);
        let e = MigrationEvent::Restored {
            session: sid,
            target: NodeId::new(2),
        };
        assert_eq!(e.session(), sid);
    }

    #[test]
    fn event_can_be_cloned() {
        let e = MigrationEvent::Aborted {
            session: SessionId::nil(),
            reason: "test",
        };
        let cloned = e.clone();
        let _ = format!("{cloned:?}");
    }
}

//! [`SessionEvent`] — events the Session Manager emits to the
//! application layer.
//!
//! See `DESIGN.md` Part 4 (lifecycle) and Part 15 (observability).
//!
//! The Session Manager is the single source of truth for "what
//! happened to this session". It emits a typed event whenever the
//! application needs to react (handshake complete, migration, idle,
//! quota exceeded, …). Adjacent-layer signals (TLS handshake,
//! QUIC CONNECTION_CLOSE, scheduler decisions) are translated into
//! these high-level events.
//!
//! # Phase 0 status
//!
//! Phase 0 ships the event taxonomy. Functional emission (queueing
//! to the application's `mpsc`) lands in Phase 1 once
//! [`crate::manager::SessionManager`] is wired up.

use crate::id::SessionId;
use crate::priority::SessionClass;
use crate::state::SessionState;

/// Reason a session closed. Carried on [`SessionEvent::Closed`].
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionCloseReason {
    /// Application requested graceful close.
    ApplicationClose,
    /// Idle timeout fired (RFC 9000 §10.2).
    IdleTimeout,
    /// Handshake timeout fired.
    HandshakeTimeout,
    /// QUIC CONNECTION_CLOSE received from the peer.
    PeerConnectionClose(u64),
    /// Local transport reported a connection error.
    LocalConnectionError,
    /// Authentication failure (bad credentials, expired token).
    AuthenticationFailed,
    /// Authorization denial (RBAC/ABAC policy).
    AuthorizationDenied,
    /// Resource quota exceeded (memory/streams/bandwidth).
    QuotaExceeded,
    /// Manager-initiated eviction (capacity pressure).
    Evicted,
    /// Fatal internal error.
    InternalError,
}

/// An event on a session, emitted to the application layer.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionEvent {
    /// A new session was created (slot reserved, handshake in
    /// progress).
    Created {
        /// The new session's id.
        id: SessionId,
        /// Initial class assignment.
        class: SessionClass,
    },
    /// Authentication completed successfully.
    Authenticated {
        /// The session that authenticated.
        id: SessionId,
    },
    /// Session activated (transitioned Ready → Active).
    Activated {
        /// The activated session.
        id: SessionId,
    },
    /// Session transitioned to Idle (no recent activity).
    IdleTimeout {
        /// The idle session.
        id: SessionId,
    },
    /// Session is migrating to a new connection (QUIC path
    /// migration or CID rotation).
    Migrating {
        /// The session being migrated.
        id: SessionId,
    },
    /// Migration completed successfully; session is Active again.
    MigrationComplete {
        /// The migrated session.
        id: SessionId,
    },
    /// Session is draining outstanding data prior to close.
    Draining {
        /// The draining session.
        id: SessionId,
    },
    /// Session fully closed.
    Closed {
        /// The closed session.
        id: SessionId,
        /// Why it closed.
        reason: SessionCloseReason,
        /// Combined state at close time (for diagnostics).
        final_state: SessionState,
    },
    /// Session evicted by the manager (capacity pressure).
    Evicted {
        /// The evicted session.
        id: SessionId,
    },
    /// Quota exceeded (recorded for backpressure signalling).
    QuotaExceeded {
        /// The session that hit a quota cap.
        id: SessionId,
    },
    /// Session resumed via 0-RTT (RFC 9001 §2.3).
    Resumed {
        /// The resumed session.
        id: SessionId,
    },
}

impl SessionEvent {
    /// The session id this event refers to.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        match self {
            Self::Created { id, .. }
            | Self::Authenticated { id }
            | Self::Activated { id }
            | Self::IdleTimeout { id }
            | Self::Migrating { id }
            | Self::MigrationComplete { id }
            | Self::Draining { id }
            | Self::Closed { id, .. }
            | Self::Evicted { id }
            | Self::QuotaExceeded { id }
            | Self::Resumed { id } => *id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_id_extractor() {
        let id = SessionId::from_pair(1, 2);
        assert_eq!(
            SessionEvent::Created {
                id,
                class: SessionClass::default()
            }
            .session_id(),
            id
        );
        assert_eq!(SessionEvent::Authenticated { id }.session_id(), id);
        assert_eq!(
            SessionEvent::Closed {
                id,
                reason: SessionCloseReason::IdleTimeout,
                final_state: SessionState::Closed
            }
            .session_id(),
            id
        );
    }

    #[test]
    fn close_reason_variants() {
        assert_eq!(
            SessionCloseReason::ApplicationClose,
            SessionCloseReason::ApplicationClose
        );
        assert_ne!(
            SessionCloseReason::PeerConnectionClose(1),
            SessionCloseReason::PeerConnectionClose(2)
        );
    }
}

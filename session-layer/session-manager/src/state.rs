//! [`SessionState`] — 10-state session lifecycle FSM.
//!
//! See `DESIGN.md` Part 3 (session state machine) and Part 4
//! (lifecycle).
//!
//! # FSM overview
//!
//! ```text
//!   ┌──────────┐  handshake    ┌──────────────────┐  auth ok   ┌────────┐
//!   │ Creating │ ────────────► │ Authenticating   │ ─────────► │ Ready  │
//!   └──────────┘               └──────────────────┘            └────┬───┘
//!        │                            │ auth fail                   │ activate
//!        │ timeout                    ▼                             ▼
//!        ▼                      ┌────────┐           ┌─────────┐  ┌────────┐
//!   ┌──────────┐                │ Closed │           │  Idle   │◄─┤ Active │
//!   │ Destroyed│                └────────┘           └────┬────┘  └───┬────┘
//!   └──────────┘                                     timeout│           │ migrate
//!         ▲                                                ▼           ▼
//!         │                                          ┌────────┐  ┌──────────┐
//!         │◄──── close ── ┌─────────┐  ┌─────────┐   │ Active │  │Migrating │
//!         │               │ Closing │◄─┤Draining │   └────────┘  └────┬─────┘
//!         │               └────┬────┘  └─────────┘                     │
//!         └────────────────────┘                                       │ ok
//!                                                                      ▼
//!                                                                 ┌────────┐
//!                                                                 │ Active │
//!                                                                 └────────┘
//! ```
//!
//! # Key invariants
//!
//! - **Creating** is the initial state; no connection is bound yet.
//! - **Authenticating** blocks until credentials are validated.
//! - **Ready** means authenticated but not yet activated by the app.
//! - **Active** ↔ **Idle** transitions are driven by activity timers.
//! - **Migrating** preserves session continuity across connection
//!   changes.
//! - **Closed** → **Destroyed** is the only path to slot reclamation.

/// Session lifecycle state (10 states).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SessionState {
    /// Slot reserved; connection not yet bound or handshake in
    /// progress.
    #[default]
    Creating,
    /// Connection established; authentication in progress.
    Authenticating,
    /// Authenticated; awaiting application activation.
    Ready,
    /// Fully active; application can send/receive.
    Active,
    /// No recent activity; idle timer running.
    Idle,
    /// Connection migration in progress (QUIC path change or CID
    /// rotation).
    Migrating,
    /// Graceful shutdown in progress; outstanding data draining.
    Draining,
    /// Drain complete; sending CONNECTION_CLOSE.
    Closing,
    /// Connection closed; awaiting final cleanup.
    Closed,
    /// Slot reclaimed; all resources released.
    Destroyed,
}

impl SessionState {
    /// Whether the state is terminal (no further transitions).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Destroyed)
    }

    /// Whether the session can accept application data (send/recv).
    #[must_use]
    pub const fn can_accept_data(self) -> bool {
        matches!(self, Self::Active | Self::Idle | Self::Migrating)
    }

    /// Whether the session is fully active (post-handshake,
    /// post-auth).
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active | Self::Idle | Self::Migrating)
    }

    /// Whether the session still requires authentication to
    /// progress.
    #[must_use]
    pub const fn requires_authentication(self) -> bool {
        matches!(self, Self::Creating | Self::Authenticating)
    }

    /// Whether the session can initiate or accept migration.
    #[must_use]
    pub const fn can_migrate(self) -> bool {
        matches!(self, Self::Active | Self::Idle)
    }

    /// Whether the session is in a closing / draining phase.
    #[must_use]
    pub const fn is_closing(self) -> bool {
        matches!(self, Self::Draining | Self::Closing | Self::Closed)
    }

    /// Whether the session can be resumed (0-RTT reconnect).
    #[must_use]
    pub const fn is_resumable(self) -> bool {
        matches!(self, Self::Idle | Self::Closed)
    }

    /// Whether a transition from `self` to `target` is legal per
    /// the FSM.
    #[must_use]
    pub const fn can_transition_to(self, target: Self) -> bool {
        use SessionState as S;
        match (self, target) {
            // Creating → Authenticating | Closed | Destroyed
            (S::Creating, S::Authenticating | S::Closed | S::Destroyed) => true,
            // Authenticating → Ready | Closed | Destroyed
            (S::Authenticating, S::Ready | S::Closed | S::Destroyed) => true,
            // Ready → Active | Closed | Destroyed
            (S::Ready, S::Active | S::Closed | S::Destroyed) => true,
            // Active → Idle | Migrating | Draining | Closed | Destroyed
            (S::Active, S::Idle | S::Migrating | S::Draining | S::Closed | S::Destroyed) => true,
            // Idle → Active | Migrating | Draining | Closed | Destroyed
            (S::Idle, S::Active | S::Migrating | S::Draining | S::Closed | S::Destroyed) => true,
            // Migrating → Active | Draining | Closed | Destroyed
            (S::Migrating, S::Active | S::Draining | S::Closed | S::Destroyed) => true,
            // Draining → Closing | Closed | Destroyed
            (S::Draining, S::Closing | S::Closed | S::Destroyed) => true,
            // Closing → Closed | Destroyed
            (S::Closing, S::Closed | S::Destroyed) => true,
            // Closed → Destroyed
            (S::Closed, S::Destroyed) => true,
            // Destroyed is terminal
            (S::Destroyed, _) => false,
            // All other transitions are illegal
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_creating() {
        assert_eq!(SessionState::default(), SessionState::Creating);
    }

    #[test]
    fn terminal_state() {
        assert!(SessionState::Destroyed.is_terminal());
        assert!(!SessionState::Active.is_terminal());
    }

    #[test]
    fn can_accept_data_predicates() {
        assert!(SessionState::Active.can_accept_data());
        assert!(SessionState::Idle.can_accept_data());
        assert!(SessionState::Migrating.can_accept_data());
        assert!(!SessionState::Creating.can_accept_data());
        assert!(!SessionState::Authenticating.can_accept_data());
        assert!(!SessionState::Draining.can_accept_data());
    }

    #[test]
    fn requires_authentication() {
        assert!(SessionState::Creating.requires_authentication());
        assert!(SessionState::Authenticating.requires_authentication());
        assert!(!SessionState::Active.requires_authentication());
    }

    #[test]
    fn can_migrate() {
        assert!(SessionState::Active.can_migrate());
        assert!(SessionState::Idle.can_migrate());
        assert!(!SessionState::Creating.can_migrate());
        assert!(!SessionState::Migrating.can_migrate());
    }

    #[test]
    fn legal_transitions() {
        use SessionState as S;
        assert!(S::Creating.can_transition_to(S::Authenticating));
        assert!(S::Authenticating.can_transition_to(S::Ready));
        assert!(S::Ready.can_transition_to(S::Active));
        assert!(S::Active.can_transition_to(S::Idle));
        assert!(S::Idle.can_transition_to(S::Active));
        assert!(S::Active.can_transition_to(S::Migrating));
        assert!(S::Migrating.can_transition_to(S::Active));
        assert!(S::Active.can_transition_to(S::Draining));
        assert!(S::Draining.can_transition_to(S::Closing));
        assert!(S::Closing.can_transition_to(S::Closed));
        assert!(S::Closed.can_transition_to(S::Destroyed));
    }

    #[test]
    fn illegal_transitions() {
        use SessionState as S;
        assert!(!S::Creating.can_transition_to(S::Active));
        assert!(!S::Authenticating.can_transition_to(S::Active));
        assert!(!S::Destroyed.can_transition_to(S::Active));
        assert!(!S::Active.can_transition_to(S::Creating));
        assert!(!S::Closed.can_transition_to(S::Active));
    }

    #[test]
    fn resumable_states() {
        assert!(SessionState::Idle.is_resumable());
        assert!(SessionState::Closed.is_resumable());
        assert!(!SessionState::Active.is_resumable());
    }
}

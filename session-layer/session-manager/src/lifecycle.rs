//! [`SessionLifecycle`] — FSM transition validator.
//!
//! See `DESIGN.md` Part 4 (session lifecycle) and Part 3 (FSM).
//!
//! # Lifecycle overview
//!
//! ```text
//!   create ─► Authenticating ─► Ready ─► Active ◄─► Idle
//!                                          │
//!                                          ├─► Migrating ─► Active
//!                                          └─► Draining ─► Closing ─► Closed ─► Destroyed
//! ```
//!
//! The lifecycle controller is a **stateless** validator: it inspects
//! the current [`SessionState`] and returns the next state if the
//! transition is legal, or an error otherwise. The caller (typically
//! [`SessionManager`](crate::manager::SessionManager)) applies the
//! returned state to the [`Session`](crate::session::Session).

use crate::error::SessionManagerError;
use crate::state::SessionState;

/// Stateless lifecycle transition validator.
///
/// Owns no per-session data — the caller supplies the current state
/// and the desired action; the validator returns the new state or an
/// error. This keeps the FSM rules in one place and lets the manager
/// decide how / when to persist the transition.
#[derive(Clone, Debug, Default)]
pub struct SessionLifecycle;

impl SessionLifecycle {
    /// Construct a new lifecycle controller.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Returns the initial state for a newly-created session.
    #[must_use]
    pub const fn create(&self) -> SessionState {
        SessionState::Creating
    }

    /// Advances a session one step along the activation chain:
    ///
    /// `Creating → Authenticating → Ready → Active`
    ///
    /// Calling `activate` on `Active` or `Idle` is idempotent (returns
    /// `Active`). This lets the manager call `activate` unconditionally
    /// after each handshake stage without tracking where in the chain
    /// the session currently sits.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::ResourceExhausted`] if `current`
    /// cannot transition toward `Active` (e.g. already draining).
    pub fn activate(&self, current: SessionState) -> Result<SessionState, SessionManagerError> {
        match current {
            SessionState::Creating => Ok(SessionState::Authenticating),
            SessionState::Authenticating => Ok(SessionState::Ready),
            SessionState::Ready => Ok(SessionState::Active),
            SessionState::Active | SessionState::Idle => Ok(SessionState::Active),
            _ => Err(SessionManagerError::ResourceExhausted(
                "cannot activate from current state",
            )),
        }
    }

    /// Transitions a session to `Draining`.
    ///
    /// Valid from `Active`, `Idle`, or `Migrating`. Idempotent on
    /// `Draining`.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::ResourceExhausted`] if the
    /// session is already closed or destroyed.
    pub fn drain(&self, current: SessionState) -> Result<SessionState, SessionManagerError> {
        match current {
            SessionState::Active
            | SessionState::Idle
            | SessionState::Migrating
            | SessionState::Draining => Ok(SessionState::Draining),
            _ => Err(SessionManagerError::ResourceExhausted(
                "cannot drain from current state",
            )),
        }
    }

    /// Transitions a session to `Destroyed` (terminal).
    ///
    /// Valid from any non-`Destroyed` state.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::ResourceExhausted`] if the
    /// session is already `Destroyed`.
    pub fn destroy(&self, current: SessionState) -> Result<SessionState, SessionManagerError> {
        match current {
            SessionState::Destroyed => Err(SessionManagerError::ResourceExhausted(
                "session already destroyed",
            )),
            _ => Ok(SessionState::Destroyed),
        }
    }

    /// Transitions a session to `Migrating`.
    ///
    /// Valid from `Active` or `Idle`. The caller later calls
    /// [`activate`](Self::activate) to return to `Active` once the
    /// migration completes.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::MigrationFailed`] if the session
    /// is already migrating or in a non-migratable state.
    pub fn migrate(&self, current: SessionState) -> Result<SessionState, SessionManagerError> {
        match current {
            SessionState::Active | SessionState::Idle => Ok(SessionState::Migrating),
            SessionState::Migrating => Err(SessionManagerError::MigrationFailed(
                "migration already in progress",
            )),
            _ => Err(SessionManagerError::MigrationFailed(
                "cannot migrate from current state",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn create_returns_creating() {
        let l = SessionLifecycle::new();
        assert_eq!(l.create(), SessionState::Creating);
    }

    #[test]
    fn activate_chains_through_to_active() {
        let l = SessionLifecycle::new();
        let s = l.activate(SessionState::Creating).unwrap();
        assert_eq!(s, SessionState::Authenticating);
        let s = l.activate(s).unwrap();
        assert_eq!(s, SessionState::Ready);
        let s = l.activate(s).unwrap();
        assert_eq!(s, SessionState::Active);
    }

    #[test]
    fn activate_idempotent_on_active() {
        let l = SessionLifecycle::new();
        assert_eq!(l.activate(SessionState::Active).unwrap(), SessionState::Active);
        assert_eq!(l.activate(SessionState::Idle).unwrap(), SessionState::Active);
    }

    #[test]
    fn activate_rejects_draining() {
        let l = SessionLifecycle::new();
        assert!(l.activate(SessionState::Draining).is_err());
    }

    #[test]
    fn drain_from_active() {
        let l = SessionLifecycle::new();
        assert_eq!(l.drain(SessionState::Active).unwrap(), SessionState::Draining);
        assert_eq!(l.drain(SessionState::Idle).unwrap(), SessionState::Draining);
        assert_eq!(l.drain(SessionState::Migrating).unwrap(), SessionState::Draining);
    }

    #[test]
    fn drain_idempotent() {
        let l = SessionLifecycle::new();
        assert_eq!(l.drain(SessionState::Draining).unwrap(), SessionState::Draining);
    }

    #[test]
    fn drain_rejects_closed() {
        let l = SessionLifecycle::new();
        assert!(l.drain(SessionState::Closed).is_err());
        assert!(l.drain(SessionState::Destroyed).is_err());
    }

    #[test]
    fn destroy_from_any_non_terminal() {
        let l = SessionLifecycle::new();
        assert_eq!(l.destroy(SessionState::Active).unwrap(), SessionState::Destroyed);
        assert_eq!(l.destroy(SessionState::Draining).unwrap(), SessionState::Destroyed);
        assert_eq!(l.destroy(SessionState::Closed).unwrap(), SessionState::Destroyed);
    }

    #[test]
    fn destroy_rejects_already_destroyed() {
        let l = SessionLifecycle::new();
        assert!(l.destroy(SessionState::Destroyed).is_err());
    }

    #[test]
    fn migrate_from_active() {
        let l = SessionLifecycle::new();
        assert_eq!(l.migrate(SessionState::Active).unwrap(), SessionState::Migrating);
        assert_eq!(l.migrate(SessionState::Idle).unwrap(), SessionState::Migrating);
    }

    #[test]
    fn migrate_rejects_already_migrating() {
        let l = SessionLifecycle::new();
        assert!(l.migrate(SessionState::Migrating).is_err());
    }

    #[test]
    fn migrate_rejects_creating() {
        let l = SessionLifecycle::new();
        assert!(l.migrate(SessionState::Creating).is_err());
    }
}

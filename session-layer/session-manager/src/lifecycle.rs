//! [`SessionLifecycle`] — FSM controller.
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
//! The lifecycle controller wraps the [`SessionState`](crate::state::SessionState)
//! FSM with side-effects (stats bumps, slot release, event
//! emission). The FSM itself is the source of truth for valid
//! transitions; this module sequences them.
//!
//! # Phase 0 status
//!
//! Phase 0 ships only the controller placeholder. Functional
//! transitions land in Phase 3+.

use crate::error::SessionManagerError;
use crate::id::SessionId;
use crate::state::SessionState;

/// Lifecycle controller. Phase 0 placeholder.
///
/// Phase 3+ will own the FSM transition rules, the idle-timeout
/// wheel, the drain queue, and the slot reclamation.
// TODO(phase-3): full transition machinery.
#[derive(Clone, Debug, Default)]
pub struct SessionLifecycle;

impl SessionLifecycle {
    /// Construct a new lifecycle controller.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Phase-3 `create` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn create(&mut self, _id: SessionId) -> Result<SessionState, SessionManagerError> {
        // TODO(phase-3): reserve slot, insert into table, bump stats.
        Err(SessionManagerError::ResourceExhausted(
            "lifecycle phase-3 stub",
        ))
    }

    /// Phase-3 `activate` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn activate(&mut self, id: SessionId) -> Result<SessionState, SessionManagerError> {
        // TODO(phase-3): transition Ready → Active.
        Err(SessionManagerError::SessionNotFound(id))
    }

    /// Phase-3 `migrate` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::MigrationFailed`] in
    /// Phase 0.
    pub fn migrate(&mut self, _id: SessionId) -> Result<SessionState, SessionManagerError> {
        // TODO(phase-3): transition Active → Migrating.
        Err(SessionManagerError::MigrationFailed(
            "lifecycle phase-3 stub",
        ))
    }

    /// Phase-3 `drain` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn drain(&mut self, id: SessionId) -> Result<SessionState, SessionManagerError> {
        // TODO(phase-3): transition to Draining, flush outstanding.
        Err(SessionManagerError::SessionNotFound(id))
    }

    /// Phase-3 `destroy` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn destroy(&mut self, id: SessionId) -> Result<SessionState, SessionManagerError> {
        // TODO(phase-3): transition to Destroyed, release slot.
        Err(SessionManagerError::SessionNotFound(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_phase0_stub() {
        let mut l = SessionLifecycle::new();
        assert!(matches!(
            l.create(SessionId::nil()),
            Err(SessionManagerError::ResourceExhausted(_))
        ));
    }

    #[test]
    fn activate_phase0_stub() {
        let mut l = SessionLifecycle::new();
        assert!(matches!(
            l.activate(SessionId::nil()),
            Err(SessionManagerError::SessionNotFound(_))
        ));
    }

    #[test]
    fn migrate_phase0_stub() {
        let mut l = SessionLifecycle::new();
        assert!(matches!(
            l.migrate(SessionId::nil()),
            Err(SessionManagerError::MigrationFailed(_))
        ));
    }

    #[test]
    fn drain_and_destroy_phase0_stubs() {
        let mut l = SessionLifecycle::new();
        assert!(l.drain(SessionId::nil()).is_err());
        assert!(l.destroy(SessionId::nil()).is_err());
    }
}

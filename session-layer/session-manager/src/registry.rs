//! [`SessionRegistry`] — local per-reactor lookup.
//!
//! See `DESIGN.md` Part 6 (session registry).
//!
//! # Role
//!
//! One registry per reactor core. Wraps the
//! [`SessionTable`](crate::table::SessionTable) with id-based
//! lookup and provides a clean register/unregister/lookup API.
//!
//! Phase 6+ adds a global / distributed registry (lease-based, gossip
//! reconciliation, perfect hashing for hot-path resolution).

use crate::config::SessionManagerConfig;
use crate::error::SessionManagerError;
use crate::id::SessionId;
use crate::session::Session;
use crate::table::{SessionTable, SlotHandle};

/// Per-reactor session registry.
///
/// Wraps [`SessionTable`] with a facade that exposes register / unregister /
/// lookup by [`SessionId`], plus pass-through accessors for the underlying
/// table.
pub struct SessionRegistry {
    cfg: SessionManagerConfig,
    table: SessionTable,
}

impl SessionRegistry {
    /// Construct a registry from a validated config.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::InvalidConfig`] if validation
    /// fails.
    pub fn new(cfg: SessionManagerConfig) -> Result<Self, SessionManagerError> {
        cfg.validate()?;
        Ok(Self {
            table: SessionTable::new(cfg),
            cfg,
        })
    }

    /// Borrow the config.
    #[must_use]
    pub const fn config(&self) -> &SessionManagerConfig {
        &self.cfg
    }

    /// Current occupancy of the registry's session table.
    #[must_use]
    pub const fn session_count(&self) -> u32 {
        self.table.occupancy()
    }

    /// Whether the registry is at capacity.
    #[must_use]
    pub const fn is_full(&self) -> bool {
        self.table.is_full()
    }

    /// Register a new [`Session`] — inserts it into the table and
    /// indexes it by `session.id`.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::ResourceExhausted`] if the table
    /// is at `max_capacity`.
    /// Returns [`SessionManagerError::InvalidConfig`] if the session
    /// has a nil or duplicate id.
    pub fn register(&mut self, session: Session) -> Result<SlotHandle, SessionManagerError> {
        self.table.insert(session)
    }

    /// Unregister a session by id — removes it from the table.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::SessionNotFound`] if the id is
    /// not registered.
    pub fn unregister(&mut self, id: SessionId) -> Result<(), SessionManagerError> {
        let _ = self.table.remove(id)?;
        Ok(())
    }

    /// Look up a session by id, returning its [`SlotHandle`].
    ///
    /// # Errors
    /// Returns [`SessionManagerError::SessionNotFound`] if not found.
    pub fn lookup(&self, id: SessionId) -> Result<SlotHandle, SessionManagerError> {
        self.table.get(id)
    }

    /// Borrow the [`Session`] at `handle`.
    #[must_use]
    pub fn get_session(&self, handle: SlotHandle) -> Option<&Session> {
        self.table.get_session(handle)
    }

    /// Mutably borrow the [`Session`] at `handle`.
    pub fn get_session_mut(&mut self, handle: SlotHandle) -> Option<&mut Session> {
        self.table.get_session_mut(handle)
    }

    /// Borrow the underlying table.
    #[must_use]
    pub const fn table(&self) -> &SessionTable {
        &self.table
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::priority::SessionClass;

    fn make_session(hi: u64) -> Session {
        Session::new(
            SessionId::from_pair(hi, hi * 2),
            SessionClass::default(),
            0,
        )
    }

    #[test]
    fn new_validates_config() {
        let r = SessionRegistry::new(SessionManagerConfig::default()).expect("default ok");
        assert_eq!(r.session_count(), 0);
        assert!(!r.is_full());
    }

    #[test]
    fn new_rejects_invalid_config() {
        let bad = SessionManagerConfig {
            max_sessions: 0,
            ..SessionManagerConfig::default()
        };
        assert!(SessionRegistry::new(bad).is_err());
    }

    #[test]
    fn register_then_lookup() {
        let mut r = SessionRegistry::new(SessionManagerConfig::default()).unwrap();
        let session = make_session(1);
        let id = session.id;

        let handle = r.register(session).unwrap();
        assert_eq!(r.session_count(), 1);

        let looked_up = r.lookup(id).unwrap();
        assert_eq!(looked_up, handle);

        assert_eq!(r.get_session(handle).unwrap().id, id);
    }

    #[test]
    fn unregister_removes_session() {
        let mut r = SessionRegistry::new(SessionManagerConfig::default()).unwrap();
        let session = make_session(1);
        let id = session.id;

        r.register(session).unwrap();
        assert_eq!(r.session_count(), 1);

        r.unregister(id).unwrap();
        assert_eq!(r.session_count(), 0);
        assert!(r.lookup(id).is_err());
    }

    #[test]
    fn unregister_unknown_returns_error() {
        let mut r = SessionRegistry::new(SessionManagerConfig::default()).unwrap();
        assert!(r.unregister(SessionId::from_pair(1, 1)).is_err());
    }

    #[test]
    fn lookup_unknown_returns_error() {
        let r = SessionRegistry::new(SessionManagerConfig::default()).unwrap();
        assert!(r.lookup(SessionId::from_pair(1, 1)).is_err());
    }
}

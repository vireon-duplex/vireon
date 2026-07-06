//! [`SessionRegistry`] — local per-reactor lookup.
//!
//! See `DESIGN.md` Part 6 (session registry).
//!
//! # Role
//!
//! One registry per reactor core. Wraps the
//! [`SessionTable`](crate::table::SessionTable) with id-based
//! lookup, GC of expired sessions, and lease/TTL management.
//!
//! Phase 6+ adds a global / distributed registry (lease-based, gossip
//! reconciliation, perfect hashing for hot-path resolution).
//!
//! # Phase 0 status
//!
//! Phase 0 ships the facade shape. Functional lookup, GC, and lease
//! management land in Phase 1+.

use crate::config::SessionManagerConfig;
use crate::error::SessionManagerError;
use crate::id::SessionId;
use crate::table::SessionTable;

/// Per-reactor session registry.
///
/// Phase 0 holds only the table + counters; Phase 1 will add the
/// lookup cache, GC thread, and lease/TTL machinery.
// TODO(phase-1): lookup cache, GC, lease/TTL.
#[allow(dead_code)]
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

    /// Phase-1 `register` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn register(&mut self, _id: SessionId) -> Result<(), SessionManagerError> {
        // TODO(phase-1): install session, index, bump stats.
        Err(SessionManagerError::ResourceExhausted(
            "registry phase-1 stub",
        ))
    }

    /// Phase-1 `unregister` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::SessionNotFound`] in
    /// Phase 0.
    pub fn unregister(&mut self, id: SessionId) -> Result<(), SessionManagerError> {
        // TODO(phase-1): remove from index, release slot.
        Err(SessionManagerError::SessionNotFound(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn register_phase0_stub() {
        let mut r = SessionRegistry::new(SessionManagerConfig::default()).expect("ok");
        assert!(r.register(SessionId::nil()).is_err());
    }
}

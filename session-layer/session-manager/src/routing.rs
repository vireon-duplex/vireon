//! [`SessionRouter`] — SessionId → ConnectionId → Reactor core
//! resolution.
//!
//! See `DESIGN.md` Part 5 (session routing).
//!
//! # Role
//!
//! Given a [`SessionId`](crate::id::SessionId), resolve the bound
//! [`ConnectionId`](connection_shard::ConnectionId) and the reactor
//! core that owns the connection. This is the hot-path resolution
//! for inbound packets:
//!
//! 1. **SessionId → ConnectionId** — table lookup (Phase 1: O(1)
//!    via the [`SessionTable`](crate::table::SessionTable) index).
//! 2. **ConnectionId → Reactor core** — affinity map (Phase 1:
//!    per-shard `ConnectionId → u8`).
//!
//! Phase 6+ adds a global routing table (consistent hashing or
//! QUIC-LB for load-balanced deployments).
//!
//! # Phase 0 status
//!
//! Phase 0 ships the type signature and the lookup result shape.
//! Functional routing lands in Phase 1+.

use crate::error::SessionManagerError;
use crate::id::SessionId;
use connection_shard::ConnectionId;

/// Routing resolution result. Combines the bound connection with
/// its reactor-core affinity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RouteInfo {
    /// Bound QUIC connection.
    pub connection_id: ConnectionId,
    /// Reactor core owning the connection.
    pub reactor_core: u8,
}

/// Session router.
///
/// Phase 0 holds no state; Phase 1 will add the per-shard affinity
/// map.
// TODO(phase-1): per-shard affinity map.
#[derive(Clone, Debug, Default)]
pub struct SessionRouter;

impl SessionRouter {
    /// Construct a new router.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Phase-1 `resolve` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::SessionNotFound`] in
    /// Phase 0 (no affinity map yet).
    pub fn resolve(&self, id: SessionId) -> Result<RouteInfo, SessionManagerError> {
        // TODO(phase-1): session id → connection id → reactor core.
        Err(SessionManagerError::SessionNotFound(id))
    }

    /// Phase-1 `bind` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn bind(&mut self, _id: SessionId, _info: RouteInfo) -> Result<(), SessionManagerError> {
        // TODO(phase-1): install affinity entry.
        Err(SessionManagerError::ResourceExhausted(
            "router phase-1 stub",
        ))
    }

    /// Phase-1 `unbind` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::SessionNotFound`] in
    /// Phase 0.
    pub fn unbind(&mut self, id: SessionId) -> Result<(), SessionManagerError> {
        // TODO(phase-1): remove affinity entry.
        Err(SessionManagerError::SessionNotFound(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_phase0_stub() {
        let r = SessionRouter::new();
        assert!(matches!(
            r.resolve(SessionId::nil()),
            Err(SessionManagerError::SessionNotFound(_))
        ));
    }

    #[test]
    fn bind_phase0_stub() {
        let mut r = SessionRouter::new();
        let info = RouteInfo {
            connection_id: ConnectionId::from_raw(1),
            reactor_core: 0,
        };
        assert!(r.bind(SessionId::nil(), info).is_err());
    }

    #[test]
    fn unbind_phase0_stub() {
        let mut r = SessionRouter::new();
        assert!(r.unbind(SessionId::nil()).is_err());
    }
}

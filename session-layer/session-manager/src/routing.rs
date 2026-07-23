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
//! for inbound packets.
//!
//! Phase 6+ adds a global routing table (consistent hashing or
//! QUIC-LB for load-balanced deployments).

use rustc_hash::FxHashMap;

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

/// Session router — per-shard affinity map for
/// `SessionId → RouteInfo` resolution.
#[derive(Debug, Default)]
pub struct SessionRouter {
    /// SessionId → RouteInfo for sessions bound to this reactor.
    affinity: FxHashMap<SessionId, RouteInfo>,
}

impl SessionRouter {
    /// Construct a new router.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of bound routes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.affinity.len()
    }

    /// Whether the router has no bound routes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.affinity.is_empty()
    }

    /// Resolve a [`SessionId`] to its [`RouteInfo`].
    ///
    /// # Errors
    /// Returns [`SessionManagerError::SessionNotFound`] if no route
    /// is bound for the given id.
    pub fn resolve(&self, id: SessionId) -> Result<RouteInfo, SessionManagerError> {
        self.affinity
            .get(&id)
            .copied()
            .ok_or(SessionManagerError::SessionNotFound(id))
    }

    /// Bind a [`SessionId`] to a [`RouteInfo`].
    ///
    /// # Errors
    /// Returns [`SessionManagerError::ResourceExhausted`] if a route
    /// for this id is already bound.
    pub fn bind(
        &mut self,
        id: SessionId,
        info: RouteInfo,
    ) -> Result<(), SessionManagerError> {
        if self.affinity.contains_key(&id) {
            return Err(SessionManagerError::ResourceExhausted(
                "route already bound for this session id",
            ));
        }
        self.affinity.insert(id, info);
        Ok(())
    }

    /// Unbind a [`SessionId`]'s route.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::SessionNotFound`] if no route
    /// is bound for the given id.
    pub fn unbind(&mut self, id: SessionId) -> Result<(), SessionManagerError> {
        if self.affinity.remove(&id).is_none() {
            return Err(SessionManagerError::SessionNotFound(id));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn route(conn: u64, core: u8) -> RouteInfo {
        RouteInfo {
            connection_id: ConnectionId::from_raw(conn),
            reactor_core: core,
        }
    }

    #[test]
    fn resolve_unknown_returns_not_found() {
        let r = SessionRouter::new();
        assert!(matches!(
            r.resolve(SessionId::nil()),
            Err(SessionManagerError::SessionNotFound(_))
        ));
    }

    #[test]
    fn bind_then_resolve() {
        let mut r = SessionRouter::new();
        let id = SessionId::from_pair(1, 1);
        let info = route(42, 3);

        r.bind(id, info).unwrap();
        assert_eq!(r.resolve(id).unwrap(), info);
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn bind_duplicate_returns_error() {
        let mut r = SessionRouter::new();
        let id = SessionId::from_pair(1, 1);
        r.bind(id, route(1, 0)).unwrap();
        assert!(r.bind(id, route(2, 1)).is_err());
    }

    #[test]
    fn unbind_removes_route() {
        let mut r = SessionRouter::new();
        let id = SessionId::from_pair(1, 1);
        r.bind(id, route(1, 0)).unwrap();

        r.unbind(id).unwrap();
        assert!(r.resolve(id).is_err());
        assert!(r.is_empty());
    }

    #[test]
    fn unbind_unknown_returns_error() {
        let mut r = SessionRouter::new();
        assert!(r.unbind(SessionId::nil()).is_err());
    }
}

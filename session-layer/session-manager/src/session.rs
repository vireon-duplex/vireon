//! [`Session`] — the per-session metadata record owned by the
//! Session Manager.
//!
//! See `DESIGN.md` Part 2 (session object) and Part 3 (FSM).
//!
//! # Cache-line layout
//!
//! Fields are ordered for cache-line friendliness on the hot path:
//! the FSM + connection binding (touched every packet) come first;
//! the auth context + tracing (touched once per session decision)
//! come last. Phase 1+ may split this into hot/cold halves if
//! profiles justify it.
//!
//! # Phase 0 status
//!
//! This module ships the field shape + accessors. The actual
//! transition methods land in Phase 1+ when the FSM controller
//! ([`crate::lifecycle`]) is wired up.

use crate::id::SessionId;
use crate::priority::SessionClass;
use crate::quota::ResourceQuota;
use crate::state::SessionState;
use connection_shard::ConnectionId;

/// Nanoseconds since manager start (monotonic — `Instant::now()`
/// minus the manager's `start_instant`). `u64` is enough for ~584
/// years at nanosecond resolution.
pub type SessionInstantNs = u64;

/// Per-session metadata. One instance per live session; the
/// [`crate::table::SessionTable`] owns these and hands out
/// slot handles for O(1) access.
///
/// Note: not `Clone` because [`ResourceQuota`](crate::quota::ResourceQuota)
/// contains atomics.
#[derive(Debug)]
pub struct Session {
    /// 128-bit application-level session id.
    pub id: SessionId,
    /// 10-state session FSM.
    pub state: SessionState,
    /// Bound QUIC connection (runtime-level id). `None` before
    /// handshake completes or after a close.
    pub connection_id: Option<ConnectionId>,
    /// Reactor core affinity (which thread owns this session).
    pub reactor_core: Option<u8>,
    /// Scheduling class (Latency/Interactive/Streaming/Bulk).
    pub class: SessionClass,
    /// Per-session resource quota.
    pub quota: ResourceQuota,
    /// Tenant id (RBAC scope). `None` for unauthenticated sessions.
    pub tenant_id: Option<u64>,
    /// Whether the session has been authenticated.
    pub authenticated: bool,
    /// Whether the session has been activated (Ready → Active).
    pub activated: bool,
    /// Nanoseconds since manager start when the session was created.
    pub created_ns: SessionInstantNs,
    /// Nanoseconds since manager start of the last activity (any
    /// direction). Used by idle-timeout GC.
    pub last_active_ns: SessionInstantNs,
    /// Nanoseconds since manager start of the last migration. Used
    /// to rate-limit migration attempts.
    pub last_migration_ns: SessionInstantNs,
    /// Trace id propagated from the application layer (W3C
    /// trace-context, 128-bit).
    pub trace_id: [u8; 16],
}

impl Session {
    /// Construct a new session in the `Creating` initial state.
    ///
    /// Phase 0 only records the metadata; Phase 1+'s
    /// [`crate::lifecycle::SessionLifecycle`] will own creation.
    #[must_use]
    pub fn new(id: SessionId, class: SessionClass, now_ns: SessionInstantNs) -> Self {
        Self {
            id,
            state: SessionState::Creating,
            connection_id: None,
            reactor_core: None,
            class,
            quota: ResourceQuota::default(),
            tenant_id: None,
            authenticated: false,
            activated: false,
            created_ns: now_ns,
            last_active_ns: now_ns,
            last_migration_ns: 0,
            trace_id: [0u8; 16],
        }
    }

    /// Whether the session can accept application data.
    #[must_use]
    pub const fn can_send(&self) -> bool {
        self.state.can_accept_data()
    }

    /// Mark the session as having seen activity (resets idle GC).
    pub fn touch(&mut self, now_ns: SessionInstantNs) {
        self.last_active_ns = now_ns;
    }

    /// Bind the session to a connection (after handshake).
    pub fn bind_connection(&mut self, cid: ConnectionId, core: u8) {
        self.connection_id = Some(cid);
        self.reactor_core = Some(core);
    }

    /// Mark the session as authenticated.
    pub fn mark_authenticated(&mut self) {
        self.authenticated = true;
    }

    /// Mark the session as activated.
    pub fn mark_activated(&mut self) {
        self.activated = true;
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new(SessionId::nil(), SessionClass::default(), 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_session_starts_in_creating() {
        let s = Session::new(SessionId::from_pair(1, 1), SessionClass::default(), 0);
        assert_eq!(s.state, SessionState::Creating);
        assert!(!s.can_send());
        assert!(!s.authenticated);
        assert!(!s.activated);
        assert!(s.connection_id.is_none());
    }

    #[test]
    fn bind_connection_sets_core() {
        let mut s = Session::default();
        s.bind_connection(ConnectionId::from_raw(42), 3);
        assert_eq!(s.connection_id, Some(ConnectionId::from_raw(42)));
        assert_eq!(s.reactor_core, Some(3));
    }

    #[test]
    fn touch_updates_last_active() {
        let mut s = Session::default();
        s.touch(5_000);
        assert_eq!(s.last_active_ns, 5_000);
    }

    #[test]
    fn authentication_lifecycle() {
        let mut s = Session::default();
        assert!(!s.authenticated);
        s.mark_authenticated();
        assert!(s.authenticated);
        s.mark_activated();
        assert!(s.activated);
    }
}

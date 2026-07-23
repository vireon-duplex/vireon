//! [`SessionManager`] — facade that owns the session table,
//! registry, router, auth pipeline, scheduler, and stats for a
//! single reactor core.
//!
//! See `DESIGN.md` Part 1 (architecture goals) and Part 4
//! (lifecycle).
//!
//! # Role
//!
//! One `SessionManager` exists per reactor core (thread-per-core,
//! shared-nothing). It is the single application-facing handle for
//! "everything sessions". The manager internally coordinates:
//!
//! - [`SessionTable`](crate::table::SessionTable) — slab + index.
//! - [`SessionRegistry`](crate::registry::SessionRegistry) — local
//!   lookup.
//! - [`SessionRouter`](crate::routing::SessionRouter) — id → core
//!   resolution.
//! - [`AuthenticationPipeline`](crate::auth::AuthenticationPipeline)
//!   — pluggable auth.
//! - [`PolicyCache`](crate::authz::PolicyCache) — RBAC/ABAC.
//! - [`MigrationManager`](crate::migration::MigrationManager) —
//!   QUIC path migration.
//! - [`SessionScheduler`](crate::scheduler::SessionScheduler) —
//!   per-class scheduling.
//! - [`ResourceManager`](crate::resource::ResourceManager) —
//!   quotas + token bucket.
//! - [`SessionStats`](crate::stats::SessionStats) — atomic counters.

use std::time::Instant;

use crate::auth::AuthenticationPipeline;
use crate::authz::PolicyCache;
use crate::config::SessionManagerConfig;
use crate::error::SessionManagerError;
use crate::health::HealthReport;
use crate::id::SessionId;
use crate::lifecycle::SessionLifecycle;
use crate::migration::MigrationManager;
use crate::priority::SessionClass;
use crate::registry::SessionRegistry;
use crate::resource::ResourceManager;
use crate::routing::SessionRouter;
use crate::scheduler::SessionScheduler;
use crate::session::Session;
use crate::stats::SessionStats;
use crate::table::{SessionTable, SlotHandle};

/// Per-reactor session manager.
///
/// Owns all per-core session state. Phase 1 implements the
/// create/destroy lifecycle; auth, scheduling, migration, and
/// backpressure remain Phase 2+ stubs.
#[allow(dead_code)]
pub struct SessionManager {
    cfg: SessionManagerConfig,
    registry: SessionRegistry,
    router: SessionRouter,
    auth: AuthenticationPipeline,
    authz: PolicyCache,
    migration: MigrationManager,
    scheduler: SessionScheduler,
    resource: ResourceManager,
    lifecycle: SessionLifecycle,
    stats: SessionStats,
    /// Monotonic clock base for `SessionInstantNs` calculations.
    start: Instant,
}

impl SessionManager {
    /// Construct a manager from a validated config.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::InvalidConfig`] if validation
    /// fails.
    pub fn new(cfg: SessionManagerConfig) -> Result<Self, SessionManagerError> {
        cfg.validate()?;
        Ok(Self {
            registry: SessionRegistry::new(cfg)?,
            router: SessionRouter::new(),
            auth: AuthenticationPipeline::new(),
            authz: PolicyCache::new(),
            migration: MigrationManager::new(),
            scheduler: SessionScheduler::new(),
            resource: ResourceManager::new(cfg),
            lifecycle: SessionLifecycle::new(),
            stats: SessionStats::new(),
            start: Instant::now(),
            cfg,
        })
    }

    /// Borrow the config.
    #[must_use]
    pub const fn config(&self) -> &SessionManagerConfig {
        &self.cfg
    }

    /// Borrow the live stats counters.
    #[must_use]
    pub const fn stats(&self) -> &SessionStats {
        &self.stats
    }

    /// Borrow the session registry.
    #[must_use]
    pub const fn registry(&self) -> &SessionRegistry {
        &self.registry
    }

    /// Mutably borrow the session registry.
    pub fn registry_mut(&mut self) -> &mut SessionRegistry {
        &mut self.registry
    }

    /// Borrow the session router.
    #[must_use]
    pub const fn router(&self) -> &SessionRouter {
        &self.router
    }

    /// Mutably borrow the session router.
    pub fn router_mut(&mut self) -> &mut SessionRouter {
        &mut self.router
    }

    /// Borrow the session table (via the registry).
    #[must_use]
    pub const fn table(&self) -> &SessionTable {
        self.registry.table()
    }

    /// Current occupancy of the session table.
    #[must_use]
    pub const fn active_sessions(&self) -> u32 {
        self.registry.session_count()
    }

    /// Derive a health report from the live stats.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        let snap = self.stats.snapshot();
        HealthReport::derive(&snap)
    }

    /// Create a new session.
    ///
    /// Allocates a [`SessionId`], constructs a [`Session`] in the
    /// `Creating` state, registers it in the table, and bumps the
    /// creation stats.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::ResourceExhausted`] if the
    /// table is at `max_capacity`.
    pub fn create_session(
        &mut self,
        class: SessionClass,
    ) -> Result<SessionId, SessionManagerError> {
        let id = SessionId::new();
        let now_ns = self.start.elapsed().as_nanos() as u64;

        let session = Session::new(id, class, now_ns);
        self.registry.register(session)?;

        self.stats.record_created();
        self.stats.record_class(class as usize);

        Ok(id)
    }

    /// Destroy a session by id.
    ///
    /// Removes the session from the table and unbinds its route
    /// (if any). Bumps the destruction stats.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::SessionNotFound`] if no
    /// session with the given id exists.
    pub fn destroy_session(&mut self, id: SessionId) -> Result<(), SessionManagerError> {
        self.registry.unregister(id)?;

        // Best-effort route unbind — session may not have been bound yet.
        let _ = self.router.unbind(id);

        self.stats.record_destroyed();
        Ok(())
    }

    /// Look up a session by id, returning its [`SlotHandle`] via the
    /// registry.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::SessionNotFound`] if not found.
    pub fn lookup(&self, id: SessionId) -> Result<SlotHandle, SessionManagerError> {
        self.registry.lookup(id)
    }

    /// Borrow the [`Session`] at `handle`.
    #[must_use]
    pub fn get_session(&self, handle: SlotHandle) -> Option<&Session> {
        self.registry.get_session(handle)
    }

    /// Mutably borrow the [`Session`] at `handle`.
    pub fn get_session_mut(&mut self, handle: SlotHandle) -> Option<&mut Session> {
        self.registry.get_session_mut(handle)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn new_validates_config() {
        let m = SessionManager::new(SessionManagerConfig::default()).expect("default ok");
        assert_eq!(m.active_sessions(), 0);
        assert!(m.health().is_healthy());
    }

    #[test]
    fn new_rejects_invalid_config() {
        let bad = SessionManagerConfig {
            max_sessions: 0,
            ..SessionManagerConfig::default()
        };
        assert!(SessionManager::new(bad).is_err());
    }

    #[test]
    fn create_session_returns_unique_id() {
        let mut m = SessionManager::new(SessionManagerConfig::default()).unwrap();
        let id1 = m.create_session(SessionClass::default()).unwrap();
        let id2 = m.create_session(SessionClass::default()).unwrap();
        assert_ne!(id1, id2);
        assert_eq!(m.active_sessions(), 2);
    }

    #[test]
    fn create_then_lookup() {
        let mut m = SessionManager::new(SessionManagerConfig::default()).unwrap();
        let id = m.create_session(SessionClass::default()).unwrap();

        let handle = m.lookup(id).unwrap();
        let session = m.get_session(handle).unwrap();
        assert_eq!(session.id, id);
    }

    #[test]
    fn destroy_session_removes_it() {
        let mut m = SessionManager::new(SessionManagerConfig::default()).unwrap();
        let id = m.create_session(SessionClass::default()).unwrap();
        assert_eq!(m.active_sessions(), 1);

        m.destroy_session(id).unwrap();
        assert_eq!(m.active_sessions(), 0);
        assert!(m.lookup(id).is_err());
    }

    #[test]
    fn destroy_unknown_returns_error() {
        let mut m = SessionManager::new(SessionManagerConfig::default()).unwrap();
        assert!(m.destroy_session(SessionId::nil()).is_err());
    }
}

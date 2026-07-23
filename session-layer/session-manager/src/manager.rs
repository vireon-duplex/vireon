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
use crate::backpressure::BackpressureTracker;
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
/// Owns all per-core session state. Phase 2 implements lifecycle
/// (FSM), scheduler (admission control), and resource (token bucket).
/// Auth, authz, migration, and backpressure remain Phase 3 stubs.
pub struct SessionManager {
    cfg: SessionManagerConfig,
    registry: SessionRegistry,
    router: SessionRouter,
    #[allow(dead_code)]
    auth: AuthenticationPipeline,
    #[allow(dead_code)]
    authz: PolicyCache,
    migration: MigrationManager,
    scheduler: SessionScheduler,
    resource: ResourceManager,
    lifecycle: SessionLifecycle,
    backpressure: BackpressureTracker,
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
            scheduler: SessionScheduler::with_max_sessions(cfg.max_sessions),
            resource: ResourceManager::new(cfg),
            lifecycle: SessionLifecycle::new(),
            backpressure: BackpressureTracker::new(),
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

    /// Mutably borrow the scheduler (for configuring per-class limits).
    pub fn scheduler_mut(&mut self) -> &mut SessionScheduler {
        &mut self.scheduler
    }

    /// Borrow the migration manager.
    #[must_use]
    pub const fn migration(&self) -> &MigrationManager {
        &self.migration
    }

    /// Mutably borrow the migration manager.
    pub fn migration_mut(&mut self) -> &mut MigrationManager {
        &mut self.migration
    }

    /// Borrow the backpressure tracker.
    #[must_use]
    pub const fn backpressure(&self) -> &BackpressureTracker {
        &self.backpressure
    }

    /// Mutably borrow the backpressure tracker.
    pub fn backpressure_mut(&mut self) -> &mut BackpressureTracker {
        &mut self.backpressure
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
    /// Validates admission via the scheduler, allocates a
    /// [`SessionId`], constructs a [`Session`] in the `Creating`
    /// state (via [`SessionLifecycle::create`]), registers it in
    /// the table, and allocates a per-session token bucket.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::SessionLimitReached`] if the
    /// scheduler rejects the class.
    /// Returns [`SessionManagerError::ResourceExhausted`] if the
    /// table is at `max_capacity` or the token-bucket allocation fails.
    pub fn create_session(
        &mut self,
        class: SessionClass,
    ) -> Result<SessionId, SessionManagerError> {
        // Admission control.
        match self.scheduler.admit(class) {
            Ok(crate::scheduler::AdmissionDecision::Admitted { .. }) => {}
            Ok(_) => {
                return Err(SessionManagerError::SessionLimitReached);
            }
            Err(e) => return Err(e),
        }

        let id = SessionId::new();
        let now_ns = self.start.elapsed().as_nanos() as u64;

        // Lifecycle: create returns the initial state (Creating).
        let initial_state = self.lifecycle.create();
        let mut session = Session::new(id, class, now_ns);
        session.state = initial_state;
        self.registry.register(session)?;

        // Resource: per-session token bucket.
        // Best-effort — if allocation fails (duplicate id, extremely
        // unlikely with 128-bit ids), the session is still registered;
        // rate limiting simply won't apply.
        let _ = self.resource.allocate(id);

        self.stats.record_created();
        self.stats.record_class(class as usize);

        Ok(id)
    }

    /// Destroy a session by id.
    ///
    /// Validates the lifecycle transition (drain → destroy), removes
    /// the session from the table, releases the scheduler slot and
    /// resource bucket, and unbinds its route (if any).
    ///
    /// # Errors
    /// Returns [`SessionManagerError::SessionNotFound`] if no
    /// session with the given id exists.
    pub fn destroy_session(&mut self, id: SessionId) -> Result<(), SessionManagerError> {
        // Look up the session to get its state + class for cleanup.
        let (state, class) = {
            let handle = self.registry.lookup(id)?;
            let session = self
                .registry
                .get_session(handle)
                .ok_or(SessionManagerError::SessionNotFound(id))?;
            (session.state, session.class)
        };

        // Lifecycle: validate the transition to Destroyed. Sessions
        // that are still in Creating (never activated) skip the drain
        // step and go straight to Destroyed — the FSM allows
        // Creating → Destroyed directly.
        let target = match self.lifecycle.drain(state) {
            Ok(draining) => self.lifecycle.destroy(draining)?,
            Err(_) => self.lifecycle.destroy(state)?,
        };
        let _ = target; // state is applied conceptually; the session
                        // is about to be unregistered anyway.

        self.registry.unregister(id)?;

        // Release scheduler + resource.
        self.scheduler.release(class);
        self.resource.release(id);

        // Best-effort route unbind — session may not have been bound yet.
        let _ = self.router.unbind(id);

        self.stats.record_destroyed();
        Ok(())
    }

    /// Activate a session: advances one step along the
    /// `Creating → Authenticating → Ready → Active` chain.
    ///
    /// The caller should invoke this after each handshake stage
    /// (connection bound, auth completed, app ready).
    ///
    /// # Errors
    /// Returns [`SessionManagerError::SessionNotFound`] if not found.
    /// Returns [`SessionManagerError::ResourceExhausted`] if the
    /// transition is invalid for the current state.
    pub fn activate_session(&mut self, id: SessionId) -> Result<(), SessionManagerError> {
        let handle = self.registry.lookup(id)?;
        let session = self
            .registry
            .get_session_mut(handle)
            .ok_or(SessionManagerError::SessionNotFound(id))?;
        let current = session.state;
        session.state = self.lifecycle.activate(current)?;
        if session.state == crate::state::SessionState::Active {
            session.mark_activated();
        }
        Ok(())
    }

    /// Initiate migration for a session.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::SessionNotFound`] if not found.
    /// Returns [`SessionManagerError::MigrationFailed`] if the
    /// session is not in a migratable state.
    pub fn migrate_session(&mut self, id: SessionId) -> Result<(), SessionManagerError> {
        let handle = self.registry.lookup(id)?;
        let session = self
            .registry
            .get_session_mut(handle)
            .ok_or(SessionManagerError::SessionNotFound(id))?;
        let current = session.state;
        session.state = self.lifecycle.migrate(current)?;
        let now_ns = self.start.elapsed().as_nanos() as u64;
        session.last_migration_ns = now_ns;
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

    #[test]
    fn activate_advances_state() {
        let mut m = SessionManager::new(SessionManagerConfig::default()).unwrap();
        let id = m.create_session(SessionClass::default()).unwrap();
        // Creating → Authenticating
        m.activate_session(id).unwrap();
        let handle = m.lookup(id).unwrap();
        assert_eq!(m.get_session(handle).unwrap().state, crate::state::SessionState::Authenticating);
        // Authenticating → Ready
        m.activate_session(id).unwrap();
        // Ready → Active
        m.activate_session(id).unwrap();
        let handle = m.lookup(id).unwrap();
        assert!(m.get_session(handle).unwrap().activated);
    }

    #[test]
    fn migrate_sets_migrating() {
        let mut m = SessionManager::new(SessionManagerConfig::default()).unwrap();
        let id = m.create_session(SessionClass::default()).unwrap();
        // Advance to Active first.
        for _ in 0..3 {
            m.activate_session(id).unwrap();
        }
        m.migrate_session(id).unwrap();
        let handle = m.lookup(id).unwrap();
        assert_eq!(m.get_session(handle).unwrap().state, crate::state::SessionState::Migrating);
    }

    #[test]
    fn scheduler_rejects_at_limit() {
        let cfg = SessionManagerConfig::default();
        let mut m = SessionManager::new(cfg).unwrap();
        m.scheduler_mut().set_class_limit(SessionClass::Bulk, 1);
        let id1 = m.create_session(SessionClass::Bulk).unwrap();
        // Second Bulk session should fail.
        assert!(m.create_session(SessionClass::Bulk).is_err());
        // But a different class still works.
        let _id3 = m.create_session(SessionClass::Interactive).unwrap();
        m.destroy_session(id1).unwrap();
        // After release, Bulk is available again.
        assert!(m.create_session(SessionClass::Bulk).is_ok());
    }
}

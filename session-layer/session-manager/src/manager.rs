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
//!
//! # Phase 0 status
//!
//! Phase 0 ships only the facade shape. Functional methods land in
//! Phase 1+.

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
use crate::stats::SessionStats;
use crate::table::SessionTable;

/// Per-reactor session manager.
///
/// Phase 0 holds the component parts so other modules can reference
/// them; Phase 1+ adds the functional surface.
// TODO(phase-1): functional methods.
#[allow(dead_code)]
pub struct SessionManager {
    cfg: SessionManagerConfig,
    table: SessionTable,
    registry: SessionRegistry,
    router: SessionRouter,
    auth: AuthenticationPipeline,
    authz: PolicyCache,
    migration: MigrationManager,
    scheduler: SessionScheduler,
    resource: ResourceManager,
    lifecycle: SessionLifecycle,
    stats: SessionStats,
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
            table: SessionTable::new(cfg),
            registry: SessionRegistry::new(cfg)?,
            router: SessionRouter::new(),
            auth: AuthenticationPipeline::new(),
            authz: PolicyCache::new(),
            migration: MigrationManager::new(),
            scheduler: SessionScheduler::new(),
            resource: ResourceManager::new(cfg),
            lifecycle: SessionLifecycle::new(),
            stats: SessionStats::new(),
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

    /// Current occupancy of the session table.
    #[must_use]
    pub const fn active_sessions(&self) -> u32 {
        self.table.occupancy()
    }

    /// Derive a health report from the live stats.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        let snap = self.stats.snapshot();
        HealthReport::derive(&snap)
    }

    /// Phase-1 `create_session` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn create_session(
        &mut self,
        _class: SessionClass,
    ) -> Result<SessionId, SessionManagerError> {
        // TODO(phase-1): allocate id, reserve slot, install Session,
        // bump stats, emit Created event.
        Err(SessionManagerError::ResourceExhausted(
            "SessionManager::create_session phase-1 stub",
        ))
    }

    /// Phase-1 `destroy_session` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::SessionNotFound`] in
    /// Phase 0.
    pub fn destroy_session(&mut self, id: SessionId) -> Result<(), SessionManagerError> {
        // TODO(phase-1): transition to Draining, emit Closed event.
        Err(SessionManagerError::SessionNotFound(id))
    }
}

#[cfg(test)]
mod tests {
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
    fn create_session_phase0_stub() {
        let mut m = SessionManager::new(SessionManagerConfig::default()).expect("ok");
        assert!(m.create_session(SessionClass::default()).is_err());
    }

    #[test]
    fn destroy_session_phase0_stub() {
        let mut m = SessionManager::new(SessionManagerConfig::default()).expect("ok");
        assert!(m.destroy_session(SessionId::nil()).is_err());
    }
}

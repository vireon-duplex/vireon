//! [`QuotaEnforcer`] — enforces per-tenant quota limits in real-time.
//!
//! Wraps [`QuotaManager`] with higher-level enforcement methods that derive
//! the tenant's stored limit automatically, so callers don't need to thread
//! a `QuotaLimit` through every call site.
//!
//! See `DESIGN.md` Part 10 (enforcer).

use crate::config::ResourceQuotasConfig;
use crate::error::QuotaError;
use crate::limit::QuotaLimit;
use crate::manager::QuotaManager;
use crate::stats::ResourceQuotaStats;
use crate::tenant::{TenantId, TenantTier};

/// Quota enforcer — wraps [`QuotaManager`] with enforcement semantics.
///
/// The enforcer exposes two layers:
///
/// 1. **Pass-through** — `register_tenant`, `reserve`, `release`,
///    `record_bytes` delegate directly to the underlying manager. Use these
///    when the caller already knows the exact `QuotaLimit` or needs the raw
///    counter semantics.
///
/// 2. **Enforcement** — `enforce_session`, `enforce_bandwidth` look up the
///    tenant's stored limit automatically. Use these in hot paths where the
///    caller only has a `TenantId` and wants the enforcer to apply the
///    tier-derived defaults.
#[derive(Debug)]
pub struct QuotaEnforcer {
    manager: QuotaManager,
}

impl QuotaEnforcer {
    /// Create a new quota enforcer.
    pub fn new(cfg: ResourceQuotasConfig, stats: ResourceQuotaStats) -> Self {
        Self {
            manager: QuotaManager::new(cfg, stats),
        }
    }

    // ── Enforcement methods (tier-derived limit lookup) ────────────

    /// Check whether a tenant can open another session (read-only — does
    /// not increment the counter; pair with [`Self::reserve`] to actually
    /// claim the slot).
    ///
    /// # Errors
    /// Returns [`QuotaError::TenantNotFound`] if unregistered.
    /// Returns [`QuotaError::LimitExceeded`] if the session quota is full.
    pub fn enforce_session(&mut self, tenant: TenantId) -> Result<(), QuotaError> {
        let limit = self.manager.limit_for(tenant)?;
        self.manager.check(tenant, &limit)
    }

    /// Record bandwidth usage for a tenant and verify it is within the
    /// tier-derived bandwidth limit.
    ///
    /// In Phase 1 this delegates to [`QuotaManager::record_bytes`] for
    /// cumulative volume tracking. True rate-limited enforcement (sliding
    /// window or token bucket) requires per-tenant window state and lands
    /// in a future phase.
    ///
    /// # Errors
    /// Returns [`QuotaError::TenantNotFound`] if unregistered.
    pub fn enforce_bandwidth(&mut self, tenant: TenantId, bytes: u64) -> Result<(), QuotaError> {
        self.manager.record_bytes(tenant, bytes)
    }

    /// Get the current health state.
    #[must_use]
    pub fn health(&self) -> bool {
        self.manager.health()
    }

    // ── Pass-through methods (delegate to QuotaManager) ────────────

    /// Register a new tenant. See [`QuotaManager::register_tenant`].
    pub fn register_tenant(&mut self, id: TenantId, tier: TenantTier) -> Result<(), QuotaError> {
        self.manager.register_tenant(id, tier)
    }

    /// Reserve a session slot. See [`QuotaManager::reserve`].
    pub fn reserve(&mut self, id: TenantId, limit: &QuotaLimit) -> Result<(), QuotaError> {
        self.manager.reserve(id, limit)
    }

    /// Release a session slot. See [`QuotaManager::release`].
    pub fn release(&mut self, id: TenantId) -> Result<(), QuotaError> {
        self.manager.release(id)
    }

    /// Record raw byte volume. See [`QuotaManager::record_bytes`].
    pub fn record_bytes(&mut self, id: TenantId, n: u64) -> Result<(), QuotaError> {
        self.manager.record_bytes(id, n)
    }

    /// Borrow the underlying manager.
    #[must_use]
    pub const fn manager(&self) -> &QuotaManager {
        &self.manager
    }

    /// Mutably borrow the underlying manager.
    pub fn manager_mut(&mut self) -> &mut QuotaManager {
        &mut self.manager
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enforcer_with_tenant(id: u64, tier: TenantTier) -> QuotaEnforcer {
        let mut e = QuotaEnforcer::new(ResourceQuotasConfig::new(), ResourceQuotaStats::new());
        e.register_tenant(TenantId::new(id), tier).unwrap();
        e
    }

    #[test]
    fn new_enforcer_created() {
        let cfg = ResourceQuotasConfig::new();
        let stats = ResourceQuotaStats::new();
        let enforcer = QuotaEnforcer::new(cfg, stats);
        assert!(enforcer.health());
    }

    #[test]
    fn enforce_session_succeeds_for_registered_tenant() {
        let mut e = enforcer_with_tenant(1, TenantTier::Internal);
        assert!(e.enforce_session(TenantId::new(1)).is_ok());
    }

    #[test]
    fn enforce_session_fails_for_unregistered_tenant() {
        let mut e = enforcer_with_tenant(1, TenantTier::Internal);
        assert!(matches!(
            e.enforce_session(TenantId::new(99)),
            Err(QuotaError::TenantNotFound(_))
        ));
    }

    #[test]
    fn enforce_session_detects_limit_exceeded() {
        let mut e = enforcer_with_tenant(1, TenantTier::Free);
        let limit = QuotaLimit::for_tier(TenantTier::Free);
        // Exhaust session quota.
        for _ in 0..limit.max_sessions {
            e.reserve(TenantId::new(1), &limit).unwrap();
        }
        // enforce_session should now detect the limit.
        assert!(matches!(
            e.enforce_session(TenantId::new(1)),
            Err(QuotaError::LimitExceeded(_))
        ));
    }

    #[test]
    fn enforce_bandwidth_records_bytes_for_registered_tenant() {
        let mut e = enforcer_with_tenant(1, TenantTier::Pro);
        assert!(e.enforce_bandwidth(TenantId::new(1), 1_000_000).is_ok());
        assert_eq!(e.manager().bytes_used(TenantId::new(1)), 1_000_000);
    }

    #[test]
    fn enforce_bandwidth_fails_for_unregistered_tenant() {
        let mut e = enforcer_with_tenant(1, TenantTier::Pro);
        assert!(matches!(
            e.enforce_bandwidth(TenantId::new(99), 100),
            Err(QuotaError::TenantNotFound(_))
        ));
    }

    #[test]
    fn pass_through_methods_work() {
        let mut e = enforcer_with_tenant(1, TenantTier::Pro);
        let limit = QuotaLimit::for_tier(TenantTier::Pro);

        assert!(e.reserve(TenantId::new(1), &limit).is_ok());
        assert_eq!(e.manager().sessions_used(TenantId::new(1)), 1);

        assert!(e.release(TenantId::new(1)).is_ok());
        assert_eq!(e.manager().sessions_used(TenantId::new(1)), 0);
    }
}

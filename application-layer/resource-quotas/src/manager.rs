//! [`QuotaManager`] — per-tenant quota tracking and enforcement.
//!
//! Provides an in-process quota registry backed by a
//! `HashMap<TenantId, TenantEntry>`. Single-threaded (reactor-local).

use std::collections::HashMap;

use crate::config::ResourceQuotasConfig;
use crate::error::QuotaError;
use crate::limit::QuotaLimit;
use crate::stats::ResourceQuotaStats;
use crate::tenant::{TenantId, TenantTier};

/// Per-tenant usage counters.
#[derive(Debug, Clone, Copy)]
struct TenantEntry {
    /// Maximum allowed values (derived from tier or custom).
    /// Stored for future "default check" use; current check/reserve
    /// methods accept an external `QuotaLimit` for per-request overrides.
    #[allow(dead_code)]
    limit: QuotaLimit,
    /// Current active session count.
    sessions_used: u32,
    /// Current active stream count.
    streams_used: u32,
}

/// Quota manager — tracks per-tenant resource usage.
///
/// All methods take `&self` or `&mut self` (single-threaded).
#[derive(Debug)]
pub struct QuotaManager {
    #[allow(dead_code)]
    cfg: ResourceQuotasConfig,
    stats: ResourceQuotaStats,
    tenants: HashMap<TenantId, TenantEntry>,
}

impl QuotaManager {
    /// Create a new quota manager.
    pub fn new(cfg: ResourceQuotasConfig, stats: ResourceQuotaStats) -> Self {
        Self {
            cfg,
            stats,
            tenants: HashMap::new(),
        }
    }

    /// Register a new tenant with tier-derived default limits.
    ///
    /// # Errors
    /// Returns [`QuotaError::DuplicateTenant`] if the tenant already exists.
    pub fn register_tenant(&mut self, id: TenantId, tier: TenantTier) -> Result<(), QuotaError> {
        if self.tenants.contains_key(&id) {
            return Err(QuotaError::DuplicateTenant(id));
        }
        self.tenants.insert(
            id,
            TenantEntry {
                limit: QuotaLimit::for_tier(tier),
                sessions_used: 0,
                streams_used: 0,
            },
        );
        self.stats.record_tenant_tracked();
        Ok(())
    }

    /// Check if a tenant is within quota limits (read-only).
    ///
    /// The `limit` parameter overrides the tenant's stored limit for
    /// this check (useful for per-request limits like rate-per-second).
    ///
    /// # Errors
    /// Returns [`QuotaError::TenantNotFound`] if unregistered.
    /// Returns [`QuotaError::LimitExceeded`] if over quota.
    pub fn check(&self, id: TenantId, limit: &QuotaLimit) -> Result<(), QuotaError> {
        let entry = self
            .tenants
            .get(&id)
            .ok_or(QuotaError::TenantNotFound(id))?;

        self.stats.record_check();

        if entry.sessions_used >= limit.max_sessions {
            self.stats.record_check_exceeded();
            return Err(QuotaError::LimitExceeded("max_sessions"));
        }
        if entry.streams_used >= limit.max_streams {
            self.stats.record_check_exceeded();
            return Err(QuotaError::LimitExceeded("max_streams"));
        }
        self.stats.record_check_passed();
        Ok(())
    }

    /// Reserve a session slot for a tenant (increments counter).
    ///
    /// # Errors
    /// Returns [`QuotaError::TenantNotFound`] if unregistered.
    /// Returns [`QuotaError::LimitExceeded`] if at capacity.
    pub fn reserve(&mut self, id: TenantId, limit: &QuotaLimit) -> Result<(), QuotaError> {
        let entry = self
            .tenants
            .get_mut(&id)
            .ok_or(QuotaError::TenantNotFound(id))?;

        self.stats.record_check();

        if entry.sessions_used >= limit.max_sessions {
            self.stats.record_check_exceeded();
            return Err(QuotaError::LimitExceeded("max_sessions"));
        }

        entry.sessions_used += 1;
        self.stats.record_session_tracked();
        self.stats.record_check_passed();
        Ok(())
    }

    /// Release a session slot (decrements counter).
    ///
    /// Silently saturates at 0 if called more times than `reserve`.
    ///
    /// # Errors
    /// Returns [`QuotaError::TenantNotFound`] if unregistered.
    pub fn release(&mut self, id: TenantId) -> Result<(), QuotaError> {
        let entry = self
            .tenants
            .get_mut(&id)
            .ok_or(QuotaError::TenantNotFound(id))?;

        entry.sessions_used = entry.sessions_used.saturating_sub(1);
        Ok(())
    }

    /// Get the current health state.
    #[must_use]
    pub fn health(&self) -> bool {
        true
    }

    /// Number of registered tenants.
    #[must_use]
    pub fn tenant_count(&self) -> usize {
        self.tenants.len()
    }

    /// Current session usage for a tenant (0 if not found).
    #[must_use]
    pub fn sessions_used(&self, id: TenantId) -> u32 {
        self.tenants.get(&id).map_or(0, |e| e.sessions_used)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> QuotaManager {
        QuotaManager::new(ResourceQuotasConfig::new(), ResourceQuotaStats::new())
    }

    #[test]
    fn new_manager_created() {
        let manager = fresh();
        assert!(manager.health());
        assert_eq!(manager.tenant_count(), 0);
    }

    #[test]
    fn register_and_check_passes() {
        let mut manager = fresh();
        manager
            .register_tenant(TenantId::new(1), TenantTier::Pro)
            .unwrap();
        let limit = QuotaLimit::for_tier(TenantTier::Pro);
        assert!(manager.check(TenantId::new(1), &limit).is_ok());
    }

    #[test]
    fn check_unknown_tenant_returns_error() {
        let manager = fresh();
        let limit = QuotaLimit::for_tier(TenantTier::Pro);
        assert!(matches!(
            manager.check(TenantId::new(99), &limit),
            Err(QuotaError::TenantNotFound(_))
        ));
    }

    #[test]
    fn duplicate_registration_returns_error() {
        let mut manager = fresh();
        manager
            .register_tenant(TenantId::new(1), TenantTier::Pro)
            .unwrap();
        assert!(matches!(
            manager.register_tenant(TenantId::new(1), TenantTier::Enterprise),
            Err(QuotaError::DuplicateTenant(_))
        ));
    }

    #[test]
    fn reserve_increments_sessions() {
        let mut manager = fresh();
        manager
            .register_tenant(TenantId::new(1), TenantTier::Pro)
            .unwrap();
        let limit = QuotaLimit::for_tier(TenantTier::Pro);

        manager.reserve(TenantId::new(1), &limit).unwrap();
        assert_eq!(manager.sessions_used(TenantId::new(1)), 1);

        manager.reserve(TenantId::new(1), &limit).unwrap();
        assert_eq!(manager.sessions_used(TenantId::new(1)), 2);
    }

    #[test]
    fn release_decrements_sessions() {
        let mut manager = fresh();
        manager
            .register_tenant(TenantId::new(1), TenantTier::Pro)
            .unwrap();
        let limit = QuotaLimit::for_tier(TenantTier::Pro);

        manager.reserve(TenantId::new(1), &limit).unwrap();
        manager.reserve(TenantId::new(1), &limit).unwrap();
        assert_eq!(manager.sessions_used(TenantId::new(1)), 2);

        manager.release(TenantId::new(1)).unwrap();
        assert_eq!(manager.sessions_used(TenantId::new(1)), 1);
    }

    #[test]
    fn reserve_beyond_limit_returns_exceeded() {
        let mut manager = fresh();
        manager
            .register_tenant(TenantId::new(1), TenantTier::Free)
            .unwrap();

        // Free tier has a small default session limit.
        let limit = QuotaLimit::for_tier(TenantTier::Free);
        for _ in 0..limit.max_sessions {
            manager.reserve(TenantId::new(1), &limit).unwrap();
        }

        assert!(matches!(
            manager.reserve(TenantId::new(1), &limit),
            Err(QuotaError::LimitExceeded(_))
        ));
    }

    #[test]
    fn release_saturates_at_zero() {
        let mut manager = fresh();
        manager
            .register_tenant(TenantId::new(1), TenantTier::Pro)
            .unwrap();

        // Release without reserve — should not panic.
        manager.release(TenantId::new(1)).unwrap();
        assert_eq!(manager.sessions_used(TenantId::new(1)), 0);
    }

    #[test]
    fn release_unknown_tenant_returns_error() {
        let mut manager = fresh();
        assert!(matches!(
            manager.release(TenantId::new(99)),
            Err(QuotaError::TenantNotFound(_))
        ));
    }

    #[test]
    fn reserve_release_cycle() {
        let mut manager = fresh();
        manager
            .register_tenant(TenantId::new(1), TenantTier::Internal)
            .unwrap();
        let limit = QuotaLimit::for_tier(TenantTier::Internal);

        for _ in 0..10 {
            manager.reserve(TenantId::new(1), &limit).unwrap();
        }
        assert_eq!(manager.sessions_used(TenantId::new(1)), 10);

        for _ in 0..10 {
            manager.release(TenantId::new(1)).unwrap();
        }
        assert_eq!(manager.sessions_used(TenantId::new(1)), 0);
    }
}

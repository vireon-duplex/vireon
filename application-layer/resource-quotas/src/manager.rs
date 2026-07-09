//! [`QuotaManager`] — skeleton for quota management functionality.
//!
//! See `DESIGN.md` Part 9 (manager).
//!
//! # Phase 1
//!
//! This module ships as a typed skeleton. Functional manager lands in
//! Phase 1.

use crate::config::ResourceQuotasConfig;
use crate::error::QuotaError;
use crate::limit::QuotaLimit;
use crate::stats::ResourceQuotaStats;
use crate::tenant::{TenantId, TenantTier};

/// Quota manager (skeleton).
///
/// In Phase 1, this will manage tenant registration, quota checking,
/// and resource reservation. Currently returns placeholder errors.
#[derive(Debug)]
pub struct QuotaManager {
    #[allow(dead_code)]
    cfg: ResourceQuotasConfig,
    #[allow(dead_code)]
    stats: ResourceQuotaStats,
}

impl QuotaManager {
    /// Create a new quota manager.
    pub fn new(cfg: ResourceQuotasConfig, stats: ResourceQuotaStats) -> Self {
        Self { cfg, stats }
    }

    /// Register a new tenant.
    ///
    /// # Errors
    /// Returns [`QuotaError::ShuttingDown`] unconditionally
    /// in this skeleton.
    pub fn register_tenant(&mut self, _id: TenantId, _tier: TenantTier) -> Result<(), QuotaError> {
        Err(QuotaError::ShuttingDown)
    }

    /// Check if a tenant is within quota limits.
    ///
    /// # Errors
    /// Returns [`QuotaError::TenantNotFound`] for any tenant in
    /// this skeleton.
    pub fn check(&self, id: TenantId, _limit: &QuotaLimit) -> Result<(), QuotaError> {
        Err(QuotaError::TenantNotFound(id))
    }

    /// Reserve resources for a tenant.
    ///
    /// # Errors
    /// Returns [`QuotaError::ShuttingDown`] unconditionally
    /// in this skeleton.
    pub fn reserve(&mut self, _id: TenantId, _limit: &QuotaLimit) -> Result<(), QuotaError> {
        Err(QuotaError::ShuttingDown)
    }

    /// Release resources for a tenant.
    ///
    /// # Errors
    /// Returns [`QuotaError::ShuttingDown`] unconditionally
    /// in this skeleton.
    pub fn release(&mut self, _id: TenantId) -> Result<(), QuotaError> {
        Err(QuotaError::ShuttingDown)
    }

    /// Get the current health state.
    #[must_use]
    pub fn health(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_manager_created() {
        let cfg = ResourceQuotasConfig::new();
        let stats = ResourceQuotaStats::new();
        let manager = QuotaManager::new(cfg, stats);
        assert!(manager.health());
    }

    #[test]
    fn register_tenant_returns_shutting_down() {
        let mut manager = QuotaManager::new(ResourceQuotasConfig::new(), ResourceQuotaStats::new());
        assert!(matches!(
            manager.register_tenant(TenantId::new(1), TenantTier::Pro),
            Err(QuotaError::ShuttingDown)
        ));
    }

    #[test]
    fn check_returns_tenant_not_found() {
        let manager = QuotaManager::new(ResourceQuotasConfig::new(), ResourceQuotaStats::new());
        let limit = QuotaLimit::for_tier(TenantTier::Pro);
        assert!(matches!(
            manager.check(TenantId::new(1), &limit),
            Err(QuotaError::TenantNotFound(_))
        ));
    }

    #[test]
    fn reserve_returns_shutting_down() {
        let mut manager = QuotaManager::new(ResourceQuotasConfig::new(), ResourceQuotaStats::new());
        let limit = QuotaLimit::for_tier(TenantTier::Pro);
        assert!(matches!(
            manager.reserve(TenantId::new(1), &limit),
            Err(QuotaError::ShuttingDown)
        ));
    }

    #[test]
    fn release_returns_shutting_down() {
        let mut manager = QuotaManager::new(ResourceQuotasConfig::new(), ResourceQuotaStats::new());
        assert!(matches!(
            manager.release(TenantId::new(1)),
            Err(QuotaError::ShuttingDown)
        ));
    }
}

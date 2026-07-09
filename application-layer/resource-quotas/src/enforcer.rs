//! [`QuotaEnforcer`] — skeleton for quota enforcement functionality.
//!
//! See `DESIGN.md` Part 10 (enforcer).
//!
//! # Phase 1
//!
//! This module ships as a typed skeleton. Functional enforcer lands in
//! Phase 1.

use crate::config::ResourceQuotasConfig;
use crate::error::QuotaError;
use crate::stats::ResourceQuotaStats;
use crate::tenant::TenantId;

/// Quota enforcer (skeleton).
///
/// In Phase 1, this will enforce quota limits in real-time for
/// operations like session creation, bandwidth usage, and request
/// rate limiting. Currently returns placeholder errors.
#[derive(Debug)]
pub struct QuotaEnforcer {
    #[allow(dead_code)]
    cfg: ResourceQuotasConfig,
    #[allow(dead_code)]
    stats: ResourceQuotaStats,
}

impl QuotaEnforcer {
    /// Create a new quota enforcer.
    pub fn new(cfg: ResourceQuotasConfig, stats: ResourceQuotaStats) -> Self {
        Self { cfg, stats }
    }

    /// Enforce session quota for a tenant.
    ///
    /// # Errors
    /// Returns [`QuotaError::ShuttingDown`] unconditionally
    /// in this skeleton.
    pub fn enforce_session(&mut self, _tenant: TenantId) -> Result<(), QuotaError> {
        Err(QuotaError::ShuttingDown)
    }

    /// Enforce bandwidth quota for a tenant.
    ///
    /// # Errors
    /// Returns [`QuotaError::ShuttingDown`] unconditionally
    /// in this skeleton.
    pub fn enforce_bandwidth(&mut self, _tenant: TenantId, _bytes: u64) -> Result<(), QuotaError> {
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
    fn new_enforcer_created() {
        let cfg = ResourceQuotasConfig::new();
        let stats = ResourceQuotaStats::new();
        let enforcer = QuotaEnforcer::new(cfg, stats);
        assert!(enforcer.health());
    }

    #[test]
    fn enforce_session_returns_shutting_down() {
        let mut enforcer =
            QuotaEnforcer::new(ResourceQuotasConfig::new(), ResourceQuotaStats::new());
        assert!(matches!(
            enforcer.enforce_session(TenantId::new(1)),
            Err(QuotaError::ShuttingDown)
        ));
    }

    #[test]
    fn enforce_bandwidth_returns_shutting_down() {
        let mut enforcer =
            QuotaEnforcer::new(ResourceQuotasConfig::new(), ResourceQuotaStats::new());
        assert!(matches!(
            enforcer.enforce_bandwidth(TenantId::new(1), 1_000_000),
            Err(QuotaError::ShuttingDown)
        ));
    }
}

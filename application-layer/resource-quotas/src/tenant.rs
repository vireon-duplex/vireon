//! [`TenantId`], [`TenantTier`] — tenant identification and classification.
//!
//! See `DESIGN.md` Part 4 (tenants).
//!
//! # Design
//!
//! - `TenantId`: 64-bit identifier for a tenant (similar to SessionId)
//! - `TenantTier`: Classification tier with default limits

// ── TenantId ─────────────────────────────────────────────────────

/// A tenant identifier (64-bit).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct TenantId(u64);

impl TenantId {
    /// The nil/unset tenant ID (all zeros).
    pub const NIL: Self = Self(0);

    /// Construct a new tenant ID from a u64.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Construct the nil tenant ID.
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this tenant ID is nil.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        self.0 == 0
    }

    /// Get the raw u64 value.
    #[must_use]
    pub const fn raw(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ── TenantTier ──────────────────────────────────────────────────

/// Tenant tier classification with default limits.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TenantTier {
    /// Free tier: lowest limits, best-effort.
    Free = 0,
    /// Pro tier: moderate limits, priority support.
    Pro = 1,
    /// Enterprise tier: high limits, SLA-backed.
    Enterprise = 2,
    /// Internal tier: maximum limits, internal use.
    #[default]
    Internal = 3,
}

impl TenantTier {
    /// Human-readable name for the tier.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Free => "free",
            Self::Pro => "pro",
            Self::Enterprise => "enterprise",
            Self::Internal => "internal",
        }
    }

    /// Default session limit for this tier.
    #[must_use]
    pub const fn default_session_limit(&self) -> u32 {
        match self {
            Self::Free => 100,
            Self::Pro => 1_000,
            Self::Enterprise => 10_000,
            Self::Internal => 100_000,
        }
    }

    /// Default bandwidth limit (bps) for this tier.
    #[must_use]
    pub const fn default_bandwidth_limit_bps(&self) -> u64 {
        match self {
            Self::Free => 10_000_000,          // 10 Mbps
            Self::Pro => 100_000_000,          // 100 Mbps
            Self::Enterprise => 1_000_000_000, // 1 Gbps
            Self::Internal => 10_000_000_000,  // 10 Gbps
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_tenant_id() {
        let id = TenantId::nil();
        assert!(id.is_nil());
        assert_eq!(id.raw(), 0);
    }

    #[test]
    fn new_tenant_id() {
        let id = TenantId::new(12345);
        assert!(!id.is_nil());
        assert_eq!(id.raw(), 12345);
    }

    #[test]
    fn tenant_id_copy() {
        let a = TenantId::new(42);
        let b = a;
        assert_eq!(a, b);
        assert_eq!(b.raw(), 42);
    }

    #[test]
    fn tenant_tier_names() {
        assert_eq!(TenantTier::Free.as_str(), "free");
        assert_eq!(TenantTier::Pro.as_str(), "pro");
        assert_eq!(TenantTier::Enterprise.as_str(), "enterprise");
        assert_eq!(TenantTier::Internal.as_str(), "internal");
    }

    #[test]
    fn tenant_tier_session_limits() {
        assert_eq!(TenantTier::Free.default_session_limit(), 100);
        assert_eq!(TenantTier::Pro.default_session_limit(), 1_000);
        assert_eq!(TenantTier::Enterprise.default_session_limit(), 10_000);
        assert_eq!(TenantTier::Internal.default_session_limit(), 100_000);
    }

    #[test]
    fn tenant_tier_bandwidth_limits() {
        assert_eq!(TenantTier::Free.default_bandwidth_limit_bps(), 10_000_000);
        assert_eq!(TenantTier::Pro.default_bandwidth_limit_bps(), 100_000_000);
        assert_eq!(
            TenantTier::Enterprise.default_bandwidth_limit_bps(),
            1_000_000_000
        );
        assert_eq!(
            TenantTier::Internal.default_bandwidth_limit_bps(),
            10_000_000_000
        );
    }

    #[test]
    fn tenant_tier_default() {
        assert_eq!(TenantTier::default(), TenantTier::Internal);
    }

    #[test]
    fn const_comparison_works() {
        // Test that we can compare in const contexts using raw().
        const A: TenantId = TenantId::new(1);
        const B: TenantId = TenantId::new(2);
        assert!(A.raw() < B.raw());
    }
}

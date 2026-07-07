//! [`LeaseInfo`], [`LeaseState`] — per-session ownership leases.
//!
//! See `DESIGN.md` Part 5 (lease & quorum) and Part 14 (ownership
//! transfer).
//!
//! # Why leases?
//!
//! Without leases, a network partition can cause split-brain: two
//! nodes both believe they own a session. Leases solve this by
//! giving every ownership a bounded TTL — only one node can hold
//! a live (non-expired) lease at a time.
//!
//! The lease is renewed periodically by the owner; if the owner
//! crashes, the lease expires and ownership can be transferred.

use crate::node::NodeId;
use crate::version::Generation;

// ── Constants ───────────────────────────────────────────────────

/// Default lease TTL (ms). 30 seconds balances failover speed
/// against renewal traffic.
pub const DEFAULT_LEASE_TTL_MS: u64 = 30_000;

/// Minimum lease TTL (ms). 1 second — anything shorter causes
/// excessive renewal traffic.
pub const MIN_LEASE_TTL_MS: u64 = 1_000;

/// Maximum lease TTL (ms). 5 minutes — anything longer delays
/// failover unacceptably.
pub const MAX_LEASE_TTL_MS: u64 = 300_000;

/// Default renewal interval = TTL / 3. Renewing at TTL/3 gives
/// two missed renewals before expiry.
pub const DEFAULT_RENEWAL_INTERVAL_DIVISOR: u64 = 3;

// ── LeaseInfo ───────────────────────────────────────────────────

/// A lease on a session's ownership.
///
/// The owner must renew before `expiry_ns`; otherwise the lease
/// expires and ownership can be transferred.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LeaseInfo {
    /// The node currently holding the lease.
    pub owner: NodeId,
    /// Ownership generation at which this lease was issued.
    pub generation: Generation,
    /// Nanoseconds since cluster epoch when the lease expires.
    pub expiry_ns: u64,
    /// Lease TTL in milliseconds.
    pub ttl_ms: u64,
}

impl LeaseInfo {
    /// Construct a new lease issued at `now_ns` with the given TTL.
    #[must_use]
    pub const fn new(owner: NodeId, generation: Generation, now_ns: u64, ttl_ms: u64) -> Self {
        Self {
            owner,
            generation,
            expiry_ns: now_ns + (ttl_ms.saturating_mul(1_000_000)),
            ttl_ms,
        }
    }

    /// Whether the lease has expired at `now_ns`.
    #[must_use]
    pub const fn is_expired(self, now_ns: u64) -> bool {
        self.expiry_ns <= now_ns
    }

    /// Remaining time (ms) before expiry. Returns 0 if expired.
    #[must_use]
    pub const fn remaining_ms(self, now_ns: u64) -> u64 {
        if self.expiry_ns <= now_ns {
            0
        } else {
            (self.expiry_ns - now_ns) / 1_000_000
        }
    }

    /// Remaining time (ns) before expiry. Returns 0 if expired.
    #[must_use]
    pub const fn remaining_ns(self, now_ns: u64) -> u64 {
        self.expiry_ns.saturating_sub(now_ns)
    }

    /// Renew the lease from `now_ns` for another TTL.
    /// Caller must verify ownership generation hasn't changed.
    #[must_use]
    pub const fn renew(self, now_ns: u64) -> Self {
        Self {
            owner: self.owner,
            generation: self.generation,
            expiry_ns: now_ns + (self.ttl_ms.saturating_mul(1_000_000)),
            ttl_ms: self.ttl_ms,
        }
    }

    /// The renewal interval (ms) — when to schedule the next
    /// renewal attempt. Defaults to TTL / 3.
    #[must_use]
    pub const fn renewal_interval_ms(self) -> u64 {
        self.ttl_ms / DEFAULT_RENEWAL_INTERVAL_DIVISOR
    }

    /// Classify the lease state at `now_ns`.
    #[must_use]
    pub fn state(self, now_ns: u64) -> LeaseState {
        if self.is_expired(now_ns) {
            LeaseState::Expired
        } else {
            let remaining = self.remaining_ms(now_ns);
            let quarter_ttl = self.ttl_ms / 4;
            if remaining < quarter_ttl {
                LeaseState::Expiring
            } else {
                LeaseState::Active
            }
        }
    }
}

// ── LeaseState ──────────────────────────────────────────────────

/// Lease lifecycle state. Derived from `LeaseInfo::state()`.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum LeaseState {
    /// Lease has plenty of remaining time (>25% TTL).
    #[default]
    Active,
    /// Lease is about to expire (<25% TTL remaining).
    Expiring,
    /// Lease has expired; ownership can be reclaimed.
    Expired,
    /// Lease was explicitly revoked (ownership transferred early).
    Revoked,
}

impl LeaseState {
    /// Whether the lease is currently valid (not expired or revoked).
    #[must_use]
    pub const fn is_valid(self) -> bool {
        matches!(self, Self::Active | Self::Expiring)
    }

    /// Whether the lease needs renewal now.
    #[must_use]
    pub const fn needs_renewal(self) -> bool {
        matches!(self, Self::Expiring | Self::Expired)
    }

    /// Whether the lease is terminal (no longer renewable).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Expired | Self::Revoked)
    }

    /// Human-readable name for metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Expiring => "expiring",
            Self::Expired => "expired",
            Self::Revoked => "revoked",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_TTL_MS: u64 = 10_000; // 10 seconds

    fn lease_at(now_ns: u64) -> LeaseInfo {
        LeaseInfo::new(NodeId::new(1), Generation::INITIAL, now_ns, TEST_TTL_MS)
    }

    #[test]
    fn new_lease_is_active() {
        let l = lease_at(0);
        assert_eq!(l.state(0), LeaseState::Active);
        assert!(!l.is_expired(0));
        assert_eq!(l.remaining_ms(0), TEST_TTL_MS);
    }

    #[test]
    fn lease_expires() {
        let l = lease_at(0);
        // At expiry_ns + 1 ns, lease is expired.
        assert!(l.is_expired(l.expiry_ns));
        assert!(l.is_expired(l.expiry_ns + 1));
        assert_eq!(l.state(l.expiry_ns), LeaseState::Expired);
    }

    #[test]
    fn lease_enter_expiring_state() {
        let l = lease_at(0);
        // At 76% TTL (7.6s of 10s), remaining_ms = 2_400 < 2_500 (25%).
        // remaining_ms at 7_600_000_000 ns = (10_000_000_000 - 7_600_000_000)/1_000_000 = 2_400 ms
        let now = 7_600_000_000;
        assert_eq!(l.remaining_ms(now), 2_400);
        assert_eq!(l.state(now), LeaseState::Expiring);
    }

    #[test]
    fn lease_renew_extends_expiry() {
        let l = lease_at(0);
        let renewed = l.renew(5_000_000_000); // renew at 5s
        // Original expiry = 10s; renewed expiry = 5s + 10s = 15s.
        assert_eq!(renewed.expiry_ns, 15_000_000_000);
        assert_eq!(renewed.generation, l.generation);
        assert_eq!(renewed.owner, l.owner);
    }

    #[test]
    fn renewal_interval_is_third_of_ttl() {
        let l = lease_at(0);
        assert_eq!(l.renewal_interval_ms(), TEST_TTL_MS / 3);
    }

    #[test]
    fn remaining_ns_and_ms_consistent() {
        let l = lease_at(0);
        let now = 3_000_000_000; // 3s
        let ns = l.remaining_ns(now);
        let ms = l.remaining_ms(now);
        assert_eq!(ns / 1_000_000, ms);
    }

    #[test]
    fn expired_lease_remaining_zero() {
        let l = lease_at(0);
        assert_eq!(l.remaining_ms(l.expiry_ns + 1_000_000), 0);
        assert_eq!(l.remaining_ns(l.expiry_ns + 1_000_000), 0);
    }

    #[test]
    fn lease_state_predicates() {
        assert!(LeaseState::Active.is_valid());
        assert!(LeaseState::Expiring.is_valid());
        assert!(!LeaseState::Expired.is_valid());
        assert!(!LeaseState::Revoked.is_valid());

        assert!(LeaseState::Expiring.needs_renewal());
        assert!(LeaseState::Expired.needs_renewal());
        assert!(!LeaseState::Active.needs_renewal());

        assert!(LeaseState::Expired.is_terminal());
        assert!(LeaseState::Revoked.is_terminal());
        assert!(!LeaseState::Active.is_terminal());
    }

    #[test]
    fn lease_state_as_str() {
        assert_eq!(LeaseState::Active.as_str(), "active");
        assert_eq!(LeaseState::Expired.as_str(), "expired");
        assert_eq!(LeaseState::Revoked.as_str(), "revoked");
    }

    #[test]
    fn ttl_constants_ordered() {
        assert!(MIN_LEASE_TTL_MS < DEFAULT_LEASE_TTL_MS);
        assert!(DEFAULT_LEASE_TTL_MS < MAX_LEASE_TTL_MS);
    }
}

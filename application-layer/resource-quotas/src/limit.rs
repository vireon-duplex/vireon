//! [`QuotaLimit`] — resource quota limits (Copy + bounds).
//!
//! See `DESIGN.md` Part 5 (quota limits).
//!
//! # Design
//!
//! Quota limits are `Copy + Clone + Debug + PartialEq + Eq` to enable
//! easy comparison and storage. All limits are inclusive (≤).

use crate::tenant::TenantTier;

// ── QuotaLimit ───────────────────────────────────────────────────

/// Resource quota limits for a tenant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuotaLimit {
    /// Maximum sessions allowed.
    pub max_sessions: u32,
    /// Maximum streams allowed.
    pub max_streams: u32,
    /// Maximum bandwidth (bps).
    pub max_bandwidth_bps: u64,
    /// Maximum memory (bytes).
    pub max_memory_bytes: u64,
    /// Maximum requests per second.
    pub max_requests_per_sec: u32,
}

impl QuotaLimit {
    /// Maximum possible value for sessions (hard cap).
    pub const MAX_SESSIONS: u32 = 1_000_000;
    /// Maximum possible value for streams (hard cap).
    pub const MAX_STREAMS: u32 = 10_000_000;
    /// Maximum possible value for bandwidth (bps, hard cap).
    pub const MAX_BANDWIDTH_BPS: u64 = 100_000_000_000; // 100 Gbps
    /// Maximum possible value for memory (bytes, hard cap).
    pub const MAX_MEMORY_BYTES: u64 = 1_000_000_000_000; // 1 TB
    /// Maximum possible value for requests/sec (hard cap).
    pub const MAX_REQUESTS_PER_SEC: u32 = 1_000_000;

    /// Construct a new quota limit with all fields set to maximum.
    #[must_use]
    pub const fn unlimited() -> Self {
        Self {
            max_sessions: Self::MAX_SESSIONS,
            max_streams: Self::MAX_STREAMS,
            max_bandwidth_bps: Self::MAX_BANDWIDTH_BPS,
            max_memory_bytes: Self::MAX_MEMORY_BYTES,
            max_requests_per_sec: Self::MAX_REQUESTS_PER_SEC,
        }
    }

    /// Construct a new quota limit from the given values.
    #[must_use]
    pub const fn new(
        max_sessions: u32,
        max_streams: u32,
        max_bandwidth_bps: u64,
        max_memory_bytes: u64,
        max_requests_per_sec: u32,
    ) -> Self {
        Self {
            max_sessions,
            max_streams,
            max_bandwidth_bps,
            max_memory_bytes,
            max_requests_per_sec,
        }
    }

    /// Construct a quota limit from tier defaults.
    #[must_use]
    pub const fn for_tier(tier: TenantTier) -> Self {
        Self {
            max_sessions: tier.default_session_limit(),
            max_streams: tier.default_session_limit() * 10, // 10x sessions
            max_bandwidth_bps: tier.default_bandwidth_limit_bps(),
            max_memory_bytes: 1_000_000_000, // 1 GB default
            max_requests_per_sec: 1_000,     // 1K RPS default
        }
    }

    /// Merge this limit with another, taking the minimum of each field.
    #[must_use]
    pub const fn merge(&self, other: Self) -> Self {
        Self {
            max_sessions: if self.max_sessions < other.max_sessions {
                self.max_sessions
            } else {
                other.max_sessions
            },
            max_streams: if self.max_streams < other.max_streams {
                self.max_streams
            } else {
                other.max_streams
            },
            max_bandwidth_bps: if self.max_bandwidth_bps < other.max_bandwidth_bps {
                self.max_bandwidth_bps
            } else {
                other.max_bandwidth_bps
            },
            max_memory_bytes: if self.max_memory_bytes < other.max_memory_bytes {
                self.max_memory_bytes
            } else {
                other.max_memory_bytes
            },
            max_requests_per_sec: if self.max_requests_per_sec < other.max_requests_per_sec {
                self.max_requests_per_sec
            } else {
                other.max_requests_per_sec
            },
        }
    }
}

impl Default for QuotaLimit {
    fn default() -> Self {
        Self::for_tier(TenantTier::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_has_max_values() {
        let limit = QuotaLimit::unlimited();
        assert_eq!(limit.max_sessions, QuotaLimit::MAX_SESSIONS);
        assert_eq!(limit.max_streams, QuotaLimit::MAX_STREAMS);
        assert_eq!(limit.max_bandwidth_bps, QuotaLimit::MAX_BANDWIDTH_BPS);
    }

    #[test]
    fn new_constructs_limit() {
        let limit = QuotaLimit::new(100, 1000, 100_000_000, 1_000_000_000, 500);
        assert_eq!(limit.max_sessions, 100);
        assert_eq!(limit.max_streams, 1000);
        assert_eq!(limit.max_bandwidth_bps, 100_000_000);
    }

    #[test]
    fn for_tier_uses_tier_defaults() {
        let limit = QuotaLimit::for_tier(TenantTier::Pro);
        assert_eq!(limit.max_sessions, 1_000);
        assert_eq!(limit.max_streams, 10_000);
        assert_eq!(limit.max_bandwidth_bps, 100_000_000);
    }

    #[test]
    fn merge_takes_minimum() {
        let a = QuotaLimit::new(100, 1000, 100_000_000, 1_000_000_000, 500);
        let b = QuotaLimit::new(200, 500, 200_000_000, 500_000_000, 1000);
        let merged = a.merge(b);
        assert_eq!(merged.max_sessions, 100); // min(100, 200)
        assert_eq!(merged.max_streams, 500); // min(1000, 500)
        assert_eq!(merged.max_bandwidth_bps, 100_000_000); // min(...)
    }

    #[test]
    fn limit_is_copy() {
        let a = QuotaLimit::new(100, 1000, 100_000_000, 1_000_000_000, 500);
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn limit_is_partial_eq() {
        let a = QuotaLimit::new(100, 1000, 100_000_000, 1_000_000_000, 500);
        let b = QuotaLimit::new(100, 1000, 100_000_000, 1_000_000_000, 500);
        assert_eq!(a, b);

        let c = QuotaLimit::new(200, 1000, 100_000_000, 1_000_000_000, 500);
        assert_ne!(a, c);
    }

    #[test]
    fn default_is_internal_tier() {
        let limit = QuotaLimit::default();
        let internal = QuotaLimit::for_tier(TenantTier::Internal);
        assert_eq!(limit.max_sessions, internal.max_sessions);
        assert_eq!(limit.max_streams, internal.max_streams);
    }
}

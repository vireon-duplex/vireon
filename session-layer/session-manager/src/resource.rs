//! [`ResourceManager`] — owns per-session quotas + token bucket.
//!
//! See `DESIGN.md` Part 11 (resource management).
//!
//! # Role
//!
//! The resource manager wraps [`ResourceQuota`](crate::quota::ResourceQuota)
//! with:
//!
//! - **Token bucket** — for bandwidth rate limiting.
//! - **Leaky bucket** — for burst smoothing.
//! - **Admission control** — reject new sessions on capacity
//!   pressure.
//! - **Garbage collection** — reclaim quota from destroyed
//!   sessions.
//!
//! # Phase 0 status
//!
//! Phase 0 ships the type signature + token-bucket state shape.
//! Functional rate limiting lands in Phase 2+.

use crate::config::SessionManagerConfig;
use crate::error::SessionManagerError;
use crate::id::SessionId;

/// Token-bucket state for bandwidth rate limiting.
#[derive(Clone, Copy, Debug, Default)]
pub struct TokenBucket {
    /// Current token count (bytes).
    pub tokens: u64,
    /// Last refill time (ns since manager start).
    pub last_refill_ns: u64,
    /// Maximum bucket capacity (burst size, bytes).
    pub capacity: u64,
    /// Refill rate (bytes/sec).
    pub refill_rate_bps: u64,
}

impl TokenBucket {
    /// Construct a new token bucket.
    #[must_use]
    pub const fn new(capacity: u64, refill_rate_bps: u64) -> Self {
        Self {
            tokens: capacity,
            last_refill_ns: 0,
            capacity,
            refill_rate_bps,
        }
    }

    /// Whether the bucket has enough tokens for `bytes`.
    #[must_use]
    pub const fn has_capacity(&self, bytes: u64) -> bool {
        self.tokens >= bytes
    }
}

/// Resource manager.
///
/// Phase 0 holds no state; Phase 2+ will add per-session quotas and
/// token buckets.
// TODO(phase-2): per-session quotas + token buckets.
#[derive(Clone, Debug, Default)]
pub struct ResourceManager {
    /// Stashed for Phase 2+ rate limiting; intentionally unread.
    #[allow(dead_code)]
    cfg: Option<SessionManagerConfig>,
}

impl ResourceManager {
    /// Construct a new manager from a config.
    #[must_use]
    pub fn new(cfg: SessionManagerConfig) -> Self {
        Self { cfg: Some(cfg) }
    }

    /// Phase-2 `allocate` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn allocate(&mut self, _id: SessionId) -> Result<(), SessionManagerError> {
        // TODO(phase-2): install quota + token bucket.
        Err(SessionManagerError::ResourceExhausted(
            "resource manager phase-2 stub",
        ))
    }

    /// Phase-2 `release` placeholder.
    pub fn release(&mut self, _id: SessionId) {
        // TODO(phase-2): reclaim quota + token bucket.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket_new_full() {
        let b = TokenBucket::new(1_000, 100);
        assert_eq!(b.tokens, 1_000);
        assert!(b.has_capacity(500));
        assert!(!b.has_capacity(1_001));
    }

    #[test]
    fn resource_manager_phase0_stub() {
        let mut m = ResourceManager::new(SessionManagerConfig::default());
        assert!(m.allocate(SessionId::nil()).is_err());
    }

    #[test]
    fn release_does_not_panic() {
        let mut m = ResourceManager::new(SessionManagerConfig::default());
        m.release(SessionId::nil());
    }
}

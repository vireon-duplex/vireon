//! [`ResourceManager`] — per-session token-bucket rate limiting.
//!
//! See `DESIGN.md` Part 11 (resource management).
//!
//! # Role
//!
//! Owns one [`TokenBucket`] per active session, configured from
//! [`SessionManagerConfig`] at allocation time. The bucket capacity
//! matches `max_bandwidth_per_session` (1-second burst) and the refill
//! rate matches the same value (sustained rate).

use rustc_hash::FxHashMap;

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
    /// Construct a new token bucket, initialised to full capacity.
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

    /// Refills the bucket based on elapsed time since `last_refill_ns`.
    /// Adds `elapsed_ns * refill_rate_bps / 1_000_000_000` tokens,
    /// capped at `capacity`.
    pub fn refill(&mut self, now_ns: u64) {
        if now_ns <= self.last_refill_ns {
            return;
        }
        let elapsed_ns = now_ns - self.last_refill_ns;
        let added = (elapsed_ns / 1_000_000_000) * self.refill_rate_bps
            + (elapsed_ns % 1_000_000_000) * self.refill_rate_bps / 1_000_000_000;
        self.tokens = self.tokens.saturating_add(added).min(self.capacity);
        self.last_refill_ns = now_ns;
    }

    /// Attempts to consume `bytes` tokens. Returns `false` if the
    /// bucket does not have enough capacity (the caller should apply
    /// backpressure). On success, deducts the tokens.
    pub fn try_consume(&mut self, bytes: u64) -> bool {
        if self.tokens >= bytes {
            self.tokens -= bytes;
            true
        } else {
            false
        }
    }
}

/// Resource manager — owns per-session token buckets.
#[derive(Debug)]
pub struct ResourceManager {
    cfg: SessionManagerConfig,
    buckets: FxHashMap<SessionId, TokenBucket>,
}

impl Default for ResourceManager {
    fn default() -> Self {
        Self::new(SessionManagerConfig::default())
    }
}

impl ResourceManager {
    /// Construct a new manager from a config.
    #[must_use]
    pub fn new(cfg: SessionManagerConfig) -> Self {
        Self {
            cfg,
            buckets: FxHashMap::default(),
        }
    }

    /// Allocate a token bucket for `id`, configured from the manager's
    /// `max_bandwidth_per_session` setting.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::ResourceExhausted`] if a bucket
    /// for `id` already exists.
    pub fn allocate(&mut self, id: SessionId) -> Result<(), SessionManagerError> {
        if self.buckets.contains_key(&id) {
            return Err(SessionManagerError::ResourceExhausted(
                "token bucket already allocated for this session",
            ));
        }
        let rate = self.cfg.max_bandwidth_per_session;
        self.buckets.insert(id, TokenBucket::new(rate, rate));
        Ok(())
    }

    /// Release the token bucket for `id` (no-op if not found).
    pub fn release(&mut self, id: SessionId) {
        self.buckets.remove(&id);
    }

    /// Attempts to consume `bytes` from the session's bucket. Refills
    /// first based on elapsed time, then checks capacity.
    ///
    /// Returns `false` if the session has no bucket or the bucket is
    /// empty (the caller should apply backpressure).
    pub fn try_consume(&mut self, id: SessionId, bytes: u64, now_ns: u64) -> bool {
        let Some(bucket) = self.buckets.get_mut(&id) else {
            return false;
        };
        bucket.refill(now_ns);
        bucket.try_consume(bytes)
    }

    /// Refills the session's bucket up to the current time.
    pub fn refill(&mut self, id: SessionId, now_ns: u64) {
        if let Some(bucket) = self.buckets.get_mut(&id) {
            bucket.refill(now_ns);
        }
    }

    /// Number of active token buckets.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buckets.len()
    }

    /// Whether the manager has no active buckets.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn token_bucket_new_full() {
        let b = TokenBucket::new(1_000, 100);
        assert_eq!(b.tokens, 1_000);
        assert!(b.has_capacity(500));
        assert!(!b.has_capacity(1_001));
    }

    #[test]
    fn token_bucket_consume_and_refill() {
        let mut b = TokenBucket::new(1_000, 1_000); // 1000 bytes/sec
        assert!(b.try_consume(800));
        assert_eq!(b.tokens, 200);
        assert!(!b.try_consume(300)); // not enough
        // Refill after 1 second.
        b.last_refill_ns = 0;
        b.refill(1_000_000_000);
        assert_eq!(b.tokens, 1_000); // back to full
    }

    #[test]
    fn token_bucket_refill_caps_at_capacity() {
        let mut b = TokenBucket::new(500, 1_000);
        b.tokens = 400;
        b.last_refill_ns = 0;
        b.refill(1_000_000_000); // would add 1000, capped at 500
        assert_eq!(b.tokens, 500);
    }

    #[test]
    fn allocate_creates_bucket() {
        let mut m = ResourceManager::new(SessionManagerConfig::default());
        assert!(m.allocate(SessionId::nil()).is_ok());
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn allocate_duplicate_returns_error() {
        let mut m = ResourceManager::new(SessionManagerConfig::default());
        m.allocate(SessionId::nil()).unwrap();
        assert!(m.allocate(SessionId::nil()).is_err());
    }

    #[test]
    fn release_removes_bucket() {
        let mut m = ResourceManager::new(SessionManagerConfig::default());
        m.allocate(SessionId::nil()).unwrap();
        assert_eq!(m.len(), 1);
        m.release(SessionId::nil());
        assert!(m.is_empty());
    }

    #[test]
    fn try_consume_unknown_session_returns_false() {
        let mut m = ResourceManager::new(SessionManagerConfig::default());
        assert!(!m.try_consume(SessionId::nil(), 10, 0));
    }

    #[test]
    fn try_consume_existing_session_works() {
        let mut m = ResourceManager::new(SessionManagerConfig::default());
        let id = SessionId::from_pair(1, 1);
        m.allocate(id).unwrap();
        // Default bandwidth is 1_310_720 bytes/sec.
        assert!(m.try_consume(id, 1_000_000, 0));
    }
}

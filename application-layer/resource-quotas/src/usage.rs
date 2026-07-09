//! [`QuotaUsage`], [`UsageSnapshot`] — atomic usage tracking.
//!
//! See `DESIGN.md` Part 6 (usage tracking).
//!
//! # Design
//!
//! Usage counters are stored as `AtomicU64` with `Relaxed` ordering:
//! usage tracking is advisory for rate limiting, not load-bearing,
//! so we trade synchronization cost for throughput.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::QuotaError;
use crate::limit::QuotaLimit;

// ── QuotaUsage ───────────────────────────────────────────────────

/// Per-tenant resource usage tracking with atomic counters.
#[derive(Debug, Clone)]
pub struct QuotaUsage {
    /// Sessions currently in use.
    pub sessions_used: Arc<AtomicU64>,
    /// Streams currently in use.
    pub streams_used: Arc<AtomicU64>,
    /// Bandwidth currently used (bps).
    pub bandwidth_used_bps: Arc<AtomicU64>,
    /// Memory currently used (bytes).
    pub memory_used_bytes: Arc<AtomicU64>,
    /// Requests this second (reset periodically).
    pub requests_this_second: Arc<AtomicU64>,
}

impl QuotaUsage {
    /// Construct a new zeroed usage instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions_used: Arc::new(AtomicU64::new(0)),
            streams_used: Arc::new(AtomicU64::new(0)),
            bandwidth_used_bps: Arc::new(AtomicU64::new(0)),
            memory_used_bytes: Arc::new(AtomicU64::new(0)),
            requests_this_second: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record a session being added.
    pub fn record_session_add(&self) {
        self.sessions_used.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a session being removed.
    pub fn record_session_remove(&self) {
        self.sessions_used.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record bandwidth being used (bytes).
    pub fn record_bandwidth(&self, bytes: u64) {
        self.bandwidth_used_bps.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a request being made.
    pub fn record_request(&self) {
        self.requests_this_second.fetch_add(1, Ordering::Relaxed);
    }

    /// Reset the request counter (call once per second).
    pub fn reset_request_counter(&self) {
        self.requests_this_second.store(0, Ordering::Relaxed);
    }

    /// Take a snapshot of current usage.
    #[must_use]
    pub fn snapshot(&self) -> UsageSnapshot {
        UsageSnapshot {
            sessions_used: self.sessions_used.load(Ordering::Relaxed),
            streams_used: self.streams_used.load(Ordering::Relaxed),
            bandwidth_used_bps: self.bandwidth_used_bps.load(Ordering::Relaxed),
            memory_used_bytes: self.memory_used_bytes.load(Ordering::Relaxed),
            requests_this_second: self.requests_this_second.load(Ordering::Relaxed),
        }
    }

    /// Check usage against a limit.
    ///
    /// # Errors
    /// Returns [`QuotaError::LimitExceeded`] if any usage field
    /// exceeds the corresponding limit.
    pub fn check_against(&self, limit: &QuotaLimit) -> Result<(), QuotaError> {
        let snap = self.snapshot();
        snap.check_against(limit)
    }
}

impl Default for QuotaUsage {
    fn default() -> Self {
        Self::new()
    }
}

// ── UsageSnapshot ───────────────────────────────────────────────

/// Point-in-time snapshot of resource usage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UsageSnapshot {
    /// Sessions currently in use.
    pub sessions_used: u64,
    /// Streams currently in use.
    pub streams_used: u64,
    /// Bandwidth currently used (bps).
    pub bandwidth_used_bps: u64,
    /// Memory currently used (bytes).
    pub memory_used_bytes: u64,
    /// Requests this second.
    pub requests_this_second: u64,
}

impl UsageSnapshot {
    /// Check usage against a limit.
    ///
    /// # Errors
    /// Returns [`QuotaError::LimitExceeded`] if any usage field
    /// exceeds the corresponding limit.
    pub fn check_against(&self, limit: &QuotaLimit) -> Result<(), QuotaError> {
        if self.sessions_used > limit.max_sessions as u64 {
            return Err(QuotaError::LimitExceeded("max_sessions"));
        }
        if self.streams_used > limit.max_streams as u64 {
            return Err(QuotaError::LimitExceeded("max_streams"));
        }
        if self.bandwidth_used_bps > limit.max_bandwidth_bps {
            return Err(QuotaError::LimitExceeded("max_bandwidth_bps"));
        }
        if self.memory_used_bytes > limit.max_memory_bytes {
            return Err(QuotaError::LimitExceeded("max_memory_bytes"));
        }
        if self.requests_this_second > limit.max_requests_per_sec as u64 {
            return Err(QuotaError::LimitExceeded("max_requests_per_sec"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_usage_is_zero() {
        let usage = QuotaUsage::new();
        let snap = usage.snapshot();
        assert_eq!(snap, UsageSnapshot::default());
    }

    #[test]
    fn record_session_add_increments_counter() {
        let usage = QuotaUsage::new();
        usage.record_session_add();
        usage.record_session_add();
        let snap = usage.snapshot();
        assert_eq!(snap.sessions_used, 2);
    }

    #[test]
    fn record_session_remove_decrements_counter() {
        let usage = QuotaUsage::new();
        usage.record_session_add();
        usage.record_session_add();
        usage.record_session_remove();
        let snap = usage.snapshot();
        assert_eq!(snap.sessions_used, 1);
    }

    #[test]
    fn record_bandwidth_increments_counter() {
        let usage = QuotaUsage::new();
        usage.record_bandwidth(1_000_000);
        usage.record_bandwidth(2_000_000);
        let snap = usage.snapshot();
        assert_eq!(snap.bandwidth_used_bps, 3_000_000);
    }

    #[test]
    fn record_request_increments_counter() {
        let usage = QuotaUsage::new();
        usage.record_request();
        usage.record_request();
        usage.record_request();
        let snap = usage.snapshot();
        assert_eq!(snap.requests_this_second, 3);
    }

    #[test]
    fn reset_request_counter_clears_counter() {
        let usage = QuotaUsage::new();
        usage.record_request();
        usage.record_request();
        usage.reset_request_counter();
        let snap = usage.snapshot();
        assert_eq!(snap.requests_this_second, 0);
    }

    #[test]
    fn check_against_passes_when_under_limit() {
        let usage = QuotaUsage::new();
        let limit = QuotaLimit::new(100, 1000, 100_000_000, 1_000_000_000, 500);
        assert!(usage.check_against(&limit).is_ok());
    }

    #[test]
    fn check_against_fails_when_sessions_exceeded() {
        let usage = QuotaUsage::new();
        usage.sessions_used.store(101, Ordering::Relaxed);
        let limit = QuotaLimit::new(100, 1000, 100_000_000, 1_000_000_000, 500);
        assert!(matches!(
            usage.check_against(&limit),
            Err(QuotaError::LimitExceeded("max_sessions"))
        ));
    }

    #[test]
    fn check_against_fails_when_streams_exceeded() {
        let usage = QuotaUsage::new();
        usage.streams_used.store(1001, Ordering::Relaxed);
        let limit = QuotaLimit::new(100, 1000, 100_000_000, 1_000_000_000, 500);
        assert!(matches!(
            usage.check_against(&limit),
            Err(QuotaError::LimitExceeded("max_streams"))
        ));
    }

    #[test]
    fn check_against_fails_when_bandwidth_exceeded() {
        let usage = QuotaUsage::new();
        usage
            .bandwidth_used_bps
            .store(101_000_000, Ordering::Relaxed);
        let limit = QuotaLimit::new(100, 1000, 100_000_000, 1_000_000_000, 500);
        assert!(matches!(
            usage.check_against(&limit),
            Err(QuotaError::LimitExceeded("max_bandwidth_bps"))
        ));
    }

    #[test]
    fn check_against_fails_when_requests_exceeded() {
        let usage = QuotaUsage::new();
        usage.requests_this_second.store(501, Ordering::Relaxed);
        let limit = QuotaLimit::new(100, 1000, 100_000_000, 1_000_000_000, 500);
        assert!(matches!(
            usage.check_against(&limit),
            Err(QuotaError::LimitExceeded("max_requests_per_sec"))
        ));
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let usage = QuotaUsage::new();
        usage.record_session_add();
        let a = usage.snapshot();
        let b = usage.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn usage_clone_shares_state() {
        let usage = QuotaUsage::new();
        let usage_clone = usage.clone();
        usage.record_session_add();
        assert_eq!(usage_clone.snapshot().sessions_used, 1);
    }
}

//! [`RoutingCacheStats`], [`RoutingCache`] — TTL-based routing
//! cache with atomic counters.
//!
//! See `DESIGN.md` Part 3 (routing cache) and Part 9 (cache
//! invalidation).
//!
//! # Design
//!
//! The routing cache sits in front of the registry and provides
//! O(1) lookups in the steady state (cache hit). Entries are
//! TTL-evicted to avoid serving stale routes after ownership
//! changes.
//!
//! Stats counters use `AtomicU64` with `Relaxed` ordering: stats
//! are advisory, never load-bearing, so we trade synchronization
//! cost for throughput.
//!
//! # Phase 0
//!
//! Stats are fully implemented (real atomic counters). The cache
//! itself (`RoutingCache`) is a typed skeleton — `get`, `put`,
//! `invalidate`, and `clear` all return errors. Phase 1 adds the
//! real `HashMap`-backed cache.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::RouterError;
use session_manager::SessionId;
use session_registry::NodeId;

// ── RoutingCacheStats ───────────────────────────────────────────

/// Atomic counters for the routing cache.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`RoutingCacheStats::snapshot`] to get a
/// point-in-time view.
#[derive(Debug)]
pub struct RoutingCacheStats {
    /// Entries currently in the cache (gauge).
    pub entries: AtomicU64,
    /// Cache hits (counter).
    pub hits: AtomicU64,
    /// Cache misses (counter).
    pub misses: AtomicU64,
    /// Entries evicted due to capacity (counter).
    pub evictions: AtomicU64,
    /// Entries expired due to TTL (counter).
    pub expirations: AtomicU64,
}

impl Default for RoutingCacheStats {
    fn default() -> Self {
        Self::new()
    }
}

impl RoutingCacheStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            expirations: AtomicU64::new(0),
        }
    }

    /// Record a cache hit.
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss.
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an eviction (capacity-based).
    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an expiration (TTL-based).
    pub fn record_expiration(&self) {
        self.expirations.fetch_add(1, Ordering::Relaxed);
    }

    /// Set the current entry count (gauge).
    pub fn set_entries(&self, count: u64) {
        self.entries.store(count, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields if updates are in
    /// flight. This is acceptable for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> CacheStatsSnapshot {
        CacheStatsSnapshot {
            entries: self.entries.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            expirations: self.expirations.load(Ordering::Relaxed),
        }
    }
}

// ── CacheStatsSnapshot ──────────────────────────────────────────

/// Point-in-time snapshot of routing cache counters.
///
/// All rates are precomputed as basis-points (bps, 1/100 of a
/// percent) so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStatsSnapshot {
    /// Entries currently in the cache.
    pub entries: u64,
    /// Total cache hits.
    pub hits: u64,
    /// Total cache misses.
    pub misses: u64,
    /// Total capacity-based evictions.
    pub evictions: u64,
    /// Total TTL-based expirations.
    pub expirations: u64,
}

impl CacheStatsSnapshot {
    /// Cache hit rate in basis points (0..=10_000).
    /// Returns 10_000 (100%) when there are no lookups yet.
    #[must_use]
    pub const fn hit_rate_bps(&self) -> u16 {
        let total = self.hits + self.misses;
        if total == 0 {
            return 10_000;
        }
        let rate = (self.hits * 10_000) / total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Total lookups (hits + misses).
    #[must_use]
    pub const fn total_lookups(&self) -> u64 {
        self.hits + self.misses
    }
}

// ── RoutingCache (Phase 1 skeleton) ─────────────────────────────

/// TTL-based routing cache for `SessionId -> NodeId` mappings.
///
/// Phase 0 ships only a typed skeleton. Phase 1 will add the
/// `HashMap`-backed implementation with TTL expiry, capacity
/// eviction, and invalidation hooks.
#[derive(Debug)]
pub struct RoutingCache {
    /// Static configuration (max entries, TTL).
    #[allow(dead_code)]
    max_entries: u32,
    /// TTL in milliseconds (0 = disabled).
    #[allow(dead_code)]
    ttl_ms: u64,
    /// Atomic observability counters.
    stats: RoutingCacheStats,
}

impl RoutingCache {
    /// Construct a cache with the given capacity and TTL.
    ///
    /// # Errors
    /// Never errors in Phase 0.
    pub fn new(max_entries: u32, ttl_ms: u64) -> Result<Self, RouterError> {
        Ok(Self {
            max_entries,
            ttl_ms,
            stats: RoutingCacheStats::new(),
        })
    }

    /// Look up a session in the cache.
    ///
    /// # Errors
    /// Always returns [`RouterError::ShuttingDown`] in Phase 0.
    pub fn get(&self, _id: SessionId) -> Result<NodeId, RouterError> {
        // TODO(phase-1): check HashMap + TTL validity.
        self.stats.record_miss();
        Err(RouterError::ShuttingDown)
    }

    /// Insert a routing entry into the cache.
    ///
    /// # Errors
    /// Always returns [`RouterError::ShuttingDown`] in Phase 0.
    pub fn put(&self, _id: SessionId, _node: NodeId) -> Result<(), RouterError> {
        // TODO(phase-1): insert into HashMap + update stats.
        Err(RouterError::ShuttingDown)
    }

    /// Invalidate a single cache entry.
    ///
    /// # Errors
    /// Always returns [`RouterError::ShuttingDown`] in Phase 0.
    pub fn invalidate(&self, _id: SessionId) -> Result<(), RouterError> {
        // TODO(phase-1): remove from HashMap.
        Err(RouterError::ShuttingDown)
    }

    /// Clear all cache entries.
    ///
    /// # Errors
    /// Always returns [`RouterError::ShuttingDown`] in Phase 0.
    pub fn clear(&self) -> Result<(), RouterError> {
        // TODO(phase-1): drain HashMap + reset gauge.
        Err(RouterError::ShuttingDown)
    }

    /// Reference to the underlying stats (for testing / metrics).
    #[must_use]
    pub fn stats(&self) -> &RoutingCacheStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = RoutingCacheStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, CacheStatsSnapshot::default());
    }

    #[test]
    fn record_hit_increments_hit_counter() {
        let s = RoutingCacheStats::new();
        s.record_hit();
        s.record_hit();
        let snap = s.snapshot();
        assert_eq!(snap.hits, 2);
        assert_eq!(snap.misses, 0);
    }

    #[test]
    fn record_miss_increments_miss_counter() {
        let s = RoutingCacheStats::new();
        s.record_miss();
        let snap = s.snapshot();
        assert_eq!(snap.hits, 0);
        assert_eq!(snap.misses, 1);
    }

    #[test]
    fn hit_rate_is_100_pct_with_no_lookups() {
        let s = RoutingCacheStats::new();
        assert_eq!(s.snapshot().hit_rate_bps(), 10_000);
    }

    #[test]
    fn hit_rate_reflects_mixed_hits_and_misses() {
        let s = RoutingCacheStats::new();
        for _ in 0..7 {
            s.record_hit();
        }
        for _ in 0..3 {
            s.record_miss();
        }
        // 7/10 = 70% = 7000 bps.
        assert_eq!(s.snapshot().hit_rate_bps(), 7_000);
    }

    #[test]
    fn eviction_and_expiration_counters() {
        let s = RoutingCacheStats::new();
        s.record_eviction();
        s.record_eviction();
        s.record_expiration();
        let snap = s.snapshot();
        assert_eq!(snap.evictions, 2);
        assert_eq!(snap.expirations, 1);
    }

    #[test]
    fn set_entries_updates_gauge() {
        let s = RoutingCacheStats::new();
        s.set_entries(500);
        assert_eq!(s.snapshot().entries, 500);
    }

    #[test]
    fn cache_get_phase0_stub() {
        let cache = RoutingCache::new(1000, 5_000).unwrap();
        let res = cache.get(SessionId::nil());
        assert!(matches!(res, Err(RouterError::ShuttingDown)));
        // Miss counter should be incremented.
        assert_eq!(cache.stats().snapshot().misses, 1);
    }

    #[test]
    fn cache_put_phase0_stub() {
        let cache = RoutingCache::new(1000, 5_000).unwrap();
        assert!(cache.put(SessionId::nil(), NodeId::new(1)).is_err());
    }

    #[test]
    fn cache_invalidate_phase0_stub() {
        let cache = RoutingCache::new(1000, 5_000).unwrap();
        assert!(cache.invalidate(SessionId::nil()).is_err());
    }

    #[test]
    fn cache_clear_phase0_stub() {
        let cache = RoutingCache::new(1000, 5_000).unwrap();
        assert!(cache.clear().is_err());
    }
}

//! [`RouterStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for routing observability.
//!
//! See `DESIGN.md` Part 11 (observability).
//!
//! # Design
//!
//! Counters are stored as `AtomicU64` with `Relaxed` ordering:
//! stats are advisory, never load-bearing, so we trade
//! synchronization cost for throughput. Snapshots are taken on
//! demand and the snapshot is what gets exported to metrics.
//!
//! # Mapping to metrics
//!
//! Each counter maps 1:1 to a Prometheus counter or gauge:
//!
//! | Counter           | Prometheus type | Name |
//! |-------------------|-----------------|------|
//! | `lookups_total`   | counter         | `router_lookups_total` |
//! | `cache_hits`      | counter         | `router_cache_hits_total` |
//! | `cache_misses`    | counter         | `router_cache_misses_total` |
//! | `forwards_total`  | counter         | `router_forwards_total` |
//! | `local_total`     | counter         | `router_local_total` |
//! | `remote_total`    | counter         | `router_remote_total` |
//! | `errors_total`    | counter         | `router_errors_total` |

use std::sync::atomic::{AtomicU64, Ordering};

// ── RouterStats ─────────────────────────────────────────────────

/// Atomic counters for the session router.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`RouterStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct RouterStats {
    // ── Lookup hot path ───────────────────────────────────────
    /// Total routing lookups (counter).
    pub lookups_total: AtomicU64,
    /// Lookups that hit the cache (counter).
    pub cache_hits: AtomicU64,
    /// Lookups that missed the cache (counter).
    pub cache_misses: AtomicU64,

    // ── Forwarding ────────────────────────────────────────────
    /// Total datagrams forwarded (counter).
    pub forwards_total: AtomicU64,
    /// Total datagrams redirected (counter).
    pub redirects_total: AtomicU64,

    // ── Per-decision counters ─────────────────────────────────
    /// Local delivery decisions (counter).
    pub local_total: AtomicU64,
    /// Remote forward decisions (counter).
    pub remote_total: AtomicU64,
    /// Anycast decisions (counter).
    pub anycast_total: AtomicU64,
    /// Broadcast decisions (counter).
    pub broadcast_total: AtomicU64,

    // ── Failures ──────────────────────────────────────────────
    /// Routing timeouts (counter).
    pub timeouts_total: AtomicU64,
    /// Routing errors (counter).
    pub errors_total: AtomicU64,
}

impl Default for RouterStats {
    fn default() -> Self {
        Self::new()
    }
}

impl RouterStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            lookups_total: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            forwards_total: AtomicU64::new(0),
            redirects_total: AtomicU64::new(0),
            local_total: AtomicU64::new(0),
            remote_total: AtomicU64::new(0),
            anycast_total: AtomicU64::new(0),
            broadcast_total: AtomicU64::new(0),
            timeouts_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
        }
    }

    /// Record a routing lookup result (hit or miss).
    pub fn record_lookup(&self, hit: bool) {
        self.lookups_total.fetch_add(1, Ordering::Relaxed);
        if hit {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a forward operation.
    pub fn record_forward(&self) {
        self.forwards_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a redirect operation.
    pub fn record_redirect(&self) {
        self.redirects_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a timeout.
    pub fn record_timeout(&self) {
        self.timeouts_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error.
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a local delivery decision.
    pub fn record_local(&self) {
        self.local_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a remote forward decision.
    pub fn record_remote(&self) {
        self.remote_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an anycast decision.
    pub fn record_anycast(&self) {
        self.anycast_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a broadcast decision.
    pub fn record_broadcast(&self) {
        self.broadcast_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields. This is acceptable
    /// for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            lookups_total: self.lookups_total.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            forwards_total: self.forwards_total.load(Ordering::Relaxed),
            redirects_total: self.redirects_total.load(Ordering::Relaxed),
            local_total: self.local_total.load(Ordering::Relaxed),
            remote_total: self.remote_total.load(Ordering::Relaxed),
            anycast_total: self.anycast_total.load(Ordering::Relaxed),
            broadcast_total: self.broadcast_total.load(Ordering::Relaxed),
            timeouts_total: self.timeouts_total.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ───────────────────────────────────────────────

/// Point-in-time snapshot of routing counters.
///
/// All rates are precomputed as basis-points (bps, 1/100 of a
/// percent) so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total routing lookups.
    pub lookups_total: u64,
    /// Lookups that hit the cache.
    pub cache_hits: u64,
    /// Lookups that missed the cache.
    pub cache_misses: u64,
    /// Total datagrams forwarded.
    pub forwards_total: u64,
    /// Total datagrams redirected.
    pub redirects_total: u64,
    /// Local delivery decisions.
    pub local_total: u64,
    /// Remote forward decisions.
    pub remote_total: u64,
    /// Anycast decisions.
    pub anycast_total: u64,
    /// Broadcast decisions.
    pub broadcast_total: u64,
    /// Routing timeouts.
    pub timeouts_total: u64,
    /// Routing errors.
    pub errors_total: u64,
}

impl StatsSnapshot {
    /// Cache hit rate in basis points (0..=10_000).
    /// Returns 10_000 (100%) when there are no lookups yet.
    #[must_use]
    pub const fn cache_hit_rate_bps(&self) -> u16 {
        if self.lookups_total == 0 {
            return 10_000;
        }
        let rate = (self.cache_hits * 10_000) / self.lookups_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Error rate in basis points (0..=10_000).
    /// Returns 0 when there are no lookups yet.
    #[must_use]
    pub const fn error_rate_bps(&self) -> u16 {
        if self.lookups_total == 0 {
            return 0;
        }
        let rate = (self.errors_total * 10_000) / self.lookups_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = RouterStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_lookup_hit_increments_hit_counter() {
        let s = RouterStats::new();
        s.record_lookup(true);
        s.record_lookup(true);
        let snap = s.snapshot();
        assert_eq!(snap.lookups_total, 2);
        assert_eq!(snap.cache_hits, 2);
        assert_eq!(snap.cache_misses, 0);
    }

    #[test]
    fn record_lookup_miss_increments_miss_counter() {
        let s = RouterStats::new();
        s.record_lookup(false);
        let snap = s.snapshot();
        assert_eq!(snap.lookups_total, 1);
        assert_eq!(snap.cache_hits, 0);
        assert_eq!(snap.cache_misses, 1);
    }

    #[test]
    fn cache_hit_rate_is_100_pct_with_no_lookups() {
        let s = RouterStats::new();
        assert_eq!(s.snapshot().cache_hit_rate_bps(), 10_000);
    }

    #[test]
    fn cache_hit_rate_reflects_mixed_hits_and_misses() {
        let s = RouterStats::new();
        for _ in 0..8 {
            s.record_lookup(true);
        }
        for _ in 0..2 {
            s.record_lookup(false);
        }
        // 8/10 = 80% = 8000 bps.
        assert_eq!(s.snapshot().cache_hit_rate_bps(), 8_000);
    }

    #[test]
    fn error_rate_is_zero_with_no_lookups() {
        let s = RouterStats::new();
        assert_eq!(s.snapshot().error_rate_bps(), 0);
    }

    #[test]
    fn error_rate_reflects_errors() {
        let s = RouterStats::new();
        for _ in 0..99 {
            s.record_lookup(true);
        }
        s.record_error(); // 1 error out of 99 lookups
        let snap = s.snapshot();
        // 1*10000/99 = 101 bps
        assert_eq!(snap.error_rate_bps(), 101);
    }

    #[test]
    fn decision_counters() {
        let s = RouterStats::new();
        s.record_local();
        s.record_remote();
        s.record_anycast();
        s.record_broadcast();
        let snap = s.snapshot();
        assert_eq!(snap.local_total, 1);
        assert_eq!(snap.remote_total, 1);
        assert_eq!(snap.anycast_total, 1);
        assert_eq!(snap.broadcast_total, 1);
    }

    #[test]
    fn forward_and_timeout_counters() {
        let s = RouterStats::new();
        s.record_forward();
        s.record_forward();
        s.record_redirect();
        s.record_timeout();
        s.record_error();
        let snap = s.snapshot();
        assert_eq!(snap.forwards_total, 2);
        assert_eq!(snap.redirects_total, 1);
        assert_eq!(snap.timeouts_total, 1);
        assert_eq!(snap.errors_total, 1);
    }
}

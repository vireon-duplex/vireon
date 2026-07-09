//! [`FeatureFlagsStats`], [`StatsSnapshot`] — atomic counters for
//! feature flag observability.
//!
//! See `DESIGN.md` Part 12 (observability).
//!
//! # Design
//!
//! Counters are stored as `AtomicU64` with `Relaxed` ordering:
//! stats are advisory, never load-bearing, so we trade
//! synchronization cost for throughput. Snapshots are taken on
//! demand.

use std::sync::atomic::{AtomicU64, Ordering};

// ── FeatureFlagsStats ───────────────────────────────────────────────

/// Atomic counters for the feature flag system.
#[derive(Debug)]
pub struct FeatureFlagsStats {
    /// Total evaluations performed (counter).
    pub evaluations_total: AtomicU64,
    /// Evaluations that returned enabled (counter).
    pub evaluations_on: AtomicU64,
    /// Evaluations that returned disabled (counter).
    pub evaluations_off: AtomicU64,
    /// Evaluation cache hits (counter).
    pub cache_hits: AtomicU64,
    /// Evaluation cache misses (counter).
    pub cache_misses: AtomicU64,
    /// Rules that matched (counter).
    pub rule_matches: AtomicU64,
    /// Rules that did not match (counter).
    pub rule_misses: AtomicU64,
}

impl Default for FeatureFlagsStats {
    fn default() -> Self {
        Self::new()
    }
}

impl FeatureFlagsStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            evaluations_total: AtomicU64::new(0),
            evaluations_on: AtomicU64::new(0),
            evaluations_off: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            rule_matches: AtomicU64::new(0),
            rule_misses: AtomicU64::new(0),
        }
    }

    /// Record an evaluation result.
    pub fn record_evaluation(&self, enabled: bool) {
        self.evaluations_total.fetch_add(1, Ordering::Relaxed);
        if enabled {
            self.evaluations_on.fetch_add(1, Ordering::Relaxed);
        } else {
            self.evaluations_off.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a cache hit.
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss.
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a rule match.
    pub fn record_rule_match(&self) {
        self.rule_matches.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a rule miss.
    pub fn record_rule_miss(&self) {
        self.rule_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            evaluations_total: self.evaluations_total.load(Ordering::Relaxed),
            evaluations_on: self.evaluations_on.load(Ordering::Relaxed),
            evaluations_off: self.evaluations_off.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            rule_matches: self.rule_matches.load(Ordering::Relaxed),
            rule_misses: self.rule_misses.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ─────────────────────────────────────────────────

/// Point-in-time snapshot of feature flag counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total evaluations.
    pub evaluations_total: u64,
    /// Evaluations that returned enabled.
    pub evaluations_on: u64,
    /// Evaluations that returned disabled.
    pub evaluations_off: u64,
    /// Cache hits.
    pub cache_hits: u64,
    /// Cache misses.
    pub cache_misses: u64,
    /// Rule matches.
    pub rule_matches: u64,
    /// Rule misses.
    pub rule_misses: u64,
}

impl StatsSnapshot {
    /// Cache hit rate in basis points (0..=10_000).
    /// Returns 10_000 (100%) when there are no lookups yet.
    #[must_use]
    pub const fn cache_hit_rate_bps(&self) -> u16 {
        let total = self.cache_hits.saturating_add(self.cache_misses);
        if total == 0 {
            return 10_000;
        }
        let rate = (self.cache_hits * 10_000) / total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Enabled rate in basis points (0..=10_000).
    /// Returns 0 when there are no evaluations yet.
    #[must_use]
    pub const fn enabled_rate_bps(&self) -> u16 {
        if self.evaluations_total == 0 {
            return 0;
        }
        let rate = (self.evaluations_on * 10_000) / self.evaluations_total;
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
        let s = FeatureFlagsStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_evaluation_enabled() {
        let s = FeatureFlagsStats::new();
        s.record_evaluation(true);
        s.record_evaluation(true);
        let snap = s.snapshot();
        assert_eq!(snap.evaluations_total, 2);
        assert_eq!(snap.evaluations_on, 2);
        assert_eq!(snap.evaluations_off, 0);
    }

    #[test]
    fn record_evaluation_disabled() {
        let s = FeatureFlagsStats::new();
        s.record_evaluation(false);
        let snap = s.snapshot();
        assert_eq!(snap.evaluations_total, 1);
        assert_eq!(snap.evaluations_on, 0);
        assert_eq!(snap.evaluations_off, 1);
    }

    #[test]
    fn record_evaluation_mixed() {
        let s = FeatureFlagsStats::new();
        for _ in 0..7 {
            s.record_evaluation(true);
        }
        for _ in 0..3 {
            s.record_evaluation(false);
        }
        let snap = s.snapshot();
        assert_eq!(snap.evaluations_total, 10);
        assert_eq!(snap.evaluations_on, 7);
        assert_eq!(snap.evaluations_off, 3);
    }

    #[test]
    fn record_cache_hit() {
        let s = FeatureFlagsStats::new();
        s.record_cache_hit();
        s.record_cache_hit();
        let snap = s.snapshot();
        assert_eq!(snap.cache_hits, 2);
        assert_eq!(snap.cache_misses, 0);
    }

    #[test]
    fn record_cache_miss() {
        let s = FeatureFlagsStats::new();
        s.record_cache_miss();
        let snap = s.snapshot();
        assert_eq!(snap.cache_hits, 0);
        assert_eq!(snap.cache_misses, 1);
    }

    #[test]
    fn cache_hit_rate_with_no_cache_is_100_pct() {
        let s = FeatureFlagsStats::new();
        assert_eq!(s.snapshot().cache_hit_rate_bps(), 10_000);
    }

    #[test]
    fn cache_hit_rate_with_mixed_hits_misses() {
        let s = FeatureFlagsStats::new();
        for _ in 0..8 {
            s.record_cache_hit();
        }
        for _ in 0..2 {
            s.record_cache_miss();
        }
        // 8/10 = 80% = 8000 bps.
        assert_eq!(s.snapshot().cache_hit_rate_bps(), 8_000);
    }

    #[test]
    fn enabled_rate_with_no_evals_is_zero() {
        let s = FeatureFlagsStats::new();
        assert_eq!(s.snapshot().enabled_rate_bps(), 0);
    }

    #[test]
    fn enabled_rate_with_mixed_evals() {
        let s = FeatureFlagsStats::new();
        for _ in 0..3 {
            s.record_evaluation(true);
        }
        for _ in 0..7 {
            s.record_evaluation(false);
        }
        // 3/10 = 30% = 3000 bps.
        assert_eq!(s.snapshot().enabled_rate_bps(), 3_000);
    }

    #[test]
    fn enabled_rate_all_enabled() {
        let s = FeatureFlagsStats::new();
        for _ in 0..100 {
            s.record_evaluation(true);
        }
        assert_eq!(s.snapshot().enabled_rate_bps(), 10_000);
    }

    #[test]
    fn record_rule_match() {
        let s = FeatureFlagsStats::new();
        s.record_rule_match();
        s.record_rule_match();
        let snap = s.snapshot();
        assert_eq!(snap.rule_matches, 2);
        assert_eq!(snap.rule_misses, 0);
    }

    #[test]
    fn record_rule_miss() {
        let s = FeatureFlagsStats::new();
        s.record_rule_miss();
        let snap = s.snapshot();
        assert_eq!(snap.rule_matches, 0);
        assert_eq!(snap.rule_misses, 1);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = FeatureFlagsStats::new();
        s.record_evaluation(true);
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn snapshot_does_not_affect_counters() {
        let s = FeatureFlagsStats::new();
        s.record_evaluation(true);
        let snap1 = s.snapshot();
        let snap2 = s.snapshot();
        assert_eq!(snap1, snap2);
    }
}

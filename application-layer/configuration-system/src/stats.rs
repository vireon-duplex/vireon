//! [`ConfigStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for observability.
//!
//! See `DESIGN.md` Part 12 (observability).

use std::sync::atomic::{AtomicU64, Ordering};

// ── ConfigStats ───────────────────────────────────────────────────

/// Atomic counters for the configuration system.
///
/// All fields are public so any subsystem can record events directly.
/// Use [`ConfigStats::snapshot`] to get a consistent point-in-time
/// view.
#[derive(Debug)]
pub struct ConfigStats {
    /// Total read operations (counter).
    pub reads_total: AtomicU64,
    /// Reads that hit a valid value (counter).
    pub hits: AtomicU64,
    /// Reads that missed (key not found) (counter).
    pub misses: AtomicU64,
    /// Parse errors encountered (counter).
    pub parse_errors: AtomicU64,
    /// Configuration reloads performed (counter).
    pub reloads: AtomicU64,
    /// Keys that have been frozen (counter).
    pub frozen_keys: AtomicU64,
    /// Watch events processed (counter).
    pub watch_events: AtomicU64,
}

impl Default for ConfigStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            reads_total: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            parse_errors: AtomicU64::new(0),
            reloads: AtomicU64::new(0),
            frozen_keys: AtomicU64::new(0),
            watch_events: AtomicU64::new(0),
        }
    }

    /// Record a read operation.
    pub fn record_read(&self, hit: bool) {
        self.reads_total.fetch_add(1, Ordering::Relaxed);
        if hit {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a parse error.
    pub fn record_parse_error(&self) {
        self.parse_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a configuration reload.
    pub fn record_reload(&self) {
        self.reloads.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a key freeze operation.
    pub fn record_freeze(&self) {
        self.frozen_keys.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a watch event.
    pub fn record_watch_event(&self) {
        self.watch_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be momentarily
    /// inconsistent across fields. This is acceptable for advisory
    /// stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            reads_total: self.reads_total.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
            reloads: self.reloads.load(Ordering::Relaxed),
            frozen_keys: self.frozen_keys.load(Ordering::Relaxed),
            watch_events: self.watch_events.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ────────────────────────────────────────────────

/// Point-in-time snapshot of configuration counters.
///
/// Rates are precomputed as basis-points (bps, 1/100 of a percent)
/// so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total read operations.
    pub reads_total: u64,
    /// Reads that hit.
    pub hits: u64,
    /// Reads that missed.
    pub misses: u64,
    /// Parse errors encountered.
    pub parse_errors: u64,
    /// Configuration reloads performed.
    pub reloads: u64,
    /// Keys that have been frozen.
    pub frozen_keys: u64,
    /// Watch events processed.
    pub watch_events: u64,
}

impl StatsSnapshot {
    /// Cache hit rate in basis points (0..=10_000).
    /// Returns 10_000 (100%) when there are no reads yet.
    #[must_use]
    pub const fn hit_rate_bps(&self) -> u16 {
        if self.reads_total == 0 {
            return 10_000;
        }
        let rate = (self.hits * 10_000) / self.reads_total;
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
        let s = ConfigStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_read_hit_increments_hit_counter() {
        let s = ConfigStats::new();
        s.record_read(true);
        s.record_read(true);
        let snap = s.snapshot();
        assert_eq!(snap.reads_total, 2);
        assert_eq!(snap.hits, 2);
        assert_eq!(snap.misses, 0);
    }

    #[test]
    fn record_read_miss_increments_miss_counter() {
        let s = ConfigStats::new();
        s.record_read(false);
        let snap = s.snapshot();
        assert_eq!(snap.reads_total, 1);
        assert_eq!(snap.hits, 0);
        assert_eq!(snap.misses, 1);
    }

    #[test]
    fn hit_rate_is_100_pct_with_no_reads() {
        let s = ConfigStats::new();
        assert_eq!(s.snapshot().hit_rate_bps(), 10_000);
    }

    #[test]
    fn hit_rate_reflects_mixed_hits_and_misses() {
        let s = ConfigStats::new();
        for _ in 0..8 {
            s.record_read(true);
        }
        for _ in 0..2 {
            s.record_read(false);
        }
        // 8/10 = 80% = 8000 bps.
        assert_eq!(s.snapshot().hit_rate_bps(), 8_000);
    }

    #[test]
    fn parse_error_counter() {
        let s = ConfigStats::new();
        s.record_parse_error();
        s.record_parse_error();
        assert_eq!(s.snapshot().parse_errors, 2);
    }

    #[test]
    fn reload_counter() {
        let s = ConfigStats::new();
        s.record_reload();
        s.record_reload();
        s.record_reload();
        assert_eq!(s.snapshot().reloads, 3);
    }

    #[test]
    fn freeze_counter() {
        let s = ConfigStats::new();
        s.record_freeze();
        assert_eq!(s.snapshot().frozen_keys, 1);
    }

    #[test]
    fn watch_event_counter() {
        let s = ConfigStats::new();
        s.record_watch_event();
        s.record_watch_event();
        assert_eq!(s.snapshot().watch_events, 2);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = ConfigStats::new();
        s.record_read(true);
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn hit_rate_clamps_at_u16_max() {
        // Test a case that would overflow if not clamped.
        // We need to be careful to not actually overflow in the test.
        // Use values that would overflow during calculation but fit in u64.
        let snap = StatsSnapshot {
            reads_total: 1, // Very small total
            hits: 10_000,   // 100% hit rate would be 10000 bps
            ..StatsSnapshot::default()
        };
        // (10000 * 10000) / 1 = 100,000,000 which exceeds u16::MAX (65535).
        // The implementation should clamp this to u16::MAX.
        assert_eq!(snap.hit_rate_bps(), u16::MAX);
    }
}

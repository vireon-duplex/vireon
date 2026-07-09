//! [`VersionNegotiationStats`], [`StatsSnapshot`] — atomic counters
//! and a point-in-time snapshot for observability.
//!
//! See `DESIGN.md` Part 12 (observability).

use std::sync::atomic::{AtomicU64, Ordering};

// ── VersionNegotiationStats ────────────────────────────────────────

/// Atomic counters for the version negotiation system.
#[derive(Debug)]
pub struct VersionNegotiationStats {
    /// Total negotiations attempted (counter).
    pub negotiations_total: AtomicU64,
    /// Negotiations that succeeded (counter).
    pub negotiations_succeeded: AtomicU64,
    /// Negotiations that failed (counter).
    pub negotiations_failed: AtomicU64,
    /// Fallback to older version (counter).
    pub fallbacks: AtomicU64,
    /// Deprecated version warnings (counter).
    pub deprecated_warnings: AtomicU64,
    /// Experimental versions allowed (counter).
    pub experimental_allowed: AtomicU64,
}

impl Default for VersionNegotiationStats {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionNegotiationStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            negotiations_total: AtomicU64::new(0),
            negotiations_succeeded: AtomicU64::new(0),
            negotiations_failed: AtomicU64::new(0),
            fallbacks: AtomicU64::new(0),
            deprecated_warnings: AtomicU64::new(0),
            experimental_allowed: AtomicU64::new(0),
        }
    }

    /// Record a negotiation attempt.
    pub fn record_negotiation(&self, succeeded: bool) {
        self.negotiations_total.fetch_add(1, Ordering::Relaxed);
        if succeeded {
            self.negotiations_succeeded.fetch_add(1, Ordering::Relaxed);
        } else {
            self.negotiations_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a fallback to an older version.
    pub fn record_fallback(&self) {
        self.fallbacks.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a deprecated version warning.
    pub fn record_deprecated_warning(&self) {
        self.deprecated_warnings.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an experimental version being allowed.
    pub fn record_experimental_allowed(&self) {
        self.experimental_allowed.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            negotiations_total: self.negotiations_total.load(Ordering::Relaxed),
            negotiations_succeeded: self.negotiations_succeeded.load(Ordering::Relaxed),
            negotiations_failed: self.negotiations_failed.load(Ordering::Relaxed),
            fallbacks: self.fallbacks.load(Ordering::Relaxed),
            deprecated_warnings: self.deprecated_warnings.load(Ordering::Relaxed),
            experimental_allowed: self.experimental_allowed.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ─────────────────────────────────────────────────

/// Point-in-time snapshot of negotiation counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total negotiations attempted.
    pub negotiations_total: u64,
    /// Negotiations that succeeded.
    pub negotiations_succeeded: u64,
    /// Negotiations that failed.
    pub negotiations_failed: u64,
    /// Fallbacks to older versions.
    pub fallbacks: u64,
    /// Deprecated version warnings.
    pub deprecated_warnings: u64,
    /// Experimental versions allowed.
    pub experimental_allowed: u64,
}

impl StatsSnapshot {
    /// Success rate in basis points (0..=10_000).
    /// Returns 10_000 (100%) when there are no negotiations yet.
    #[must_use]
    pub const fn success_rate_bps(&self) -> u16 {
        if self.negotiations_total == 0 {
            return 10_000;
        }
        let rate = (self.negotiations_succeeded * 10_000) / self.negotiations_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Fallback rate in basis points (0..=10_000).
    #[must_use]
    pub const fn fallback_rate_bps(&self) -> u16 {
        if self.negotiations_total == 0 {
            return 0;
        }
        let rate = (self.fallbacks * 10_000) / self.negotiations_total;
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
        let s = VersionNegotiationStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_negotiation_increments_counters() {
        let s = VersionNegotiationStats::new();
        s.record_negotiation(true);
        s.record_negotiation(false);
        let snap = s.snapshot();
        assert_eq!(snap.negotiations_total, 2);
        assert_eq!(snap.negotiations_succeeded, 1);
        assert_eq!(snap.negotiations_failed, 1);
    }

    #[test]
    fn success_rate_is_100_pct_with_no_negotiations() {
        let s = VersionNegotiationStats::new();
        assert_eq!(s.snapshot().success_rate_bps(), 10_000);
    }

    #[test]
    fn success_rate_reflects_mixed_results() {
        let s = VersionNegotiationStats::new();
        for _ in 0..8 {
            s.record_negotiation(true);
        }
        for _ in 0..2 {
            s.record_negotiation(false);
        }
        assert_eq!(s.snapshot().success_rate_bps(), 8_000); // 80%
    }

    #[test]
    fn fallback_counter() {
        let s = VersionNegotiationStats::new();
        s.record_fallback();
        s.record_fallback();
        assert_eq!(s.snapshot().fallbacks, 2);
    }

    #[test]
    fn deprecated_warning_counter() {
        let s = VersionNegotiationStats::new();
        s.record_deprecated_warning();
        assert_eq!(s.snapshot().deprecated_warnings, 1);
    }

    #[test]
    fn experimental_allowed_counter() {
        let s = VersionNegotiationStats::new();
        s.record_experimental_allowed();
        assert_eq!(s.snapshot().experimental_allowed, 1);
    }

    #[test]
    fn fallback_rate_calculation() {
        let s = VersionNegotiationStats::new();
        for _ in 0..10 {
            s.record_negotiation(true);
        }
        for _ in 0..5 {
            s.record_fallback();
        }
        // 5/10 = 50% = 5000 bps
        assert_eq!(s.snapshot().fallback_rate_bps(), 5_000);
    }
}

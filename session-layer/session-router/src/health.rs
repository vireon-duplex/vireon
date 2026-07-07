//! [`HealthReport`], [`HealthState`] — operational health summary
//! derived from routing stats.
//!
//! See `DESIGN.md` Part 8 (health checking) and Part 11
//! (observability).
//!
//! # Tri-state classification
//!
//! Health is classified into three states, each with a clear
//! operator action:
//!
//! | State     | Meaning                          | Operator action |
//! |-----------|----------------------------------|-----------------|
//! | `Healthy` | All systems nominal              | None |
//! | `Degraded`| Performance impact               | Investigate |
//! | `Critical`| Severe impact; data loss risk    | Page on-call |
//!
//! # Thresholds
//!
//! The "worst signal wins" policy classifies each signal
//! independently and takes the worst:
//!
//! - Cache hit rate >= 99% (`9_900` bps): healthy.
//! - Cache hit rate < 95% (`9_500` bps): degraded.
//! - Cache hit rate < 90% (`9_000` bps): critical.
//! - Error rate > 1% (`100` bps): degraded.
//! - Error rate > 5% (`500` bps): critical.

use crate::stats::StatsSnapshot;

// ── HealthState ─────────────────────────────────────────────────

/// Tri-state health classification.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum HealthState {
    /// All systems nominal.
    #[default]
    Healthy,
    /// Performance or availability degraded; investigate.
    Degraded,
    /// Severe impact; page on-call.
    Critical,
}

impl HealthState {
    /// Whether the state is `Healthy`.
    #[must_use]
    pub const fn is_healthy(self) -> bool {
        matches!(self, Self::Healthy)
    }

    /// Whether the state requires operator attention.
    #[must_use]
    pub const fn needs_attention(self) -> bool {
        matches!(self, Self::Degraded | Self::Critical)
    }

    /// Whether the state is critical (page on-call).
    #[must_use]
    pub const fn is_critical(self) -> bool {
        matches!(self, Self::Critical)
    }

    /// Human-readable name for metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Critical => "critical",
        }
    }
}

// ── HealthReport ────────────────────────────────────────────────

/// Operational health snapshot derived from [`StatsSnapshot`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    /// Overall health state.
    pub state: HealthState,
    /// Cache hit rate in basis points (0..=10_000).
    pub cache_hit_rate_bps: u16,
    /// Error rate in basis points (0..=10_000).
    pub error_rate_bps: u16,
    /// Average RPC latency in nanoseconds.
    pub avg_rpc_latency_ns: u64,
}

impl HealthReport {
    /// Threshold: cache hit rate >= this → healthy (in bps).
    pub const CACHE_HEALTHY_BPS: u16 = 9_900;
    /// Threshold: cache hit rate < this → degraded (in bps).
    pub const CACHE_DEGRADED_BPS: u16 = 9_500;
    /// Threshold: cache hit rate < this → critical (in bps).
    pub const CACHE_CRITICAL_BPS: u16 = 9_000;

    /// Threshold: error rate > this → degraded (in bps).
    pub const ERROR_DEGRADED_BPS: u16 = 100;
    /// Threshold: error rate > this → critical (in bps).
    pub const ERROR_CRITICAL_BPS: u16 = 500;

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a "worst signal wins" policy: if
    /// any individual signal is critical, the overall state is
    /// critical; otherwise the worst degraded signal wins.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let cache_hit_rate_bps = stats.cache_hit_rate_bps();
        let error_rate_bps = stats.error_rate_bps();

        let state = Self::classify(cache_hit_rate_bps, error_rate_bps);

        Self {
            state,
            cache_hit_rate_bps,
            error_rate_bps,
            avg_rpc_latency_ns: 0,
        }
    }

    /// Combine health signals into one overall state using a
    /// "worst signal wins" policy.
    fn classify(cache_bps: u16, error_bps: u16) -> HealthState {
        // Cache hit rate signal.
        let cache_state = if cache_bps >= Self::CACHE_HEALTHY_BPS {
            HealthState::Healthy
        } else if cache_bps < Self::CACHE_CRITICAL_BPS {
            HealthState::Critical
        } else {
            HealthState::Degraded
        };

        // Error rate signal.
        let error_state = if error_bps > Self::ERROR_CRITICAL_BPS {
            HealthState::Critical
        } else if error_bps > Self::ERROR_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Worst signal wins.
        worst_of([cache_state, error_state])
    }

    /// Whether the report indicates a healthy system.
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        self.state.is_healthy()
    }

    /// Whether the report requires operator attention.
    #[must_use]
    pub const fn needs_attention(&self) -> bool {
        self.state.needs_attention()
    }
}

/// Pick the worst (most severe) of two health states.
const fn worst_of(states: [HealthState; 2]) -> HealthState {
    let mut i = 0;
    let mut worst = HealthState::Healthy;
    while i < 2 {
        let s = states[i];
        if matches!(s, HealthState::Critical) {
            return HealthState::Critical;
        }
        if matches!(s, HealthState::Degraded) {
            worst = HealthState::Degraded;
        }
        i += 1;
    }
    worst
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats_with_cache(hit: u64, miss: u64) -> StatsSnapshot {
        StatsSnapshot {
            lookups_total: hit + miss,
            cache_hits: hit,
            cache_misses: miss,
            ..StatsSnapshot::default()
        }
    }

    #[test]
    fn empty_stats_derive_healthy() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert!(report.is_healthy());
    }

    #[test]
    fn high_hit_rate_is_healthy() {
        // 9999 / 10000 = 99.99%.
        let stats = stats_with_cache(9_999, 1);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn degraded_hit_rate_is_degraded() {
        // 9700 / 10000 = 97% → between 9500 and 9900.
        let stats = stats_with_cache(9_700, 300);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn low_hit_rate_is_critical() {
        // 8000 / 10000 = 80% < 9000.
        let stats = stats_with_cache(8_000, 2_000);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn high_error_rate_is_degraded() {
        let mut stats = stats_with_cache(10_000, 0);
        // 2% error rate → 200 bps > 100 degraded threshold.
        stats.lookups_total = 10_000;
        stats.errors_total = 200;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn very_high_error_rate_is_critical() {
        let mut stats = stats_with_cache(10_000, 0);
        stats.lookups_total = 10_000;
        stats.errors_total = 600; // 6% > 5% critical.
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn health_state_predicates() {
        assert!(HealthState::Healthy.is_healthy());
        assert!(!HealthState::Degraded.is_healthy());
        assert!(HealthState::Degraded.needs_attention());
        assert!(HealthState::Critical.is_critical());
    }
}

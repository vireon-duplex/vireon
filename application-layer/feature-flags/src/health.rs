//! [`HealthReport`], [`HealthState`] — operational health summary
//! derived from stats.
//!
//! See `DESIGN.md` Part 12 (observability).
//!
//! # Tri-state classification
//!
//! Health is classified into three states, each with a clear
//! operator action:
//!
//! | State | Meaning | Operator action |
//! |-------|---------|-----------------|
//! | `Healthy` | All systems nominal | None |
//! | `Degraded` | Performance or availability impact | Investigate during business hours |
//! | `Critical` | Severe impact; data loss risk possible | Page on-call immediately |

use crate::stats::StatsSnapshot;

// ── HealthState ───────────────────────────────────────────────────

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

// ── HealthReport ──────────────────────────────────────────────────

/// Operational health snapshot derived from [`StatsSnapshot`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    /// Overall health state.
    pub state: HealthState,
    /// Total evaluations performed.
    pub evaluations_total: u64,
    /// Enabled rate in basis points (0..=10_000).
    pub enabled_rate_bps: u16,
    /// Cache hit rate in basis points (0..=10_000).
    pub cache_hit_rate_bps: u16,
    /// Number of rule errors (stub for now).
    pub rule_errors: u64,
}

impl HealthReport {
    /// Threshold: cache hit rate ≥ this → healthy (in bps).
    pub const CACHE_HEALTHY_BPS: u16 = 9_500;
    /// Threshold: cache hit rate < this → degraded (in bps).
    pub const CACHE_DEGRADED_BPS: u16 = 8_000;

    /// Derive a health report from a stats snapshot.
    ///
    /// Classification follows "worst signal wins" policy.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let enabled_rate_bps = stats.enabled_rate_bps();
        let cache_hit_rate_bps = stats.cache_hit_rate_bps();

        // Cache health signal.
        let cache_state = if cache_hit_rate_bps >= Self::CACHE_HEALTHY_BPS {
            HealthState::Healthy
        } else if cache_hit_rate_bps < Self::CACHE_DEGRADED_BPS {
            HealthState::Critical
        } else {
            HealthState::Degraded
        };

        // For Phase 0, treat enabled rate as healthy.
        let eval_state = HealthState::Healthy;

        // Worst signal wins.
        let state = worst_of([cache_state, eval_state]);

        Self {
            state,
            evaluations_total: stats.evaluations_total,
            enabled_rate_bps,
            cache_hit_rate_bps,
            rule_errors: 0, // Stub for now.
        }
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

/// Pick the worst (most severe) health state.
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

    #[test]
    fn empty_stats_derive_healthy() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert!(report.is_healthy());
    }

    #[test]
    fn high_cache_hit_rate_is_healthy() {
        let mut stats = StatsSnapshot::default();
        stats.cache_hits = 9_800;
        stats.cache_misses = 200;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert_eq!(report.cache_hit_rate_bps, 9_800);
    }

    #[test]
    fn degraded_cache_hit_rate_is_degraded() {
        let mut stats = StatsSnapshot::default();
        stats.cache_hits = 8_500;
        stats.cache_misses = 1_500;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn low_cache_hit_rate_is_critical() {
        let mut stats = StatsSnapshot::default();
        stats.cache_hits = 7_000;
        stats.cache_misses = 3_000;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn health_state_predicates() {
        assert!(HealthState::Healthy.is_healthy());
        assert!(!HealthState::Healthy.needs_attention());
        assert!(!HealthState::Healthy.is_critical());

        assert!(!HealthState::Degraded.is_healthy());
        assert!(HealthState::Degraded.needs_attention());
        assert!(!HealthState::Degraded.is_critical());

        assert!(HealthState::Critical.is_critical());
        assert!(HealthState::Critical.needs_attention());
    }

    #[test]
    fn health_state_as_str() {
        assert_eq!(HealthState::Healthy.as_str(), "healthy");
        assert_eq!(HealthState::Degraded.as_str(), "degraded");
        assert_eq!(HealthState::Critical.as_str(), "critical");
    }

    #[test]
    fn worst_signal_wins() {
        let mut stats = StatsSnapshot::default();
        // Healthy evals but critical cache.
        stats.evaluations_total = 10_000;
        stats.cache_hits = 5_000;
        stats.cache_misses = 5_000;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn health_report_fields() {
        let mut stats = StatsSnapshot::default();
        stats.evaluations_total = 100;
        stats.evaluations_on = 30;
        stats.cache_hits = 95;
        stats.cache_misses = 5;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.evaluations_total, 100);
        assert_eq!(report.enabled_rate_bps, 3_000); // 30%
        assert_eq!(report.cache_hit_rate_bps, 9_500); // 95%
    }

    #[test]
    fn health_report_copy_and_clone() {
        let report = HealthReport::default();
        let report2 = report;
        assert_eq!(report, report2);
    }

    #[test]
    fn health_state_hash() {
        let _ = std::collections::HashSet::from([
            HealthState::Healthy,
            HealthState::Degraded,
            HealthState::Critical,
        ]);
    }
}

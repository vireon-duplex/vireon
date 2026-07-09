//! [`HealthReport`], [`HealthState`] — operational health summary
//! derived from stats.
//!
//! See `DESIGN.md` Part 12 (observability).

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
    pub const fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy)
    }

    /// Whether the state requires operator attention.
    #[must_use]
    pub const fn needs_attention(&self) -> bool {
        matches!(self, Self::Degraded | Self::Critical)
    }

    /// Whether the state is critical (page on-call).
    #[must_use]
    pub const fn is_critical(&self) -> bool {
        matches!(self, Self::Critical)
    }

    /// Human-readable name for metrics labels.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
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
    /// Total read operations.
    pub reads_total: u64,
    /// Hit rate in basis points (0..=10_000).
    pub hit_rate_bps: u16,
    /// Parse errors encountered.
    pub parse_errors: u64,
    /// Configuration reloads performed.
    pub reloads: u64,
}

impl HealthReport {
    /// Threshold: hit rate ≥ this → healthy (in bps).
    pub const HIT_HEALTHY_BPS: u16 = 9_900;
    /// Threshold: hit rate < this → degraded (in bps).
    pub const HIT_DEGRADED_BPS: u16 = 9_000;
    /// Threshold: hit rate < this → critical (in bps).
    pub const HIT_CRITICAL_BPS: u16 = 8_000;

    /// Threshold: parse errors > this → degraded.
    pub const PARSE_ERRORS_DEGRADED: u64 = 10;
    /// Threshold: parse errors > this → critical.
    pub const PARSE_ERRORS_CRITICAL: u64 = 100;

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a "worst signal wins" policy: if
    /// any individual signal is critical, the overall state is
    /// critical; otherwise the worst degraded signal wins.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let hit_rate_bps = stats.hit_rate_bps();

        // Compute per-signal state, then take the worst.
        let state = Self::classify(hit_rate_bps, stats.parse_errors);

        Self {
            state,
            reads_total: stats.reads_total,
            hit_rate_bps,
            parse_errors: stats.parse_errors,
            reloads: stats.reloads,
        }
    }

    /// Combine multiple health signals into one overall state
    /// using a "worst signal wins" policy.
    fn classify(hit_rate_bps: u16, parse_errors: u64) -> HealthState {
        // Hit rate signal.
        let hit_state = if hit_rate_bps >= Self::HIT_HEALTHY_BPS {
            HealthState::Healthy
        } else if hit_rate_bps < Self::HIT_CRITICAL_BPS {
            HealthState::Critical
        } else {
            HealthState::Degraded
        };

        // Parse errors signal.
        let parse_state = if parse_errors > Self::PARSE_ERRORS_CRITICAL {
            HealthState::Critical
        } else if parse_errors > Self::PARSE_ERRORS_DEGRADED {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Worst signal wins.
        worst_of([hit_state, parse_state])
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

    fn stats_with_hit_rate(hit: u64, total: u64) -> StatsSnapshot {
        StatsSnapshot {
            reads_total: total,
            hits: hit,
            misses: total.saturating_sub(hit),
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
        let stats = stats_with_hit_rate(9_999, 10_000);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn degraded_hit_rate_is_degraded() {
        // 9500 / 10000 = 95% → between 8000 and 9900.
        let stats = stats_with_hit_rate(9_500, 10_000);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn low_hit_rate_is_critical() {
        // 7000 / 10000 = 70% < 8000.
        let stats = stats_with_hit_rate(7_000, 10_000);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn high_parse_errors_is_degraded() {
        let mut stats = stats_with_hit_rate(10_000, 10_000);
        stats.parse_errors = 15; // > 10
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn very_high_parse_errors_is_critical() {
        let mut stats = stats_with_hit_rate(10_000, 10_000);
        stats.parse_errors = 150; // > 100
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
        // If hit rate is healthy but parse errors are critical, overall is critical.
        let mut stats = stats_with_hit_rate(10_000, 10_000); // healthy hit rate
        stats.parse_errors = 150; // critical parse errors
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn degraded_hit_worst_of_healthy_parse() {
        // If hit rate is degraded but parse errors are healthy, overall is degraded.
        let stats = stats_with_hit_rate(8_500, 10_000); // degraded hit rate
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn threshold_constants_are_correct() {
        assert_eq!(HealthReport::HIT_HEALTHY_BPS, 9_900);
        assert_eq!(HealthReport::HIT_DEGRADED_BPS, 9_000);
        assert_eq!(HealthReport::HIT_CRITICAL_BPS, 8_000);
        assert_eq!(HealthReport::PARSE_ERRORS_DEGRADED, 10);
        assert_eq!(HealthReport::PARSE_ERRORS_CRITICAL, 100);
    }
}

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

// ── HealthReport ─────────────────────────────────────────────────

/// Operational health snapshot derived from [`StatsSnapshot`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    /// Overall health state.
    pub state: HealthState,
    /// Total negotiations attempted.
    pub negotiations_total: u64,
    /// Success rate in basis points (0..=10_000).
    pub success_rate_bps: u16,
    /// Fallback rate in basis points (0..=10_000).
    pub fallback_rate_bps: u16,
    /// Deprecated version warnings.
    pub deprecated_warnings: u64,
}

impl HealthReport {
    /// Threshold: success rate ≥ this → healthy (in bps).
    pub const SUCCESS_HEALTHY_BPS: u16 = 9_900;
    /// Threshold: success rate < this → critical (in bps).
    pub const SUCCESS_CRITICAL_BPS: u16 = 9_000;

    /// Threshold: fallback rate > this → degraded (in bps).
    pub const FALLBACK_DEGRADED_BPS: u16 = 500;
    /// Threshold: fallback rate > this → critical (in bps).
    pub const FALLBACK_CRITICAL_BPS: u16 = 2_000;

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a "worst signal wins" policy.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let success_rate_bps = stats.success_rate_bps();
        let fallback_rate_bps = stats.fallback_rate_bps();

        let state = Self::classify(success_rate_bps, fallback_rate_bps);

        Self {
            state,
            negotiations_total: stats.negotiations_total,
            success_rate_bps,
            fallback_rate_bps,
            deprecated_warnings: stats.deprecated_warnings,
        }
    }

    /// Combine multiple health signals into one overall state.
    fn classify(success_bps: u16, fallback_bps: u16) -> HealthState {
        // Success rate signal.
        let success_state = if success_bps >= Self::SUCCESS_HEALTHY_BPS {
            HealthState::Healthy
        } else if success_bps < Self::SUCCESS_CRITICAL_BPS {
            HealthState::Critical
        } else {
            HealthState::Degraded
        };

        // Fallback rate signal.
        let fallback_state = if fallback_bps > Self::FALLBACK_CRITICAL_BPS {
            HealthState::Critical
        } else if fallback_bps > Self::FALLBACK_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Worst signal wins.
        worst_of([success_state, fallback_state])
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

    fn stats_with_success_rate(succeeded: u64, total: u64) -> StatsSnapshot {
        StatsSnapshot {
            negotiations_succeeded: succeeded,
            negotiations_total: total,
            ..StatsSnapshot::default()
        }
    }

    #[test]
    fn empty_stats_derive_healthy() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn high_success_rate_is_healthy() {
        let stats = stats_with_success_rate(9_999, 10_000);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn low_success_rate_is_critical() {
        let stats = stats_with_success_rate(8_000, 10_000);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn high_fallback_rate_is_critical() {
        let mut stats = stats_with_success_rate(10_000, 10_000);
        stats.fallbacks = 3_000;
        stats.negotiations_total = 10_000;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn moderate_fallback_rate_is_degraded() {
        let mut stats = stats_with_success_rate(10_000, 10_000);
        stats.fallbacks = 1_000;
        stats.negotiations_total = 10_000;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn worst_signal_wins() {
        // High success rate but very high fallback rate → critical.
        let mut stats = stats_with_success_rate(10_000, 10_000);
        stats.fallbacks = 3_000;
        stats.negotiations_total = 10_000;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }
}

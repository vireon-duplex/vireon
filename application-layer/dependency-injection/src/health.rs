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
//!-------|---------|-----------------|
//! | `Healthy` | All systems nominal | None |
//! | `Degraded` | Performance or availability impact | Investigate during business hours |
//! | `Critical` | Severe impact; data loss risk possible | Page on-call immediately |

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
    /// Total services registered.
    pub registrations_total: u64,
    /// Resolution failure rate in basis points (0..=10_000).
    pub resolution_failure_rate_bps: u16,
    /// Circular dependency errors detected.
    pub circular_errors: u64,
}

impl HealthReport {
    /// Threshold: failure rate > this → degraded (in bps).
    pub const FAILURE_DEGRADED_BPS: u16 = 500;
    /// Threshold: failure rate > this → critical (in bps).
    pub const FAILURE_CRITICAL_BPS: u16 = 2_000;

    /// Threshold: circular errors > this → degraded.
    pub const CIRCULAR_DEGRADED: u64 = 1;
    /// Threshold: circular errors > this → critical.
    pub const CIRCULAR_CRITICAL: u64 = 5;

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a "worst signal wins" policy: if
    /// any individual signal is critical, the overall state is
    /// critical; otherwise the worst degraded signal wins.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let resolution_failure_rate_bps = stats.resolution_failure_rate_bps();
        let circular_errors = stats.circular_errors;

        // Compute per-signal state, then take the worst.
        let state = Self::classify(resolution_failure_rate_bps, circular_errors);

        Self {
            state,
            registrations_total: stats.registrations_total,
            resolution_failure_rate_bps,
            circular_errors,
        }
    }

    /// Combine multiple health signals into one overall state
    /// using a "worst signal wins" policy.
    fn classify(failure_rate_bps: u16, circular_errors: u64) -> HealthState {
        // Failure rate signal.
        // > FAILURE_CRITICAL_BPS → Critical
        // > FAILURE_DEGRADED_BPS → Degraded
        // Otherwise → Healthy
        let failure_state = if failure_rate_bps > Self::FAILURE_CRITICAL_BPS {
            HealthState::Critical
        } else if failure_rate_bps > Self::FAILURE_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Circular errors signal.
        let circular_state = if circular_errors > Self::CIRCULAR_CRITICAL {
            HealthState::Critical
        } else if circular_errors >= Self::CIRCULAR_DEGRADED {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Worst signal wins.
        worst_of([failure_state, circular_state])
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

    fn stats_with_failure_rate(success: u64, failure: u64) -> StatsSnapshot {
        StatsSnapshot {
            resolutions_total: success + failure,
            resolutions_succeeded: success,
            resolutions_failed: failure,
            ..StatsSnapshot::default()
        }
    }

    fn stats_with_circular_errors(errors: u64) -> StatsSnapshot {
        StatsSnapshot {
            circular_errors: errors,
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
    fn low_failure_rate_is_healthy() {
        // 1% failure rate = 100 bps < 500.
        let stats = stats_with_failure_rate(99, 1);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn degraded_failure_rate_is_degraded() {
        // 7% failure rate = 700 bps > 500.
        let stats = stats_with_failure_rate(93, 7);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn high_failure_rate_is_critical() {
        // 25% failure rate = 2500 bps > 2000.
        let stats = stats_with_failure_rate(75, 25);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn one_circular_error_is_degraded() {
        let stats = stats_with_circular_errors(1);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn many_circular_errors_is_critical() {
        let stats = stats_with_circular_errors(6);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn zero_circular_errors_is_healthy() {
        let stats = stats_with_circular_errors(0);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
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
        // If any signal is critical, overall is critical.
        let mut stats = stats_with_failure_rate(100, 0); // healthy failure
        stats.circular_errors = 6; // critical circular errors
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn degraded_signal_wins_over_healthy() {
        let mut stats = stats_with_failure_rate(93, 7); // degraded failure
        stats.circular_errors = 1; // degraded circular errors
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn health_report_contains_snapshot_data() {
        let stats = StatsSnapshot {
            registrations_total: 42,
            resolutions_total: 100,
            resolutions_succeeded: 90,
            resolutions_failed: 10,
            circular_errors: 0,
            ..StatsSnapshot::default()
        };
        let report = HealthReport::derive(&stats);
        assert_eq!(report.registrations_total, 42);
        assert_eq!(report.circular_errors, 0);
        // 10% failure = 1000 bps > 500 degraded threshold
        assert_eq!(report.resolution_failure_rate_bps, 1_000);
    }

    #[test]
    fn threshold_constants_are_correct() {
        assert_eq!(HealthReport::FAILURE_DEGRADED_BPS, 500);
        assert_eq!(HealthReport::FAILURE_CRITICAL_BPS, 2_000);
        assert_eq!(HealthReport::CIRCULAR_DEGRADED, 1);
        assert_eq!(HealthReport::CIRCULAR_CRITICAL, 5);
    }

    #[test]
    fn failure_rate_exactly_at_degraded_threshold() {
        // 5% = 500 bps exactly at threshold.
        let stats = stats_with_failure_rate(95, 5);
        let report = HealthReport::derive(&stats);
        // > threshold triggers degraded, so exactly at is healthy
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn failure_rate_just_above_degraded_threshold() {
        // 5.01% ≈ 501 bps just above threshold.
        let stats = stats_with_failure_rate(94, 6);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }
}

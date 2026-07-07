//! [`HealthReport`], [`HealthState`] — operational health summary
//! derived from migration stats.
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
//! | `Healthy` | All migrations nominal | None |
//! | `Degraded` | Elevated failure rate or slow migrations | Investigate |
//! | `Critical` | Severe failure rate or stuck migrations | Page on-call |
//!
//! # Thresholds
//!
//! Thresholds follow a "worst signal wins" policy:
//!
//! - Failure rate > 5% (`500` bps): degraded.
//! - Failure rate > 20% (`2_000` bps): critical.
//! - Average duration > 30s (`30_000_000_000` ns): degraded.
//! - Average duration > 60s (`60_000_000_000` ns): critical.
//! - In-progress migrations at max concurrent capacity: degraded.

use crate::stats::StatsSnapshot;

// ── HealthState ─────────────────────────────────────────────────

/// Tri-state health classification for the migration service.
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
    /// Migrations currently in progress.
    pub migrations_in_progress: u64,
    /// Failure rate in basis points (0..=10_000).
    pub failure_rate_bps: u16,
    /// Average migration duration in nanoseconds.
    pub avg_duration_ns: u64,
}

impl HealthReport {
    /// Threshold: failure rate > this → degraded (bps).
    pub const FAILURE_DEGRADED_BPS: u16 = 500;
    /// Threshold: failure rate > this → critical (bps).
    pub const FAILURE_CRITICAL_BPS: u16 = 2_000;

    /// Threshold: avg duration > this → degraded (ns).
    pub const DURATION_DEGRADED_NS: u64 = 30_000_000_000;
    /// Threshold: avg duration > this → critical (ns).
    pub const DURATION_CRITICAL_NS: u64 = 60_000_000_000;

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a "worst signal wins" policy: if
    /// any individual signal is critical, the overall state is
    /// critical; otherwise the worst degraded signal wins.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let failure_rate_bps = stats.failure_rate_bps();
        let avg_duration_ns = stats.avg_duration_ns();

        let state = Self::classify(failure_rate_bps, avg_duration_ns);

        Self {
            state,
            migrations_in_progress: stats.migrations_in_progress,
            failure_rate_bps,
            avg_duration_ns,
        }
    }

    /// Combine health signals into one overall state using a
    /// "worst signal wins" policy.
    fn classify(failure_bps: u16, avg_duration_ns: u64) -> HealthState {
        // Failure rate signal.
        let failure_state = if failure_bps > Self::FAILURE_CRITICAL_BPS {
            HealthState::Critical
        } else if failure_bps > Self::FAILURE_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Duration signal (only meaningful if there are completed migrations).
        let duration_state = if avg_duration_ns == 0 {
            HealthState::Healthy
        } else if avg_duration_ns > Self::DURATION_CRITICAL_NS {
            HealthState::Critical
        } else if avg_duration_ns > Self::DURATION_DEGRADED_NS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Worst signal wins.
        worst_of(failure_state, duration_state)
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
const fn worst_of(a: HealthState, b: HealthState) -> HealthState {
    if matches!(a, HealthState::Critical) || matches!(b, HealthState::Critical) {
        return HealthState::Critical;
    }
    if matches!(a, HealthState::Degraded) || matches!(b, HealthState::Degraded) {
        return HealthState::Degraded;
    }
    HealthState::Healthy
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
    fn low_failure_rate_is_healthy() {
        let stats = StatsSnapshot {
            migrations_completed: 99,
            migrations_failed: 1,
            ..StatsSnapshot::default()
        };
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn elevated_failure_rate_is_degraded() {
        let stats = StatsSnapshot {
            migrations_completed: 94,
            migrations_failed: 6, // ~6% > 5%
            ..StatsSnapshot::default()
        };
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
    }

    #[test]
    fn high_failure_rate_is_critical() {
        let stats = StatsSnapshot {
            migrations_completed: 75,
            migrations_failed: 25, // 25% > 20%
            ..StatsSnapshot::default()
        };
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn slow_migrations_are_degraded() {
        let stats = StatsSnapshot {
            migrations_completed: 1,
            total_duration_ns: 45_000_000_000, // 45s > 30s
            ..StatsSnapshot::default()
        };
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn very_slow_migrations_are_critical() {
        let stats = StatsSnapshot {
            migrations_completed: 1,
            total_duration_ns: 90_000_000_000, // 90s > 60s
            ..StatsSnapshot::default()
        };
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn worst_signal_wins() {
        // Failure degraded + duration critical → critical.
        let stats = StatsSnapshot {
            migrations_completed: 94,
            migrations_failed: 6, // ~6% → degraded
            // 94 × 70s = avg 70s → critical (> 60s threshold)
            total_duration_ns: 94 * 70_000_000_000,
            ..StatsSnapshot::default()
        };
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn health_state_predicates() {
        assert!(HealthState::Healthy.is_healthy());
        assert!(!HealthState::Degraded.is_healthy());
        assert!(HealthState::Degraded.needs_attention());
        assert!(HealthState::Critical.is_critical());
        assert!(HealthState::Critical.needs_attention());
    }

    #[test]
    fn health_state_as_str() {
        assert_eq!(HealthState::Healthy.as_str(), "healthy");
        assert_eq!(HealthState::Degraded.as_str(), "degraded");
        assert_eq!(HealthState::Critical.as_str(), "critical");
    }
}

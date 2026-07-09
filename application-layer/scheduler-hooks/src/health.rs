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
//! | `Degraded` | Performance or availability degraded | Investigate during business hours |
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
    /// Number of hooks registered.
    pub hooks_registered: u64,
    /// Failure rate in basis points (0..=10_000).
    pub failure_rate_bps: u16,
    /// Average hook time in nanoseconds.
    pub avg_hook_time_ns: u64,
    /// Total ticks processed.
    pub tick_count: u64,
}

impl HealthReport {
    /// Threshold: failure rate > this → degraded (in bps).
    pub const FAILURE_DEGRADED_BPS: u16 = 100;
    /// Threshold: failure rate > this → critical (in bps).
    pub const FAILURE_CRITICAL_BPS: u16 = 1000;

    /// Derive a health report from a stats snapshot.
    ///
    /// Classification follows "worst signal wins" policy.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let failure_rate_bps = stats.failure_rate_bps();
        let avg_hook_time_ns = stats.avg_hook_time_ns();

        // Failure rate signal.
        let failure_state = if failure_rate_bps >= Self::FAILURE_CRITICAL_BPS {
            HealthState::Critical
        } else if failure_rate_bps >= Self::FAILURE_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Hook time signal (Phase 0: always healthy)
        let time_state = HealthState::Healthy;

        // Worst signal wins.
        let state = worst_of([failure_state, time_state]);

        Self {
            state,
            hooks_registered: stats.hooks_registered,
            failure_rate_bps,
            avg_hook_time_ns,
            tick_count: stats.tick_count,
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
    fn low_failure_rate_is_healthy() {
        let mut stats = StatsSnapshot::default();
        stats.hooks_invoked = 1000;
        stats.hooks_failed = 5; // 0.5% failure rate
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn moderate_failure_rate_is_degraded() {
        let mut stats = StatsSnapshot::default();
        stats.hooks_invoked = 100;
        stats.hooks_failed = 2; // 2% failure rate = 200 bps
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn high_failure_rate_is_critical() {
        let mut stats = StatsSnapshot::default();
        stats.hooks_invoked = 10;
        stats.hooks_failed = 5; // 50% failure rate = 5000 bps
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
        // Healthy hook time but critical failure rate
        stats.hooks_invoked = 10;
        stats.hooks_failed = 5;
        stats.total_hook_time_ns = 1000; // healthy
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn health_report_fields() {
        let mut stats = StatsSnapshot::default();
        stats.hooks_registered = 42;
        stats.hooks_invoked = 100;
        stats.hooks_failed = 1;
        stats.total_hook_time_ns = 10_000;
        stats.tick_count = 1000;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.hooks_registered, 42);
        assert_eq!(report.failure_rate_bps, 100); // 1%
        assert_eq!(report.avg_hook_time_ns, 100);
        assert_eq!(report.tick_count, 1000);
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

    #[test]
    fn failure_thresholds() {
        assert_eq!(HealthReport::FAILURE_DEGRADED_BPS, 100); // 1%
        assert_eq!(HealthReport::FAILURE_CRITICAL_BPS, 1000); // 10%
    }
}

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

// ── HealthReport ───────────────────────────────────────────────

/// Operational health snapshot derived from [`StatsSnapshot`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    /// Overall health state.
    pub state: HealthState,
    /// Number of startups completed successfully.
    pub startups_completed: u64,
    /// Startup failure rate in basis points (0..=10_000).
    pub startup_failure_rate_bps: u16,
    /// Forced shutdown rate in basis points (0..=10_000).
    pub forced_shutdown_rate_bps: u16,
    /// Number of shutdowns currently in progress.
    pub shutdowns_in_progress: u64,
}

impl HealthReport {
    /// Threshold: startup failure rate ≥ this → degraded (in bps).
    pub const STARTUP_FAILURE_DEGRADED_BPS: u16 = 500;
    /// Threshold: startup failure rate ≥ this → critical (in bps).
    pub const STARTUP_FAILURE_CRITICAL_BPS: u16 = 2_000;

    /// Threshold: forced shutdown rate > this → degraded (in bps).
    /// ANY forced shutdown is degraded.
    pub const FORCED_DEGRADED_BPS: u16 = 0;
    /// Threshold: forced shutdown rate ≥ this → critical (in bps).
    pub const FORCED_CRITICAL_BPS: u16 = 500;

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a "worst signal wins" policy: if
    /// any individual signal is critical, the overall state is
    /// critical; otherwise the worst degraded signal wins.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot, shutdowns_in_progress: u64) -> Self {
        let startup_failure_rate_bps = stats.startup_failure_rate_bps();
        let forced_shutdown_rate_bps = stats.forced_shutdown_rate_bps();

        // Compute per-signal state, then take the worst.
        let state = Self::classify(startup_failure_rate_bps, forced_shutdown_rate_bps);

        Self {
            state,
            startups_completed: stats.startups_completed,
            startup_failure_rate_bps,
            forced_shutdown_rate_bps,
            shutdowns_in_progress,
        }
    }

    /// Classify health based on signal thresholds.
    fn classify(startup_failure_bps: u16, forced_shutdown_bps: u16) -> HealthState {
        // Startup failure signal.
        let startup_state = if startup_failure_bps >= Self::STARTUP_FAILURE_CRITICAL_BPS {
            HealthState::Critical
        } else if startup_failure_bps >= Self::STARTUP_FAILURE_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Forced shutdown signal.
        let forced_state = if forced_shutdown_bps >= Self::FORCED_CRITICAL_BPS {
            HealthState::Critical
        } else if forced_shutdown_bps > Self::FORCED_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Worst signal wins.
        worst_of([startup_state, forced_state])
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

    fn stats_with_startup_failure(total: u64, failed: u64) -> StatsSnapshot {
        StatsSnapshot {
            startups_total: total,
            startups_failed: failed,
            ..StatsSnapshot::default()
        }
    }

    fn stats_with_forced_shutdown(total: u64, forced: u64) -> StatsSnapshot {
        StatsSnapshot {
            shutdowns_total: total,
            shutdowns_forced: forced,
            ..StatsSnapshot::default()
        }
    }

    #[test]
    fn empty_stats_derive_healthy() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats, 0);
        assert_eq!(report.state, HealthState::Healthy);
        assert!(report.is_healthy());
    }

    #[test]
    fn low_startup_failure_is_healthy() {
        // 2 failures out of 1000 = 0.2% = 20 bps < 500.
        let stats = stats_with_startup_failure(1000, 2);
        let report = HealthReport::derive(&stats, 0);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn moderate_startup_failure_is_degraded() {
        // 100 failures out of 1000 = 10% = 1000 bps >= 500.
        let stats = stats_with_startup_failure(1000, 100);
        let report = HealthReport::derive(&stats, 0);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn high_startup_failure_is_critical() {
        // 300 failures out of 1000 = 30% = 3000 bps >= 2000.
        let stats = stats_with_startup_failure(1000, 300);
        let report = HealthReport::derive(&stats, 0);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn any_forced_shutdown_is_degraded() {
        // 1 forced out of 100 = 1% = 100 bps (> 0, degraded, not critical).
        let stats = stats_with_forced_shutdown(100, 1);
        let report = HealthReport::derive(&stats, 0);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn high_forced_shutdown_rate_is_critical() {
        // 5 forced out of 100 = 5% = 500 bps >= 500.
        let stats = stats_with_forced_shutdown(100, 5);
        let report = HealthReport::derive(&stats, 0);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn worst_signal_wins() {
        // Healthy startup failure + critical forced shutdown = critical.
        let mut stats = stats_with_startup_failure(1000, 0); // healthy
        stats.shutdowns_total = 100;
        stats.shutdowns_forced = 5; // critical
        let report = HealthReport::derive(&stats, 0);
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
    fn health_report_includes_shutdowns_in_progress() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats, 3);
        assert_eq!(report.shutdowns_in_progress, 3);
    }

    #[test]
    fn health_report_includes_startups_completed() {
        let stats = StatsSnapshot {
            startups_completed: 42,
            ..StatsSnapshot::default()
        };
        let report = HealthReport::derive(&stats, 0);
        assert_eq!(report.startups_completed, 42);
    }

    #[test]
    fn thresholds_are_correct() {
        assert_eq!(HealthReport::STARTUP_FAILURE_DEGRADED_BPS, 500);
        assert_eq!(HealthReport::STARTUP_FAILURE_CRITICAL_BPS, 2000);
        assert_eq!(HealthReport::FORCED_DEGRADED_BPS, 0);
        assert_eq!(HealthReport::FORCED_CRITICAL_BPS, 500);
    }
}

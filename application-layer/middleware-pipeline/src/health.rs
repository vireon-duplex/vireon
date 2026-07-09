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
//! | `Critical` | Severe impact; page on-call immediately |

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
    /// Total pipeline executions.
    pub executions_total: u64,
    /// Rejection rate in basis points (0..=10_000).
    pub rejection_rate_bps: u16,
    /// Average execution time in ns.
    pub avg_time_ns: u64,
}

impl HealthReport {
    /// Threshold: rejection rate > this → degraded (in bps).
    pub const REJECTION_DEGRADED_BPS: u16 = 1_000;
    /// Threshold: rejection rate > this → critical (in bps).
    pub const REJECTION_CRITICAL_BPS: u16 = 5_000;

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a simple threshold-based policy
    /// on rejection rate and average execution time.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let rejection_rate_bps = stats.rejection_rate_bps();
        let avg_time_ns = stats.avg_time_ns();

        // Determine state based on rejection rate.
        let state = if rejection_rate_bps > Self::REJECTION_CRITICAL_BPS {
            HealthState::Critical
        } else if rejection_rate_bps > Self::REJECTION_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        Self {
            state,
            executions_total: stats.executions_total,
            rejection_rate_bps,
            avg_time_ns,
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
    fn low_rejection_rate_is_healthy() {
        let mut stats = StatsSnapshot::default();
        stats.executions_total = 100;
        stats.executions_rejected = 5; // 5% < 10%.
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn medium_rejection_rate_is_degraded() {
        let mut stats = StatsSnapshot::default();
        stats.executions_total = 100;
        stats.executions_rejected = 25; // 25% > 10% but < 50%.
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn high_rejection_rate_is_critical() {
        let mut stats = StatsSnapshot::default();
        stats.executions_total = 100;
        stats.executions_rejected = 60; // 60% > 50%.
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
    fn executions_total_reflected_in_report() {
        let mut stats = StatsSnapshot::default();
        stats.executions_total = 1234;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.executions_total, 1234);
    }

    #[test]
    fn rejection_rate_reflected_in_report() {
        let mut stats = StatsSnapshot::default();
        stats.executions_total = 100;
        stats.executions_rejected = 30; // 30% = 3000 bps.
        let report = HealthReport::derive(&stats);
        assert_eq!(report.rejection_rate_bps, 3_000);
    }

    #[test]
    fn avg_time_reflected_in_report() {
        let mut stats = StatsSnapshot::default();
        stats.executions_total = 10;
        stats.total_time_ns = 50_000; // 50_000 / 10 = 5_000.
        let report = HealthReport::derive(&stats);
        assert_eq!(report.avg_time_ns, 5_000);
    }

    #[test]
    fn threshold_constants_are_reasonable() {
        assert!(HealthReport::REJECTION_DEGRADED_BPS < HealthReport::REJECTION_CRITICAL_BPS);
        assert_eq!(HealthReport::REJECTION_DEGRADED_BPS, 1_000); // 10%
        assert_eq!(HealthReport::REJECTION_CRITICAL_BPS, 5_000); // 50%
    }
}

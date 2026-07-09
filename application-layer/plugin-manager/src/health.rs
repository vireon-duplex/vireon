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

// ── HealthReport ────────────────────────────────────────────────

/// Operational health snapshot derived from [`StatsSnapshot`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    /// Overall health state.
    pub state: HealthState,
    /// Number of plugins currently loaded.
    pub plugins_loaded: u64,
    /// Load failure rate in basis points (0..=10_000).
    pub load_failure_rate_bps: u16,
    /// API version mismatches detected.
    pub api_mismatches: u64,
}

impl HealthReport {
    /// Threshold: load failure rate > this → degraded (in bps).
    pub const LOAD_FAILURE_DEGRADED_BPS: u16 = 500;
    /// Threshold: load failure rate > this → critical (in bps).
    pub const LOAD_FAILURE_CRITICAL_BPS: u16 = 2_000;

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a simple threshold policy:
    /// - Load failure rate > 5% (500 bps) → degraded
    /// - Load failure rate > 20% (2000 bps) → critical
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let load_failure_rate_bps = stats.load_failure_rate_bps();

        let state = if load_failure_rate_bps > Self::LOAD_FAILURE_CRITICAL_BPS {
            HealthState::Critical
        } else if load_failure_rate_bps > Self::LOAD_FAILURE_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        Self {
            state,
            plugins_loaded: stats.plugins_loaded,
            load_failure_rate_bps,
            api_mismatches: stats.api_mismatches,
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

    fn stats_with_load_failures(loaded: u64, failed: u64) -> StatsSnapshot {
        StatsSnapshot {
            plugins_loaded: loaded,
            load_failures: failed,
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
    fn zero_load_failure_rate_is_healthy() {
        let stats = stats_with_load_failures(10, 0);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert_eq!(report.load_failure_rate_bps, 0);
    }

    #[test]
    fn load_failure_rate_below_threshold_is_healthy() {
        // 3 failures / 1000 attempts = 0.3% = 30 bps < 500.
        let stats = stats_with_load_failures(997, 3);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn load_failure_rate_above_degraded_threshold() {
        // 100 failures / 1000 attempts = 10% = 1000 bps > 500.
        let stats = stats_with_load_failures(900, 100);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn load_failure_rate_above_critical_threshold() {
        // 300 failures / 1000 attempts = 30% = 3000 bps > 2000.
        let stats = stats_with_load_failures(700, 300);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
        assert!(report.needs_attention());
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
    fn health_report_includes_api_mismatches() {
        let mut stats = stats_with_load_failures(100, 0);
        stats.api_mismatches = 5;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.api_mismatches, 5);
        // API mismatches don't affect health in Phase 0.
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn health_report_includes_plugins_loaded() {
        let stats = stats_with_load_failures(42, 0);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.plugins_loaded, 42);
    }

    #[test]
    fn load_failure_rate_at_degraded_threshold() {
        // Exactly 5% failure = 500 bps.
        let stats = stats_with_load_failures(95, 5);
        let report = HealthReport::derive(&stats);
        // At exactly 500 bps, should still be healthy.
        // Only > 500 triggers degraded.
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn load_failure_rate_at_critical_threshold() {
        // Exactly 20% failure = 2000 bps.
        let stats = stats_with_load_failures(80, 20);
        let report = HealthReport::derive(&stats);
        // At exactly 2000 bps, should be degraded (not critical yet).
        // Only > 2000 triggers critical.
        assert_eq!(report.state, HealthState::Degraded);
    }
}

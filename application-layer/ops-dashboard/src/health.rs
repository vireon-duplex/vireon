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
//!
//! # Thresholds
//!
//! Thresholds are based on query success rate in basis points:
//!
//! - Success rate ≥ 99% (`9900` bps): Healthy.
//! - Success rate ≥ 95% (`9500` bps): Degraded.
//! - Success rate ≥ 90% (`9000` bps): Still Degraded.
//! - Success rate < 90% (`9000` bps): Critical.

use crate::stats::StatsSnapshot;

// ── Constants ───────────────────────────────────────────────────

/// Threshold: success rate ≥ this → Healthy (in bps).
pub const SUCCESS_HEALTHY_BPS: u16 = 9_900;
/// Threshold: success rate ≥ this → Degraded (in bps).
pub const SUCCESS_DEGRADED_BPS: u16 = 9_500;
/// Threshold: success rate < this → Critical (in bps).
pub const SUCCESS_CRITICAL_BPS: u16 = 9_000;

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

    /// Worst-of two health states (worst-signal-wins).
    ///
    /// `Critical` > `Degraded` > `Healthy`.
    #[must_use]
    pub const fn worst_of(a: Self, b: Self) -> Self {
        match (a, b) {
            (Self::Critical, _) | (_, Self::Critical) => Self::Critical,
            (Self::Degraded, _) | (_, Self::Degraded) => Self::Degraded,
            (Self::Healthy, Self::Healthy) => Self::Healthy,
        }
    }
}

// ── HealthReport ────────────────────────────────────────────────

/// Operational health snapshot derived from [`StatsSnapshot`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    /// Overall health state.
    pub state: HealthState,
    /// Total queries since startup.
    pub queries_total: u64,
    /// Widgets rendered since startup.
    pub widgets_rendered: u64,
    /// Data points fetched since startup.
    pub data_points_fetched: u64,
    /// Query success rate in basis points (0..=10_000).
    pub query_success_rate_bps: u16,
}

impl HealthReport {
    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a worst-signal-wins policy based
    /// on query success rate:
    /// - ≥ 99% (9900 bps): Healthy
    /// - ≥ 95% (9500 bps): Degraded
    /// - ≥ 90% (9000 bps): Degraded
    /// - < 90%: Critical
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let query_success_rate_bps = stats.query_success_rate_bps();

        let state = if query_success_rate_bps >= SUCCESS_HEALTHY_BPS {
            HealthState::Healthy
        } else if query_success_rate_bps >= SUCCESS_CRITICAL_BPS {
            HealthState::Degraded
        } else {
            HealthState::Critical
        };

        Self {
            state,
            queries_total: stats.queries_total,
            widgets_rendered: stats.widgets_rendered,
            data_points_fetched: stats.data_points_fetched,
            query_success_rate_bps,
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

    fn stats_with_queries(total: u64, succeeded: u64) -> StatsSnapshot {
        StatsSnapshot {
            queries_total: total,
            queries_succeeded: succeeded,
            queries_failed: total - succeeded,
            ..StatsSnapshot::default()
        }
    }

    #[test]
    fn empty_stats_derive_healthy() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert!(report.is_healthy());
        // No queries → 100% success rate.
        assert_eq!(report.query_success_rate_bps, 10_000);
    }

    #[test]
    fn high_success_rate_is_healthy() {
        // 9910/10000 = 99.1% ≥ 99%.
        let stats = stats_with_queries(10_000, 9_910);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn moderate_success_rate_is_degraded() {
        // 9600/10000 = 96% < 99%, ≥ 90%.
        let stats = stats_with_queries(10_000, 9_600);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn low_success_rate_is_critical() {
        // 8000/10000 = 80% < 90%.
        let stats = stats_with_queries(10_000, 8_000);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn boundary_healthy_to_degraded() {
        // Exactly 99% = 9900 bps → Healthy (≥ threshold).
        let stats = stats_with_queries(10_000, 9_900);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn boundary_degraded_to_critical() {
        // Exactly 90% = 9000 bps → Degraded (≥ critical threshold).
        let stats = stats_with_queries(10_000, 9_000);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn just_below_critical_threshold() {
        // 8999/10000 = 89.99% < 90%.
        let stats = stats_with_queries(10_000, 8_999);
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
    fn worst_of_returns_worst() {
        assert_eq!(
            HealthState::worst_of(HealthState::Healthy, HealthState::Healthy),
            HealthState::Healthy
        );
        assert_eq!(
            HealthState::worst_of(HealthState::Healthy, HealthState::Degraded),
            HealthState::Degraded
        );
        assert_eq!(
            HealthState::worst_of(HealthState::Degraded, HealthState::Healthy),
            HealthState::Degraded
        );
        assert_eq!(
            HealthState::worst_of(HealthState::Healthy, HealthState::Critical),
            HealthState::Critical
        );
        assert_eq!(
            HealthState::worst_of(HealthState::Critical, HealthState::Degraded),
            HealthState::Critical
        );
        assert_eq!(
            HealthState::worst_of(HealthState::Critical, HealthState::Critical),
            HealthState::Critical
        );
    }

    #[test]
    fn report_includes_stats_fields() {
        let mut stats = stats_with_queries(100, 95);
        stats.widgets_rendered = 500;
        stats.data_points_fetched = 10_000;

        let report = HealthReport::derive(&stats);
        assert_eq!(report.queries_total, 100);
        assert_eq!(report.widgets_rendered, 500);
        assert_eq!(report.data_points_fetched, 10_000);
    }
}

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
//! Thresholds are encoded in [`HealthReport::derive`]. The numbers
//! are derived from typical SLO targets for observability systems:
//!
//! - Export failure rate ≤ 5% (`500` bps): healthy.
//! - Export failure rate > 5% (`500` bps): degraded.
//! - Export failure rate > 20% (`2000` bps): critical.

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
    /// Number of metrics recorded since startup.
    pub metrics_recorded: u64,
    /// Number of spans currently active (opened - closed).
    pub spans_active: u64,
    /// Export failure rate in basis points (0..=10_000).
    pub export_failure_rate_bps: u16,
}

impl HealthReport {
    /// Threshold: export failure rate > this → degraded (in bps).
    pub const EXPORT_FAILURE_DEGRADED_BPS: u16 = 500;
    /// Threshold: export failure rate > this → critical (in bps).
    pub const EXPORT_FAILURE_CRITICAL_BPS: u16 = 2_000;

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a simple policy based on export
    /// failure rate:
    /// - ≤ 5% (500 bps): Healthy
    /// - > 5% and ≤ 20%: Degraded
    /// - > 20%: Critical
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let export_failure_rate_bps = stats.export_failure_rate_bps();
        let spans_active = stats.active_spans();

        let state = if export_failure_rate_bps > Self::EXPORT_FAILURE_CRITICAL_BPS {
            HealthState::Critical
        } else if export_failure_rate_bps > Self::EXPORT_FAILURE_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        Self {
            state,
            metrics_recorded: stats.metrics_recorded,
            spans_active,
            export_failure_rate_bps,
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

    fn stats_with_exports(total: u64, failed: u64) -> StatsSnapshot {
        StatsSnapshot {
            exports_total: total,
            exports_failed: failed,
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
    fn low_export_failure_is_healthy() {
        // 3 failures out of 100 = 3% < 5%.
        let stats = stats_with_exports(100, 3);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn moderate_export_failure_is_degraded() {
        // 10 failures out of 100 = 10% > 5%, ≤ 20%.
        let stats = stats_with_exports(100, 10);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn high_export_failure_is_critical() {
        // 30 failures out of 100 = 30% > 20%.
        let stats = stats_with_exports(100, 30);
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
    fn report_includes_metrics_and_spans() {
        let mut stats = stats_with_exports(100, 5);
        stats.metrics_recorded = 1_000;
        stats.spans_opened = 50;
        stats.spans_closed = 30;

        let report = HealthReport::derive(&stats);
        assert_eq!(report.metrics_recorded, 1_000);
        assert_eq!(report.spans_active, 20); // 50 - 30
    }
}

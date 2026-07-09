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
//! are derived from typical SLO targets for quota systems:
//!
//! - Exceed rate ≤ 1% (`100` bps): healthy.
//! - Exceed rate > 1% (`100` bps): degraded.
//! - Exceed rate > 10% (`1000` bps): critical.

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
    /// Number of tenants tracked.
    pub tenants_tracked: u64,
    /// Total quota checks performed.
    pub checks_total: u64,
    /// Exceed rate in basis points (0..=10_000).
    pub exceed_rate_bps: u16,
    /// Total bandwidth across all tenants (bps).
    pub bandwidth_total_bps: u64,
}

impl HealthReport {
    /// Threshold: exceed rate > this → degraded (in bps).
    pub const EXCEED_DEGRADED_BPS: u16 = 100;
    /// Threshold: exceed rate > this → critical (in bps).
    pub const EXCEED_CRITICAL_BPS: u16 = 1_000;

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a simple policy based on exceed
    /// rate:
    /// - ≤ 1% (100 bps): Healthy
    /// - > 1% and ≤ 10%: Degraded
    /// - > 10%: Critical
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let exceed_rate_bps = stats.exceed_rate_bps();

        let state = if exceed_rate_bps > Self::EXCEED_CRITICAL_BPS {
            HealthState::Critical
        } else if exceed_rate_bps > Self::EXCEED_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        Self {
            state,
            tenants_tracked: stats.tenants_tracked,
            checks_total: stats.checks_total,
            exceed_rate_bps,
            bandwidth_total_bps: stats.bandwidth_total_bps,
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

    fn stats_with_exceeds(total: u64, exceeded: u64) -> StatsSnapshot {
        StatsSnapshot {
            checks_total: total,
            checks_exceeded: exceeded,
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
    fn low_exceed_rate_is_healthy() {
        // 5 exceeds out of 1000 = 0.5% < 1%.
        let stats = stats_with_exceeds(1000, 5);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn moderate_exceed_rate_is_degraded() {
        // 30 exceeds out of 1000 = 3% > 1%, ≤ 10%.
        let stats = stats_with_exceeds(1000, 30);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn high_exceed_rate_is_critical() {
        // 150 exceeds out of 1000 = 15% > 10%.
        let stats = stats_with_exceeds(1000, 150);
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
    fn report_includes_tenants_and_bandwidth() {
        let mut stats = stats_with_exceeds(1000, 10);
        stats.tenants_tracked = 100;
        stats.bandwidth_total_bps = 5_000_000_000;

        let report = HealthReport::derive(&stats);
        assert_eq!(report.tenants_tracked, 100);
        assert_eq!(report.bandwidth_total_bps, 5_000_000_000);
    }

    #[test]
    fn exceed_threshold_constants() {
        assert_eq!(HealthReport::EXCEED_DEGRADED_BPS, 100); // 1%
        assert_eq!(HealthReport::EXCEED_CRITICAL_BPS, 1_000); // 10%
    }
}

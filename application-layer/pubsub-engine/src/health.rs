//! [`HealthReport`], [`HealthState`] — operational health summary
//! derived from pub/sub engine stats.
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

// ── HealthReport ────────────────────────────────────────────────

/// Operational health snapshot derived from [`StatsSnapshot`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    /// Overall health state.
    pub state: HealthState,
    /// Total messages published.
    pub messages_published: u64,
    /// Delivery success rate in basis points (0..=10_000).
    pub delivery_success_rate_bps: u16,
    /// Delivery failure rate in basis points (0..=10_000).
    pub delivery_failure_rate_bps: u16,
    /// Currently active subscriptions.
    pub active_subscriptions: u64,
}

impl HealthReport {
    /// Threshold: delivery failure rate > this → degraded (in bps).
    pub const FAILURE_DEGRADED_BPS: u16 = 100; // 1%
    /// Threshold: delivery failure rate > this → critical (in bps).
    pub const FAILURE_CRITICAL_BPS: u16 = 500; // 5%

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a "worst signal wins" policy: if
    /// any individual signal is critical, the overall state is
    /// critical; otherwise the worst degraded signal wins.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let delivery_success_rate_bps = stats.delivery_success_rate_bps();
        let delivery_failure_rate_bps = stats.delivery_failure_rate_bps();

        // Compute per-signal state, then take the worst.
        let state = Self::classify(delivery_failure_rate_bps);

        Self {
            state,
            messages_published: stats.messages_published,
            delivery_success_rate_bps,
            delivery_failure_rate_bps,
            active_subscriptions: stats.subscriptions_active,
        }
    }

    /// Classify the health state from the delivery failure rate.
    ///
    /// "Worst signal wins" — currently only the failure rate is
    /// used, but this can be extended in future phases.
    fn classify(failure_rate_bps: u16) -> HealthState {
        if failure_rate_bps > Self::FAILURE_CRITICAL_BPS {
            HealthState::Critical
        } else if failure_rate_bps > Self::FAILURE_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
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

    fn stats_with_failures(delivered: u64, failures: u64) -> StatsSnapshot {
        StatsSnapshot {
            messages_delivered: delivered,
            delivery_failures: failures,
            ..StatsSnapshot::default()
        }
    }

    #[test]
    fn empty_stats_derive_healthy() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert!(report.is_healthy());
        assert_eq!(report.messages_published, 0);
        assert_eq!(report.active_subscriptions, 0);
    }

    #[test]
    fn low_failure_rate_is_healthy() {
        // 0.5% failures < 1% threshold.
        let stats = stats_with_failures(1000, 5);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn moderate_failure_rate_is_degraded() {
        // 2% failures > 1% but < 5% threshold.
        let stats = stats_with_failures(1000, 20);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn high_failure_rate_is_critical() {
        // 10% failures > 5% threshold.
        let stats = stats_with_failures(1000, 100);
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
    fn healthy_report_needs_no_attention() {
        let stats = stats_with_failures(1000, 0);
        let report = HealthReport::derive(&stats);
        assert!(report.is_healthy());
        assert!(!report.needs_attention());
    }

    #[test]
    fn degraded_report_needs_attention() {
        let stats = stats_with_failures(1000, 20);
        let report = HealthReport::derive(&stats);
        assert!(!report.is_healthy());
        assert!(report.needs_attention());
    }

    #[test]
    fn critical_report_needs_attention() {
        let stats = stats_with_failures(1000, 100);
        let report = HealthReport::derive(&stats);
        assert!(!report.is_healthy());
        assert!(report.needs_attention());
    }

    #[test]
    fn report_carries_stats_values() {
        let stats = StatsSnapshot {
            messages_published: 500,
            messages_delivered: 480,
            delivery_failures: 20,
            subscriptions_active: 10,
            subscriptions_created: 15,
        };
        let report = HealthReport::derive(&stats);
        assert_eq!(report.messages_published, 500);
        assert_eq!(report.active_subscriptions, 10);
        // 480 delivered / 500 total = 96% = 9600 bps success
        assert_eq!(report.delivery_success_rate_bps, 9_600);
        // 20 failures / 500 total = 4% = 400 bps failure
        assert_eq!(report.delivery_failure_rate_bps, 400);
    }

    #[test]
    fn no_deliveries_is_healthy() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert_eq!(report.delivery_success_rate_bps, 10_000);
        assert_eq!(report.delivery_failure_rate_bps, 0);
    }

    #[test]
    fn all_failures_is_critical() {
        let stats = stats_with_failures(0, 100);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
        assert_eq!(report.delivery_success_rate_bps, 0);
        assert_eq!(report.delivery_failure_rate_bps, 10_000);
    }
}

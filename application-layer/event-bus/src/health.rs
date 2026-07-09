//! [`HealthReport`], [`HealthState`] — operational health summary
//! derived from event bus stats.
//!
//! See `DESIGN.md` Part 12 (observability).

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
    /// Total messages published.
    pub messages_published: u64,
    /// Publish error rate in basis points (0..=10_000).
    pub publish_error_rate_bps: u16,
    /// Whether there are active rebalances.
    pub active_rebalances: bool,
}

impl HealthReport {
    /// Threshold: publish error rate > this → degraded (in bps).
    pub const ERROR_DEGRADED_BPS: u16 = 100; // 1%
    /// Threshold: publish error rate > this → critical (in bps).
    pub const ERROR_CRITICAL_BPS: u16 = 500; // 5%

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a "worst signal wins" policy: if
    /// any individual signal is critical, the overall state is
    /// critical; otherwise the worst degraded signal wins.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let publish_error_rate_bps = stats.publish_error_rate_bps();
        let active_rebalances = stats.has_active_rebalances();

        // Compute per-signal state, then take the worst.
        let state = Self::classify(publish_error_rate_bps, active_rebalances);

        Self {
            state,
            messages_published: stats.messages_published,
            publish_error_rate_bps,
            active_rebalances,
        }
    }

    /// Combine multiple health signals into one overall state
    /// using a "worst signal wins" policy.
    fn classify(error_rate_bps: u16, active_rebalances: bool) -> HealthState {
        // Error rate signal.
        let error_state = if error_rate_bps > Self::ERROR_CRITICAL_BPS {
            HealthState::Critical
        } else if error_rate_bps > Self::ERROR_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Rebalance signal (active rebalances are degraded).
        let rebalance_state = if active_rebalances {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Worst signal wins.
        worst_of([error_state, rebalance_state])
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

    fn stats_with_errors(published: u64, errors: u64) -> StatsSnapshot {
        StatsSnapshot {
            messages_published: published,
            publish_errors: errors,
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
        assert!(!report.active_rebalances);
    }

    #[test]
    fn low_error_rate_is_healthy() {
        // 0.5% errors < 1% threshold.
        let stats = stats_with_errors(1000, 5);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn moderate_error_rate_is_degraded() {
        // 2% errors > 1% but < 5% threshold.
        let stats = stats_with_errors(1000, 20);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn high_error_rate_is_critical() {
        // 10% errors > 5% threshold.
        let stats = stats_with_errors(1000, 100);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn active_rebalances_is_degraded() {
        let mut stats = StatsSnapshot::default();
        stats.rebalance_count = 1;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.active_rebalances);
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
        // High error rate + active rebalances = critical (error wins)
        let mut stats = stats_with_errors(1000, 100); // critical error rate
        stats.rebalance_count = 1; // degraded rebalance
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn healthy_report_needs_no_attention() {
        let stats = stats_with_errors(1000, 0);
        let report = HealthReport::derive(&stats);
        assert!(report.is_healthy());
        assert!(!report.needs_attention());
    }

    #[test]
    fn degraded_report_needs_attention() {
        let stats = stats_with_errors(1000, 20);
        let report = HealthReport::derive(&stats);
        assert!(!report.is_healthy());
        assert!(report.needs_attention());
    }

    #[test]
    fn critical_report_needs_attention() {
        let stats = stats_with_errors(1000, 100);
        let report = HealthReport::derive(&stats);
        assert!(!report.is_healthy());
        assert!(report.needs_attention());
    }
}

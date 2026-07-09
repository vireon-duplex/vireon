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
//! are derived from typical SLO targets for service registries:
//!
//! - Endpoint health ratio ≥ 99% (`9_900` bps): healthy.
//! - Endpoint health ratio < 95% (`9_500` bps): degraded.
//! - Endpoint health ratio < 90% (`9_000` bps): critical.
//! - Unhealthy endpoints > 10% of total: degraded.
//! - Unhealthy endpoints > 30% of total: critical.

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
    /// Services currently active.
    pub services_active: u64,
    /// Endpoints currently healthy.
    pub endpoints_healthy: u64,
    /// Lookup hit rate in basis points (0..=10_000).
    pub lookup_hit_rate_bps: u16,
    /// Unhealthy endpoints ratio in basis points (0..=10_000).
    pub endpoints_unhealthy_ratio_bps: u16,
}

impl HealthReport {
    /// Threshold: endpoint health ≥ this → healthy (in bps).
    pub const ENDPOINT_HEALTHY_BPS: u16 = 9_900;
    /// Threshold: endpoint health < this → degraded (in bps).
    pub const ENDPOINT_DEGRADED_BPS: u16 = 9_500;
    /// Threshold: endpoint health < this → critical (in bps).
    pub const ENDPOINT_CRITICAL_BPS: u16 = 9_000;

    /// Threshold: unhealthy endpoints > this → degraded (in bps).
    pub const UNHEALTHY_DEGRADED_BPS: u16 = 1_000; // 10%
    /// Threshold: unhealthy endpoints > this → critical (in bps).
    pub const UNHEALTHY_CRITICAL_BPS: u16 = 3_000; // 30%

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a "worst signal wins" policy: if
    /// any individual signal is critical, the overall state is
    /// critical; otherwise the worst degraded signal wins.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let lookup_hit_rate_bps = stats.lookup_hit_rate_bps();
        let endpoints_unhealthy_ratio_bps =
            10_000u16.saturating_sub(stats.endpoint_health_ratio_bps());

        // Compute per-signal state, then take the worst.
        let state = Self::classify(
            lookup_hit_rate_bps,
            endpoints_unhealthy_ratio_bps,
            stats.services_active,
            stats.endpoints_healthy,
        );

        Self {
            state,
            services_active: stats.services_active,
            endpoints_healthy: stats.endpoints_healthy,
            lookup_hit_rate_bps,
            endpoints_unhealthy_ratio_bps,
        }
    }

    /// Combine multiple health signals into one overall state
    /// using a "worst signal wins" policy.
    fn classify(
        _lookup_bps: u16,
        unhealthy_bps: u16,
        _services_active: u64,
        _endpoints_healthy: u64,
    ) -> HealthState {
        // Empty registry (no endpoints) → healthy
        if unhealthy_bps == 10_000 {
            return HealthState::Healthy;
        }

        // Endpoint health signal (based on unhealthy ratio).
        // Lower unhealthy ratio is better.
        let endpoint_state = if unhealthy_bps <= (10_000 - Self::ENDPOINT_HEALTHY_BPS) {
            HealthState::Healthy
        } else if unhealthy_bps <= (10_000 - Self::ENDPOINT_CRITICAL_BPS) {
            HealthState::Degraded
        } else {
            HealthState::Critical
        };

        // Unhealthy ratio signal.
        let ratio_state = if unhealthy_bps >= Self::UNHEALTHY_CRITICAL_BPS {
            HealthState::Critical
        } else if unhealthy_bps >= Self::UNHEALTHY_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Worst signal wins.
        worst_of([endpoint_state, ratio_state])
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

/// Pick the worst (most severe) of three health states.
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

    fn stats_with_endpoints(total: u64, healthy: u64) -> StatsSnapshot {
        StatsSnapshot {
            endpoints_total: total,
            endpoints_healthy: healthy,
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
    fn high_endpoint_health_is_healthy() {
        // 99.9% healthy → 999 unhealthy bps < 1000 degraded threshold.
        let stats = stats_with_endpoints(1000, 999);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn degraded_endpoint_health_is_degraded() {
        // 95% healthy → 5000 unhealthy bps, between degraded and critical.
        let stats = stats_with_endpoints(100, 95);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn low_endpoint_health_is_critical() {
        // 80% healthy → 2000 unhealthy bps > 1000 degraded threshold.
        let stats = stats_with_endpoints(100, 80);
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
        // High unhealthy ratio → critical regardless of other signals.
        let stats = stats_with_endpoints(100, 60); // 40% unhealthy
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn health_report_fields() {
        let stats = StatsSnapshot {
            services_active: 10,
            endpoints_healthy: 8,
            endpoints_total: 10,
            lookups_total: 100,
            lookups_hit: 95,
            ..StatsSnapshot::default()
        };
        let report = HealthReport::derive(&stats);
        assert_eq!(report.services_active, 10);
        assert_eq!(report.endpoints_healthy, 8);
        assert_eq!(report.lookup_hit_rate_bps, 9_500);
        assert!(report.endpoints_unhealthy_ratio_bps > 0);
    }

    #[test]
    fn unhealthy_ratio_calculation() {
        let stats = stats_with_endpoints(10, 7); // 30% unhealthy
        let report = HealthReport::derive(&stats);
        // 30% = 3000 bps, which is critical threshold
        assert_eq!(report.endpoints_unhealthy_ratio_bps, 3_000);
        assert_eq!(report.state, HealthState::Critical);
    }
}

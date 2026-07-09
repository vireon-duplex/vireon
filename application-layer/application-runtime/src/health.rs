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
//! Thresholds are based on startup success rate in basis points:
//!
//! - Success rate >= 99% (`9900` bps): Healthy.
//! - Success rate >= 90% (`9000` bps): Degraded.
//! - Success rate < 90% (`9000` bps): Critical.

use crate::stats::StatsSnapshot;

// ── Constants ───────────────────────────────────────────────────

/// Threshold: success rate >= this -> Healthy (in bps).
pub const SUCCESS_HEALTHY_BPS: u16 = 9_900;
/// Threshold: success rate >= this -> Degraded (in bps).
pub const SUCCESS_DEGRADED_BPS: u16 = 9_000;
/// Threshold: success rate < this -> Critical (in bps).
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
    /// Modules currently loaded.
    pub modules_loaded: u64,
    /// Modules currently started.
    pub modules_started: u64,
    /// Modules that failed since startup.
    pub modules_failed: u64,
    /// Total startup attempts since process start.
    pub startups_total: u64,
    /// Total shutdown attempts since process start.
    pub shutdowns_total: u64,
    /// Total health checks performed.
    pub health_checks_total: u64,
    /// Startup success rate in basis points (0..=10_000).
    pub startup_success_rate_bps: u16,
}

impl HealthReport {
    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a worst-signal-wins policy based
    /// on startup success rate:
    /// - >= 99% (9900 bps): Healthy
    /// - >= 90% (9000 bps): Degraded
    /// - < 90%: Critical
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let startup_success_rate_bps = stats.startup_success_rate_bps();

        let state = if startup_success_rate_bps >= SUCCESS_HEALTHY_BPS {
            HealthState::Healthy
        } else if startup_success_rate_bps >= SUCCESS_CRITICAL_BPS {
            HealthState::Degraded
        } else {
            HealthState::Critical
        };

        Self {
            state,
            modules_loaded: stats.modules_loaded,
            modules_started: stats.modules_started,
            modules_failed: stats.modules_failed,
            startups_total: stats.startups_total,
            shutdowns_total: stats.shutdowns_total,
            health_checks_total: stats.health_checks_total,
            startup_success_rate_bps,
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

    fn stats_with_startups(total: u64, failures: u64) -> StatsSnapshot {
        StatsSnapshot {
            startups_total: total,
            modules_failed: failures,
            modules_loaded: total,
            ..StatsSnapshot::default()
        }
    }

    #[test]
    fn empty_stats_derive_healthy() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert!(report.is_healthy());
        // No startups -> 100% success rate.
        assert_eq!(report.startup_success_rate_bps, 10_000);
    }

    #[test]
    fn high_success_rate_is_healthy() {
        // 9910/10000 = 99.1% >= 99%.
        let stats = stats_with_startups(10_000, 90);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn moderate_success_rate_is_degraded() {
        // 9600/10000 = 96% < 99%, >= 90%.
        let stats = stats_with_startups(10_000, 400);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn low_success_rate_is_critical() {
        // 8000/10000 = 80% < 90%.
        let stats = stats_with_startups(10_000, 2_000);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn boundary_healthy_to_degraded() {
        // Exactly 99% = 9900 bps -> Healthy (>= threshold).
        let stats = stats_with_startups(10_000, 100);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn boundary_degraded_to_critical() {
        // Exactly 90% = 9000 bps -> Degraded (>= critical threshold).
        let stats = stats_with_startups(10_000, 1_000);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn just_below_critical_threshold() {
        // 8999/10000 = 89.99% < 90%.
        let stats = stats_with_startups(10_000, 1_001);
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
        let stats = StatsSnapshot {
            modules_loaded: 10,
            modules_started: 8,
            modules_failed: 2,
            startups_total: 10,
            shutdowns_total: 5,
            health_checks_total: 100,
        };
        let report = HealthReport::derive(&stats);
        assert_eq!(report.modules_loaded, 10);
        assert_eq!(report.modules_started, 8);
        assert_eq!(report.modules_failed, 2);
        assert_eq!(report.startups_total, 10);
        assert_eq!(report.shutdowns_total, 5);
        assert_eq!(report.health_checks_total, 100);
    }
}

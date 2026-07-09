//! [`HealthReport`], [`HealthState`] — operational health summary.

use crate::stats::StatsSnapshot;

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum HealthState {
    #[default]
    Healthy,
    Degraded,
    Critical,
}

impl HealthState {
    #[must_use]
    pub const fn is_healthy(self) -> bool {
        matches!(self, Self::Healthy)
    }
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Critical => "critical",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    pub state: HealthState,
    pub auth_failure_rate_bps: u16,
    pub authz_denial_rate_bps: u16,
    pub rate_limited_count: u64,
}

impl HealthReport {
    pub const AUTH_FAILURE_DEGRADED_BPS: u16 = 500;
    pub const AUTH_FAILURE_CRITICAL_BPS: u16 = 2_000;
    pub const AUTHZ_DENIAL_DEGRADED_BPS: u16 = 1_000;
    pub const AUTHZ_DENIAL_CRITICAL_BPS: u16 = 3_000;

    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let auth_failure_rate_bps = stats.auth_failure_rate_bps();
        let authz_denial_rate_bps = stats.authz_denial_rate_bps();

        let auth_state = if auth_failure_rate_bps >= Self::AUTH_FAILURE_CRITICAL_BPS {
            HealthState::Critical
        } else if auth_failure_rate_bps >= Self::AUTH_FAILURE_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        let authz_state = if authz_denial_rate_bps >= Self::AUTHZ_DENIAL_CRITICAL_BPS {
            HealthState::Critical
        } else if authz_denial_rate_bps >= Self::AUTHZ_DENIAL_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        let state = if auth_state == HealthState::Critical || authz_state == HealthState::Critical {
            HealthState::Critical
        } else if auth_state == HealthState::Degraded || authz_state == HealthState::Degraded {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        Self {
            state,
            auth_failure_rate_bps,
            authz_denial_rate_bps,
            rate_limited_count: stats.rate_limited_count,
        }
    }

    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        self.state.is_healthy()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_stats_healthy() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn high_failure_rate_critical() {
        let mut stats = StatsSnapshot::default();
        stats.auth_attempts_total = 100;
        stats.auth_failed = 25; // 25% > 20% critical
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }
}

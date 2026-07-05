//! [`HealthReport`] + [`HealthState`] — derived from
//! [`StatsCollector`] and engine state.
//!
//! Phase 14 — Observability. See `DESIGN.md` §4.14.
//!
//! Mirrors the pattern of `connection_shard::health::HealthReport`.
//!
//! States:
//!
//! - **Healthy** — all counters within nominal bands
//! - **Degraded** — packet loss > 1%, or handshake failure > 5%,
//!   or memory pressure
//! - **Critical** — packet loss > 10%, or handshake failure > 20%,
//!   or engine is shutting down

use crate::stats::StatsSnapshot;

/// Phase-0 skeleton. Phase 14 will compute derived metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    /// Overall state.
    pub state: HealthState,
    /// Loss rate (0.0 ..= 1.0).
    pub loss_rate_milli: u16,
    /// Handshake failure rate (0.0 ..= 1.0).
    pub handshake_failure_rate_milli: u16,
}

/// Tri-state classification.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HealthState {
    /// All metrics nominal.
    #[default]
    Healthy,
    /// Some metric above soft threshold.
    Degraded,
    /// Some metric above hard threshold.
    Critical,
}

impl HealthState {
    /// Whether the state requires operator action.
    #[must_use]
    pub const fn is_actionable(self) -> bool {
        matches!(self, Self::Degraded | Self::Critical)
    }
}

impl HealthReport {
    /// Phase-0 placeholder — always returns Healthy. Phase 14 will
    /// derive from the snapshot.
    #[must_use]
    pub fn derive(_stats: &StatsSnapshot) -> Self {
        Self {
            state: HealthState::Healthy,
            loss_rate_milli: 0,
            handshake_failure_rate_milli: 0,
        }
    }

    /// Convenience predicate.
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        matches!(self.state, HealthState::Healthy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let snap = StatsSnapshot::default();
        let report = HealthReport::derive(&snap);
        assert!(report.is_healthy());
        // TODO(phase-14): full health derivation, threshold tuning,
        // Prometheus health gauge.
    }
}

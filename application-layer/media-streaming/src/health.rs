//! [`HealthReport`], [`HealthState`] — operational health summary
//! derived from stats.
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

// ── HealthReport ───────────────────────────────────────────────────

/// Operational health snapshot derived from [`StatsSnapshot`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    /// Overall health state.
    pub state: HealthState,
    /// Number of tracks currently active.
    pub tracks_active: u64,
    /// Packet loss rate in basis points (0..=10_000).
    pub packet_loss_rate_bps: u16,
    /// Frame drops since startup.
    pub frame_drops: u64,
}

impl HealthReport {
    /// Threshold: packet loss rate ≥ this → critical (in bps).
    pub const PACKET_LOSS_CRITICAL_BPS: u16 = 2_000; // 20%
    /// Threshold: packet loss rate ≥ this → degraded (in bps).
    pub const PACKET_LOSS_DEGRADED_BPS: u16 = 500; // 5%
    /// Threshold: frame drops ≥ this per minute → degraded.
    pub const FRAME_DROPS_DEGRADED_PER_MIN: u64 = 10;
    /// Threshold: frame drops ≥ this per minute → critical.
    pub const FRAME_DROPS_CRITICAL_PER_MIN: u64 = 60;

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a "worst signal wins" policy: if
    /// any individual signal is critical, the overall state is
    /// critical; otherwise the worst degraded signal wins.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let packet_loss_rate_bps = stats.packet_loss_rate_bps();
        let frame_drops = stats.frame_drops;

        // Compute per-signal state, then take the worst.
        let state = Self::classify(packet_loss_rate_bps, frame_drops);

        Self {
            state,
            tracks_active: stats.tracks_active,
            packet_loss_rate_bps,
            frame_drops,
        }
    }

    /// Classify health based on packet loss and frame drops.
    fn classify(packet_loss_rate_bps: u16, frame_drops: u64) -> HealthState {
        // Packet loss signal.
        let loss_state = if packet_loss_rate_bps >= Self::PACKET_LOSS_CRITICAL_BPS {
            HealthState::Critical
        } else if packet_loss_rate_bps >= Self::PACKET_LOSS_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Frame drop signal (simplification: total drops, not per-minute).
        let drop_state = if frame_drops >= Self::FRAME_DROPS_CRITICAL_PER_MIN {
            HealthState::Critical
        } else if frame_drops >= Self::FRAME_DROPS_DEGRADED_PER_MIN {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Worst signal wins.
        worst_of([loss_state, drop_state])
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

    fn stats_with_packet_loss(loss_bps: u16) -> StatsSnapshot {
        // Construct stats that yield the given loss rate.
        // loss_bps = lost * 10000 / total
        // For 1000 bps (10%): lost = 100, total = 1000
        let total = 10_000u64;
        let lost = (total * loss_bps as u64) / 10_000;
        let sent = total;
        let received = sent.saturating_sub(lost);

        StatsSnapshot {
            packets_sent: sent,
            packets_received: received,
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
    fn low_packet_loss_is_healthy() {
        // 2% loss = 200 bps < 500 degraded threshold.
        let stats = stats_with_packet_loss(200);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn medium_packet_loss_is_degraded() {
        // 8% loss = 800 bps, between degraded (500) and critical (2000).
        let stats = stats_with_packet_loss(800);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn high_packet_loss_is_critical() {
        // 25% loss = 2500 bps > 2000 critical threshold.
        let stats = stats_with_packet_loss(2500);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
        assert!(report.state.is_critical());
    }

    #[test]
    fn frame_drops_threshold_degraded() {
        let mut stats = StatsSnapshot::default();
        stats.frame_drops = 15; // ≥ 10 degraded threshold
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn frame_drops_threshold_critical() {
        let mut stats = StatsSnapshot::default();
        stats.frame_drops = 70; // ≥ 60 critical threshold
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn combined_signals_worst_wins() {
        // Healthy packet loss + degraded frame drops = degraded
        let mut stats = stats_with_packet_loss(200); // healthy
        stats.frame_drops = 15; // degraded
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);

        // Degraded packet loss + critical frame drops = critical
        let mut stats = stats_with_packet_loss(800); // degraded
        stats.frame_drops = 70; // critical
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
    fn health_report_contains_fields() {
        let mut stats = StatsSnapshot::default();
        stats.tracks_active = 42;
        stats.packets_sent = 1000;
        stats.packets_received = 950; // 5% loss = 500 bps (degraded boundary)
        stats.frame_drops = 5;

        let report = HealthReport::derive(&stats);
        assert_eq!(report.tracks_active, 42);
        assert_eq!(report.frame_drops, 5);
        assert!(report.packet_loss_rate_bps >= 400);
        assert!(report.packet_loss_rate_bps <= 600);
    }

    #[test]
    fn health_report_constants() {
        assert_eq!(HealthReport::PACKET_LOSS_DEGRADED_BPS, 500);
        assert_eq!(HealthReport::PACKET_LOSS_CRITICAL_BPS, 2000);
        assert_eq!(HealthReport::FRAME_DROPS_DEGRADED_PER_MIN, 10);
        assert_eq!(HealthReport::FRAME_DROPS_CRITICAL_PER_MIN, 60);
    }

    #[test]
    fn boundary_packet_loss_exact_degraded() {
        // Exactly 500 bps = 5% → degraded
        let stats = stats_with_packet_loss(500);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn boundary_packet_loss_exact_critical() {
        // Exactly 2000 bps = 20% → critical
        let stats = stats_with_packet_loss(2000);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn boundary_frame_drops_exact_degraded() {
        let mut stats = StatsSnapshot::default();
        stats.frame_drops = 10; // exactly degraded threshold
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn boundary_frame_drops_exact_critical() {
        let mut stats = StatsSnapshot::default();
        stats.frame_drops = 60; // exactly critical threshold
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn healthy_report_needs_no_attention() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats);
        assert!(!report.needs_attention());
        assert!(report.is_healthy());
    }
}

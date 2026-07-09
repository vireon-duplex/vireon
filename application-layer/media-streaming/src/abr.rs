//! [`AdaptiveBitrateController`] — typed skeleton for ABR control.
//!
//! This is a Phase 0 skeleton. Full implementation lands in Phase 1.

use crate::bitrate::AdaptiveBitrateConfig;
use crate::error::MediaError;
use crate::health::{HealthReport, HealthState};
use crate::stats::MediaStreamingStats;
use crate::track::MediaTrackId;

/// Adaptive bitrate controller for dynamic quality adjustment.
#[derive(Debug)]
pub struct AdaptiveBitrateController {
    /// ABR configuration.
    #[allow(dead_code)]
    cfg: AdaptiveBitrateConfig,
    /// Statistics tracker.
    #[allow(dead_code)]
    stats: MediaStreamingStats,
}

impl AdaptiveBitrateController {
    /// Construct a new ABR controller.
    #[must_use]
    pub fn new(cfg: AdaptiveBitrateConfig) -> Self {
        Self {
            cfg,
            stats: MediaStreamingStats::new(),
        }
    }

    /// Evaluate current network conditions and recommend adjustment.
    ///
    /// # Errors
    /// Returns [`MediaError::ShuttingDown`] if the controller is stopped.
    pub fn evaluate(&mut self, _rtt_ns: u64, _loss_bps: u16) -> Result<(), MediaError> {
        Err(MediaError::ShuttingDown)
    }

    /// Adjust bitrate for a specific track.
    ///
    /// # Errors
    /// Returns [`MediaError::ShuttingDown`] if the controller is stopped.
    pub fn adjust(&mut self, _track: MediaTrackId, _new_bitrate: u32) -> Result<(), MediaError> {
        Err(MediaError::ShuttingDown)
    }

    /// Select the best simulcast layer for current bandwidth.
    ///
    /// # Errors
    /// Returns [`MediaError::ShuttingDown`] if the controller is stopped.
    pub fn select_layer(
        &mut self,
        _track: MediaTrackId,
        _bandwidth: u32,
    ) -> Result<Option<u8>, MediaError> {
        Err(MediaError::ShuttingDown)
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport {
            state: HealthState::Healthy,
            tracks_active: 0,
            packet_loss_rate_bps: 0,
            frame_drops: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_controller_has_config() {
        let cfg = AdaptiveBitrateConfig::video_conference();
        let controller = AdaptiveBitrateController::new(cfg);
        assert_eq!(controller.cfg.min_bps, 300_000);
        assert_eq!(controller.cfg.max_bps, 5_000_000);
    }

    #[test]
    fn evaluate_returns_shutting_down() {
        let mut controller =
            AdaptiveBitrateController::new(AdaptiveBitrateConfig::video_conference());
        let res = controller.evaluate(50_000_000, 500);
        assert!(matches!(res, Err(MediaError::ShuttingDown)));
    }

    #[test]
    fn adjust_returns_shutting_down() {
        let mut controller =
            AdaptiveBitrateController::new(AdaptiveBitrateConfig::video_conference());
        let track = MediaTrackId::new(1);
        let res = controller.adjust(track, 2_000_000);
        assert!(matches!(res, Err(MediaError::ShuttingDown)));
    }

    #[test]
    fn select_layer_returns_shutting_down() {
        let mut controller =
            AdaptiveBitrateController::new(AdaptiveBitrateConfig::video_conference());
        let track = MediaTrackId::new(1);
        let res = controller.select_layer(track, 3_000_000);
        assert!(matches!(res, Err(MediaError::ShuttingDown)));
    }

    #[test]
    fn health_returns_healthy_report() {
        let controller = AdaptiveBitrateController::new(AdaptiveBitrateConfig::video_conference());
        let report = controller.health();
        assert_eq!(report.state, HealthState::Healthy);
        assert!(report.is_healthy());
    }
}

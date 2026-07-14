//! [`AdaptiveBitrateController`] — adaptive bitrate control with network
//! quality evaluation, per-track bitrate state, and layer selection.
//!
//! Uses a [`BitrateLadder`] to map available bandwidth to simulcast layers.
//! Evaluates RTT and packet loss to classify network health and recommend
//! bitrate adjustments.
//!
//! ```text
//! adjust(track=1, bitrate=2M)           → track 1 current bitrate = 2M
//! select_layer(track=1, bandwidth=3M)   → highest layer ≤ 3M
//! evaluate(rtt=50ms, loss=200bps)       → updates health state
//! ```

use crate::bitrate::{AdaptiveBitrateConfig, BitrateLadder};
use crate::error::MediaError;
use crate::health::{HealthReport, HealthState};
use crate::stats::MediaStreamingStats;
use crate::track::MediaTrackId;
use rustc_hash::FxHashMap;

/// Per-track ABR state.
struct TrackAbrState {
    current_bitrate: u32,
    ladder: BitrateLadder,
}

/// Adaptive bitrate controller.
///
/// Tracks per-track bitrate, evaluates network quality from RTT/loss
/// metrics, and selects the best simulcast layer for available bandwidth.
pub struct AdaptiveBitrateController {
    cfg: AdaptiveBitrateConfig,
    stats: MediaStreamingStats,
    tracks: FxHashMap<MediaTrackId, TrackAbrState>,
    /// Latest measured RTT (nanoseconds).
    last_rtt_ns: u64,
    /// Latest measured packet loss (basis points, 0..=10000).
    last_loss_bps: u16,
}

impl std::fmt::Debug for AdaptiveBitrateController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdaptiveBitrateController")
            .field("cfg", &self.cfg)
            .field("tracked_tracks", &self.tracks.len())
            .field("last_rtt_ns", &self.last_rtt_ns)
            .field("last_loss_bps", &self.last_loss_bps)
            .finish()
    }
}

/// Health thresholds (basis points).
const LOSS_DEGRADED_BPS: u16 = 500; // 5%
const LOSS_CRITICAL_BPS: u16 = 2000; // 20%
/// RTT threshold for degraded state (200ms in nanoseconds).
const RTT_DEGRADED_NS: u64 = 200_000_000;

impl AdaptiveBitrateController {
    /// Construct a new ABR controller.
    #[must_use]
    pub fn new(cfg: AdaptiveBitrateConfig) -> Self {
        Self {
            cfg,
            stats: MediaStreamingStats::new(),
            tracks: FxHashMap::default(),
            last_rtt_ns: 0,
            last_loss_bps: 0,
        }
    }

    /// Evaluate current network conditions and update internal health state.
    ///
    /// Records RTT and loss metrics. The health report returned by
    /// [`health()`](Self::health) reflects the latest evaluation.
    ///
    /// # Errors
    /// Currently never errors.
    pub fn evaluate(&mut self, rtt_ns: u64, loss_bps: u16) -> Result<(), MediaError> {
        self.last_rtt_ns = rtt_ns;
        self.last_loss_bps = loss_bps;
        Ok(())
    }

    /// Sets the current bitrate for a track and optionally registers a
    /// bitrate ladder for layer selection.
    ///
    /// If the track is new, a default empty ladder is created. The
    /// bitrate is clamped to `[cfg.min_bps, cfg.max_bps]`.
    ///
    /// # Errors
    /// Returns [`MediaError::BitrateUnavailable`] if `new_bitrate` is zero.
    pub fn adjust(&mut self, track: MediaTrackId, new_bitrate: u32) -> Result<(), MediaError> {
        if new_bitrate == 0 {
            return Err(MediaError::BitrateUnavailable);
        }
        let clamped = new_bitrate.clamp(self.cfg.min_bps, self.cfg.max_bps);
        let state = self
            .tracks
            .entry(track)
            .or_insert_with(|| TrackAbrState {
                current_bitrate: self.cfg.start_bps,
                ladder: BitrateLadder::new(),
            });
        state.current_bitrate = clamped;
        Ok(())
    }

    /// Selects the best simulcast layer for a track given available bandwidth.
    ///
    /// Uses the track's registered [`BitrateLadder`] to find the highest
    /// layer whose bitrate does not exceed `bandwidth`.
    ///
    /// # Errors
    /// Returns [`MediaError::TrackNotFound`] if the track was never
    /// registered via [`adjust`](Self::adjust).
    pub fn select_layer(
        &mut self,
        track: MediaTrackId,
        bandwidth: u32,
    ) -> Result<Option<u8>, MediaError> {
        let state = self
            .tracks
            .get(&track)
            .ok_or(MediaError::TrackNotFound(track))?;
        Ok(state.ladder.best_for_bandwidth(bandwidth))
    }

    /// Registers a bitrate ladder for a track (for layer selection).
    pub fn set_ladder(&mut self, track: MediaTrackId, ladder: BitrateLadder) {
        let state = self
            .tracks
            .entry(track)
            .or_insert_with(|| TrackAbrState {
                current_bitrate: self.cfg.start_bps,
                ladder: BitrateLadder::new(),
            });
        state.ladder = ladder;
    }

    /// Returns the current bitrate for a track.
    #[must_use]
    pub fn current_bitrate(&self, track: MediaTrackId) -> Option<u32> {
        self.tracks.get(&track).map(|s| s.current_bitrate)
    }

    /// Returns the number of tracked tracks.
    #[must_use]
    pub fn track_count(&self) -> usize {
        self.tracks.len()
    }

    /// Computes the network health state from the latest RTT and loss.
    fn network_state(&self) -> HealthState {
        if self.last_loss_bps >= LOSS_CRITICAL_BPS {
            return HealthState::Critical;
        }
        if self.last_loss_bps >= LOSS_DEGRADED_BPS || self.last_rtt_ns >= RTT_DEGRADED_NS {
            return HealthState::Degraded;
        }
        HealthState::Healthy
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport {
            state: self.network_state(),
            tracks_active: self.tracks.len() as u64,
            packet_loss_rate_bps: self.last_loss_bps,
            frame_drops: 0,
        }
    }

    /// Returns a reference to the config.
    #[must_use]
    pub const fn cfg(&self) -> &AdaptiveBitrateConfig {
        &self.cfg
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_controller_has_config() {
        let cfg = AdaptiveBitrateConfig::video_conference();
        let controller = AdaptiveBitrateController::new(cfg);
        assert_eq!(controller.cfg().min_bps, 300_000);
        assert_eq!(controller.cfg().max_bps, 5_000_000);
    }

    #[test]
    fn adjust_sets_bitrate() {
        let mut c = AdaptiveBitrateController::new(AdaptiveBitrateConfig::video_conference());
        c.adjust(MediaTrackId::new(1), 2_000_000).unwrap();
        assert_eq!(c.current_bitrate(MediaTrackId::new(1)), Some(2_000_000));
    }

    #[test]
    fn adjust_clamps_to_range() {
        let mut c = AdaptiveBitrateController::new(AdaptiveBitrateConfig::video_conference());
        // Below min → clamped to min.
        c.adjust(MediaTrackId::new(1), 100).unwrap();
        assert_eq!(c.current_bitrate(MediaTrackId::new(1)), Some(300_000));
        // Above max → clamped to max.
        c.adjust(MediaTrackId::new(1), 99_999_999).unwrap();
        assert_eq!(c.current_bitrate(MediaTrackId::new(1)), Some(5_000_000));
    }

    #[test]
    fn adjust_zero_bitrate_errors() {
        let mut c = AdaptiveBitrateController::new(AdaptiveBitrateConfig::video_conference());
        assert!(c.adjust(MediaTrackId::new(1), 0).is_err());
    }

    #[test]
    fn evaluate_updates_health() {
        let mut c = AdaptiveBitrateController::new(AdaptiveBitrateConfig::video_conference());
        c.evaluate(50_000_000, 100).unwrap(); // 50ms RTT, 1% loss
        assert_eq!(c.health().state, HealthState::Healthy);

        c.evaluate(250_000_000, 600).unwrap(); // 250ms RTT, 6% loss
        assert_eq!(c.health().state, HealthState::Degraded);

        c.evaluate(500_000_000, 2500).unwrap(); // 500ms RTT, 25% loss
        assert_eq!(c.health().state, HealthState::Critical);
    }

    #[test]
    fn select_layer_uses_ladder() {
        let mut c = AdaptiveBitrateController::new(AdaptiveBitrateConfig::video_conference());
        let track = MediaTrackId::new(1);

        let mut ladder = BitrateLadder::new();
        ladder.add_layer(0, 3_000_000).unwrap();
        ladder.add_layer(1, 1_500_000).unwrap();
        ladder.add_layer(2, 500_000).unwrap();
        c.set_ladder(track, ladder);

        // High bandwidth → highest layer.
        assert_eq!(c.select_layer(track, 5_000_000).unwrap(), Some(0));
        // Medium → middle.
        assert_eq!(c.select_layer(track, 2_000_000).unwrap(), Some(1));
        // Low → lowest.
        assert_eq!(c.select_layer(track, 600_000).unwrap(), Some(2));
        // Very low → none.
        assert!(c.select_layer(track, 100_000).unwrap().is_none());
    }

    #[test]
    fn select_layer_unknown_track_errors() {
        let mut c = AdaptiveBitrateController::new(AdaptiveBitrateConfig::video_conference());
        assert!(c.select_layer(MediaTrackId::new(99), 1_000_000).is_err());
    }

    #[test]
    fn health_tracks_active_count() {
        let mut c = AdaptiveBitrateController::new(AdaptiveBitrateConfig::video_conference());
        c.adjust(MediaTrackId::new(1), 1_000_000).unwrap();
        c.adjust(MediaTrackId::new(2), 2_000_000).unwrap();
        assert_eq!(c.health().tracks_active, 2);
    }

    #[test]
    fn health_returns_healthy_initially() {
        let c = AdaptiveBitrateController::new(AdaptiveBitrateConfig::video_conference());
        assert!(c.health().is_healthy());
    }
}

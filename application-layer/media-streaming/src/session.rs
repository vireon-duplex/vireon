//! [`MediaSession`] — typed skeleton for session management.
//!
//! This is a Phase 0 skeleton. Full implementation lands in Phase 1.

use crate::config::MediaStreamingConfig;
use crate::error::MediaError;
use crate::health::{HealthReport, HealthState};
use crate::stats::MediaStreamingStats;
use crate::track::{MediaTrackId, TrackDirection, TrackKind};

/// Media session for managing tracks within a single session.
#[derive(Debug)]
pub struct MediaSession {
    /// Session configuration.
    #[allow(dead_code)]
    cfg: MediaStreamingConfig,
    /// Statistics tracker.
    #[allow(dead_code)]
    stats: MediaStreamingStats,
}

impl MediaSession {
    /// Construct a new media session.
    #[must_use]
    pub fn new(cfg: MediaStreamingConfig) -> Self {
        Self {
            cfg,
            stats: MediaStreamingStats::new(),
        }
    }

    /// Add a track to the session.
    ///
    /// # Errors
    /// Returns [`MediaError::ShuttingDown`] if the session is closing.
    pub fn add_track(
        &mut self,
        _id: MediaTrackId,
        _kind: TrackKind,
        _direction: TrackDirection,
    ) -> Result<(), MediaError> {
        Err(MediaError::ShuttingDown)
    }

    /// Remove a track from the session.
    ///
    /// # Errors
    /// Returns [`MediaError::TrackNotFound`] if the track does not exist.
    pub fn remove_track(&mut self, id: MediaTrackId) -> Result<(), MediaError> {
        Err(MediaError::TrackNotFound(id))
    }

    /// Negotiate codec and track parameters.
    ///
    /// # Errors
    /// Returns [`MediaError::ShuttingDown`] if the session is closing.
    pub fn negotiate(&mut self) -> Result<(), MediaError> {
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
    use crate::config::Preset;

    #[test]
    fn new_session_has_config() {
        let cfg = Preset::VideoCall.to_config();
        let session = MediaSession::new(cfg);
        assert_eq!(session.cfg.default_bitrate_bps, 1_500_000);
    }

    #[test]
    fn add_track_returns_shutting_down() {
        let mut session = MediaSession::new(Preset::VideoCall.to_config());
        let id = MediaTrackId::new(1);
        let res = session.add_track(id, TrackKind::Audio, TrackDirection::SendRecv);
        assert!(matches!(res, Err(MediaError::ShuttingDown)));
    }

    #[test]
    fn remove_track_returns_not_found() {
        let mut session = MediaSession::new(Preset::VideoCall.to_config());
        let id = MediaTrackId::new(1);
        let res = session.remove_track(id);
        assert!(matches!(res, Err(MediaError::TrackNotFound(_))));
    }

    #[test]
    fn negotiate_returns_shutting_down() {
        let mut session = MediaSession::new(Preset::VideoCall.to_config());
        let res = session.negotiate();
        assert!(matches!(res, Err(MediaError::ShuttingDown)));
    }

    #[test]
    fn health_returns_healthy_report() {
        let session = MediaSession::new(Preset::VideoCall.to_config());
        let report = session.health();
        assert_eq!(report.state, HealthState::Healthy);
        assert!(report.is_healthy());
    }
}

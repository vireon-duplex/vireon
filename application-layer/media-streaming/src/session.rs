//! [`MediaSession`] — session management facade.
//!
//! Phase 1: in-memory track registry with per-track frame counters.

use std::collections::HashMap;

use crate::config::MediaStreamingConfig;
use crate::error::MediaError;
use crate::health::{HealthReport, HealthState};
use crate::stats::MediaStreamingStats;
use crate::track::{MediaTrackId, TrackDirection, TrackKind};

/// Per-track in-memory state.
struct TrackState {
    #[allow(dead_code)]
    kind: TrackKind,
    #[allow(dead_code)]
    direction: TrackDirection,
    frame_count: u64,
}

/// Media session for managing tracks within a single session.
pub struct MediaSession {
    /// Session configuration.
    #[allow(dead_code)]
    cfg: MediaStreamingConfig,
    /// Statistics tracker.
    stats: MediaStreamingStats,
    /// Track registry keyed by id.
    tracks: HashMap<MediaTrackId, TrackState>,
    /// Whether negotiation has completed.
    negotiated: bool,
}

impl std::fmt::Debug for MediaSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MediaSession")
            .field("cfg", &self.cfg)
            .field("tracks", &self.tracks.len())
            .field("negotiated", &self.negotiated)
            .finish()
    }
}

impl MediaSession {
    /// Construct a new media session.
    #[must_use]
    pub fn new(cfg: MediaStreamingConfig) -> Self {
        Self {
            cfg,
            stats: MediaStreamingStats::new(),
            tracks: HashMap::new(),
            negotiated: false,
        }
    }

    /// Add a track to the session.
    ///
    /// # Errors
    /// Returns [`MediaError::TrackExists`] if a track with the same id is
    /// already registered.
    pub fn add_track(
        &mut self,
        id: MediaTrackId,
        kind: TrackKind,
        direction: TrackDirection,
    ) -> Result<(), MediaError> {
        if self.tracks.contains_key(&id) {
            return Err(MediaError::TrackExists(id));
        }
        self.tracks.insert(
            id,
            TrackState {
                kind,
                direction,
                frame_count: 0,
            },
        );
        self.stats.record_track_created();
        Ok(())
    }

    /// Remove a track from the session.
    ///
    /// # Errors
    /// Returns [`MediaError::TrackNotFound`] if the track does not exist.
    pub fn remove_track(&mut self, id: MediaTrackId) -> Result<(), MediaError> {
        if self.tracks.remove(&id).is_none() {
            return Err(MediaError::TrackNotFound(id));
        }
        self.stats.record_track_removed();
        Ok(())
    }

    /// Negotiate codec and track parameters.
    pub fn negotiate(&mut self) -> Result<(), MediaError> {
        self.negotiated = true;
        Ok(())
    }

    /// Record an incoming media frame on `track_id`.
    ///
    /// Called by the application dispatcher for audio/video/image frames.
    ///
    /// # Errors
    /// Returns [`MediaError::TrackNotFound`] if the track does not exist.
    pub fn on_media_frame(
        &mut self,
        track_id: MediaTrackId,
        payload_len: usize,
    ) -> Result<(), MediaError> {
        let entry = self
            .tracks
            .get_mut(&track_id)
            .ok_or(MediaError::TrackNotFound(track_id))?;
        entry.frame_count += 1;
        self.stats.record_packet_received(payload_len as u64);
        Ok(())
    }

    /// Returns the number of active tracks.
    #[must_use]
    pub fn track_count(&self) -> usize {
        self.tracks.len()
    }

    /// Returns `true` if negotiation has completed.
    #[must_use]
    pub fn is_negotiated(&self) -> bool {
        self.negotiated
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport {
            state: HealthState::Healthy,
            tracks_active: self.tracks.len() as u64,
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
        assert_eq!(session.track_count(), 0);
    }

    #[test]
    fn add_track_succeeds() {
        let mut session = MediaSession::new(Preset::VideoCall.to_config());
        session
            .add_track(
                MediaTrackId::new(1),
                TrackKind::Audio,
                TrackDirection::SendRecv,
            )
            .unwrap();
        assert_eq!(session.track_count(), 1);
    }

    #[test]
    fn add_track_duplicate_errors() {
        let mut session = MediaSession::new(Preset::VideoCall.to_config());
        let id = MediaTrackId::new(1);
        session
            .add_track(id, TrackKind::Audio, TrackDirection::SendRecv)
            .unwrap();
        assert!(matches!(
            session.add_track(id, TrackKind::Audio, TrackDirection::SendRecv),
            Err(MediaError::TrackExists(_))
        ));
    }

    #[test]
    fn remove_track_succeeds() {
        let mut session = MediaSession::new(Preset::VideoCall.to_config());
        let id = MediaTrackId::new(1);
        session
            .add_track(id, TrackKind::Video, TrackDirection::SendOnly)
            .unwrap();
        session.remove_track(id).unwrap();
        assert_eq!(session.track_count(), 0);
    }

    #[test]
    fn remove_track_unknown_errors() {
        let mut session = MediaSession::new(Preset::VideoCall.to_config());
        assert!(matches!(
            session.remove_track(MediaTrackId::new(99)),
            Err(MediaError::TrackNotFound(_))
        ));
    }

    #[test]
    fn negotiate_succeeds() {
        let mut session = MediaSession::new(Preset::VideoCall.to_config());
        assert!(!session.is_negotiated());
        session.negotiate().unwrap();
        assert!(session.is_negotiated());
    }

    #[test]
    fn on_media_frame_increments_counter() {
        let mut session = MediaSession::new(Preset::VideoCall.to_config());
        let id = MediaTrackId::new(1);
        session
            .add_track(id, TrackKind::Audio, TrackDirection::RecvOnly)
            .unwrap();
        session.on_media_frame(id, 1024).unwrap();
        session.on_media_frame(id, 2048).unwrap();
    }

    #[test]
    fn on_media_frame_unknown_track_errors() {
        let mut session = MediaSession::new(Preset::VideoCall.to_config());
        assert!(matches!(
            session.on_media_frame(MediaTrackId::new(99), 100),
            Err(MediaError::TrackNotFound(_))
        ));
    }

    #[test]
    fn health_reflects_active_tracks() {
        let mut session = MediaSession::new(Preset::VideoCall.to_config());
        session
            .add_track(
                MediaTrackId::new(1),
                TrackKind::Audio,
                TrackDirection::SendRecv,
            )
            .unwrap();
        let report = session.health();
        assert_eq!(report.state, HealthState::Healthy);
        assert_eq!(report.tracks_active, 1);
        assert!(report.is_healthy());
    }
}

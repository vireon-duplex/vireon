//! [`MediaStreamingConfig`], [`Preset`] — declarative configuration for
//! the media streaming system.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).

use crate::error::MediaError;

// ── Constants ─────────────────────────────────────────────────────

/// Minimum number of tracks per session.
pub const MIN_TRACKS: u8 = 1;

/// Maximum number of tracks per session (hard cap).
pub const MAX_TRACKS: u8 = 32;

/// Minimum bitrate in bits per second.
pub const MIN_BITRATE_BPS: u32 = 8_000; // 8 kbps

/// Maximum bitrate in bits per second (hard cap; 4K60 HDR).
pub const MAX_BITRATE_BPS: u32 = 50_000_000; // 50 Mbps

/// Minimum ABR evaluation interval in milliseconds.
pub const MIN_ABR_INTERVAL_MS: u16 = 100;

/// Maximum ABR evaluation interval in milliseconds.
pub const MAX_ABR_INTERVAL_MS: u16 = 10_000;

// ── MediaStreamingConfig ───────────────────────────────────────────

/// Declarative configuration for the media streaming system.
///
/// Constructed via [`MediaStreamingConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MediaStreamingConfig {
    /// Maximum tracks per session.
    pub max_tracks_per_session: u8,
    /// Default bitrate in bits per second.
    pub default_bitrate_bps: u32,
    /// Minimum bitrate in bits per second.
    pub min_bitrate_bps: u32,
    /// Maximum bitrate in bits per second.
    pub max_bitrate_bps: u32,
    /// ABR evaluation interval in milliseconds.
    pub abr_interval_ms: u16,
    /// Whether simulcast is enabled.
    pub enable_simulcast: bool,
}

impl MediaStreamingConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: 16 tracks, 1.5 Mbps default, 500 kbps minimum,
    /// 10 Mbps maximum, 500 ms ABR interval, simulcast enabled.
    pub const fn new() -> Self {
        Self {
            max_tracks_per_session: 16,
            default_bitrate_bps: 1_500_000,
            min_bitrate_bps: 500_000,
            max_bitrate_bps: 10_000_000,
            abr_interval_ms: 500,
            enable_simulcast: true,
        }
    }

    /// Builder-style: set max tracks per session.
    #[must_use]
    pub const fn with_max_tracks(mut self, max: u8) -> Self {
        self.max_tracks_per_session = max;
        self
    }

    /// Builder-style: set default bitrate.
    #[must_use]
    pub const fn with_default_bitrate(mut self, bps: u32) -> Self {
        self.default_bitrate_bps = bps;
        self
    }

    /// Builder-style: set minimum bitrate.
    #[must_use]
    pub const fn with_min_bitrate(mut self, bps: u32) -> Self {
        self.min_bitrate_bps = bps;
        self
    }

    /// Builder-style: set maximum bitrate.
    #[must_use]
    pub const fn with_max_bitrate(mut self, bps: u32) -> Self {
        self.max_bitrate_bps = bps;
        self
    }

    /// Builder-style: set ABR interval.
    #[must_use]
    pub const fn with_abr_interval_ms(mut self, ms: u16) -> Self {
        self.abr_interval_ms = ms;
        self
    }

    /// Builder-style: enable/disable simulcast.
    #[must_use]
    pub const fn with_simulcast(mut self, enable: bool) -> Self {
        self.enable_simulcast = enable;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`MediaError::InvalidSdp`] if any field is out of range.
    pub fn validate(&self) -> Result<(), MediaError> {
        if self.max_tracks_per_session < MIN_TRACKS || self.max_tracks_per_session > MAX_TRACKS {
            return Err(MediaError::InvalidSdp(format!(
                "max_tracks_per_session {n} out of range [{MIN_TRACKS}, {MAX_TRACKS}]",
                n = self.max_tracks_per_session
            )));
        }
        if self.min_bitrate_bps < MIN_BITRATE_BPS || self.min_bitrate_bps > MAX_BITRATE_BPS {
            return Err(MediaError::InvalidSdp(format!(
                "min_bitrate_bps {n} out of range [{MIN_BITRATE_BPS}, {MAX_BITRATE_BPS}]",
                n = self.min_bitrate_bps
            )));
        }
        if self.max_bitrate_bps < MIN_BITRATE_BPS || self.max_bitrate_bps > MAX_BITRATE_BPS {
            return Err(MediaError::InvalidSdp(format!(
                "max_bitrate_bps {n} out of range [{MIN_BITRATE_BPS}, {MAX_BITRATE_BPS}]",
                n = self.max_bitrate_bps
            )));
        }
        if self.default_bitrate_bps < self.min_bitrate_bps
            || self.default_bitrate_bps > self.max_bitrate_bps
        {
            return Err(MediaError::InvalidSdp(
                "default_bitrate_bps must be between min and max".to_string(),
            ));
        }
        if self.abr_interval_ms < MIN_ABR_INTERVAL_MS || self.abr_interval_ms > MAX_ABR_INTERVAL_MS
        {
            return Err(MediaError::InvalidSdp(format!(
                "abr_interval_ms {n} out of range [{MIN_ABR_INTERVAL_MS}, {MAX_ABR_INTERVAL_MS}]",
                n = self.abr_interval_ms
            )));
        }
        Ok(())
    }
}

impl Default for MediaStreamingConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ─────────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// media profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Voice-only: low bitrate, single audio track.
    VoiceOnly,
    /// Standard video call: moderate bitrate, audio + video.
    VideoCall,
    /// Conference: higher bitrate, multiple participants.
    Conference,
    /// Screen sharing: high bitrate, variable framerate.
    ScreenShare,
    /// Broadcasting: maximum bitrate, simulcast enabled.
    Broadcasting,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    pub const fn to_config(self) -> MediaStreamingConfig {
        match self {
            Self::VoiceOnly => MediaStreamingConfig {
                max_tracks_per_session: 2,
                default_bitrate_bps: 64_000,
                min_bitrate_bps: 8_000,
                max_bitrate_bps: 128_000,
                abr_interval_ms: 1000,
                enable_simulcast: false,
            },
            Self::VideoCall => MediaStreamingConfig {
                max_tracks_per_session: 4,
                default_bitrate_bps: 1_500_000,
                min_bitrate_bps: 300_000,
                max_bitrate_bps: 5_000_000,
                abr_interval_ms: 500,
                enable_simulcast: true,
            },
            Self::Conference => MediaStreamingConfig {
                max_tracks_per_session: 16,
                default_bitrate_bps: 2_000_000,
                min_bitrate_bps: 500_000,
                max_bitrate_bps: 8_000_000,
                abr_interval_ms: 300,
                enable_simulcast: true,
            },
            Self::ScreenShare => MediaStreamingConfig {
                max_tracks_per_session: 4,
                default_bitrate_bps: 5_000_000,
                min_bitrate_bps: 1_000_000,
                max_bitrate_bps: 20_000_000,
                abr_interval_ms: 1000,
                enable_simulcast: false,
            },
            Self::Broadcasting => MediaStreamingConfig {
                max_tracks_per_session: 8,
                default_bitrate_bps: 10_000_000,
                min_bitrate_bps: 2_000_000,
                max_bitrate_bps: 50_000_000,
                abr_interval_ms: 200,
                enable_simulcast: true,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = MediaStreamingConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_tracks_per_session, 16);
        assert_eq!(cfg.default_bitrate_bps, 1_500_000);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = MediaStreamingConfig::new()
            .with_max_tracks(8)
            .with_default_bitrate(2_000_000)
            .with_min_bitrate(500_000)
            .with_max_bitrate(8_000_000)
            .with_abr_interval_ms(300)
            .with_simulcast(false);
        assert_eq!(cfg.max_tracks_per_session, 8);
        assert_eq!(cfg.default_bitrate_bps, 2_000_000);
        assert_eq!(cfg.min_bitrate_bps, 500_000);
        assert_eq!(cfg.max_bitrate_bps, 8_000_000);
        assert_eq!(cfg.abr_interval_ms, 300);
        assert!(!cfg.enable_simulcast);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_bad_track_count() {
        let mut cfg = MediaStreamingConfig::default();
        cfg.max_tracks_per_session = 0;
        assert!(cfg.validate().is_err());
        cfg.max_tracks_per_session = MAX_TRACKS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_min_bitrate() {
        let mut cfg = MediaStreamingConfig::default();
        cfg.min_bitrate_bps = MIN_BITRATE_BPS - 1;
        assert!(cfg.validate().is_err());
        cfg.min_bitrate_bps = MAX_BITRATE_BPS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_max_bitrate() {
        let mut cfg = MediaStreamingConfig::default();
        cfg.max_bitrate_bps = MIN_BITRATE_BPS - 1;
        assert!(cfg.validate().is_err());
        cfg.max_bitrate_bps = MAX_BITRATE_BPS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_default_out_of_range() {
        let mut cfg = MediaStreamingConfig::default();
        cfg.default_bitrate_bps = cfg.min_bitrate_bps - 1;
        assert!(cfg.validate().is_err());
        cfg.default_bitrate_bps = cfg.max_bitrate_bps + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_abr_interval() {
        let mut cfg = MediaStreamingConfig::default();
        cfg.abr_interval_ms = MIN_ABR_INTERVAL_MS - 1;
        assert!(cfg.validate().is_err());
        cfg.abr_interval_ms = MAX_ABR_INTERVAL_MS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn voice_only_preset_uses_low_bitrate() {
        let cfg = Preset::VoiceOnly.to_config();
        assert_eq!(cfg.max_bitrate_bps, 128_000);
        assert_eq!(cfg.max_tracks_per_session, 2);
        assert!(!cfg.enable_simulcast);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn video_call_preset_moderate_bitrate() {
        let cfg = Preset::VideoCall.to_config();
        assert_eq!(cfg.default_bitrate_bps, 1_500_000);
        assert_eq!(cfg.max_tracks_per_session, 4);
        assert!(cfg.enable_simulcast);
    }

    #[test]
    fn conference_preset_higher_capacity() {
        let cfg = Preset::Conference.to_config();
        assert_eq!(cfg.max_tracks_per_session, 16);
        assert_eq!(cfg.abr_interval_ms, 300);
    }

    #[test]
    fn screen_share_preset_high_bitrate() {
        let cfg = Preset::ScreenShare.to_config();
        assert_eq!(cfg.max_bitrate_bps, 20_000_000);
        assert_eq!(cfg.abr_interval_ms, 1000);
        assert!(!cfg.enable_simulcast);
    }

    #[test]
    fn broadcasting_preset_maximum_quality() {
        let cfg = Preset::Broadcasting.to_config();
        assert_eq!(cfg.max_bitrate_bps, 50_000_000);
        assert_eq!(cfg.abr_interval_ms, 200);
        assert!(cfg.enable_simulcast);
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [
            Preset::VoiceOnly,
            Preset::VideoCall,
            Preset::Conference,
            Preset::ScreenShare,
            Preset::Broadcasting,
        ] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn preset_overrides_preserve_validation() {
        let cfg = Preset::VideoCall
            .to_config()
            .with_max_tracks(8)
            .with_abr_interval_ms(250);
        assert_eq!(cfg.max_tracks_per_session, 8);
        assert_eq!(cfg.abr_interval_ms, 250);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn config_equality() {
        let a = MediaStreamingConfig::new();
        let b = MediaStreamingConfig::new();
        assert_eq!(a, b);

        let c = a.with_max_tracks(8);
        assert_ne!(a, c);
    }
}

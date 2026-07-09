//! [`MediaStreamingBuilder`] — fluent builder for [`MediaStreamingConfig`].
//!
//! See `DESIGN.md` Part 13 (configuration).

use crate::config::{MediaStreamingConfig, Preset};
use crate::error::MediaError;

/// Fluent builder for [`MediaStreamingConfig`].
#[derive(Debug, Clone)]
pub struct MediaStreamingBuilder {
    cfg: MediaStreamingConfig,
}

impl MediaStreamingBuilder {
    /// Start a new builder with defaults.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: MediaStreamingConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override max tracks per session.
    #[must_use]
    pub const fn max_tracks(mut self, max: u8) -> Self {
        self.cfg.max_tracks_per_session = max;
        self
    }

    /// Override default bitrate.
    #[must_use]
    pub const fn default_bitrate(mut self, bps: u32) -> Self {
        self.cfg.default_bitrate_bps = bps;
        self
    }

    /// Override minimum bitrate.
    #[must_use]
    pub const fn min_bitrate(mut self, bps: u32) -> Self {
        self.cfg.min_bitrate_bps = bps;
        self
    }

    /// Override maximum bitrate.
    #[must_use]
    pub const fn max_bitrate(mut self, bps: u32) -> Self {
        self.cfg.max_bitrate_bps = bps;
        self
    }

    /// Override ABR interval.
    #[must_use]
    pub const fn abr_interval_ms(mut self, ms: u16) -> Self {
        self.cfg.abr_interval_ms = ms;
        self
    }

    /// Enable or disable simulcast.
    #[must_use]
    pub const fn simulcast(mut self, enable: bool) -> Self {
        self.cfg.enable_simulcast = enable;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`MediaError::InvalidSdp`] if any field is out of range.
    pub fn build(self) -> Result<MediaStreamingConfig, MediaError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for MediaStreamingBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_new_uses_defaults() {
        let cfg = MediaStreamingBuilder::new().build().unwrap();
        assert_eq!(cfg, MediaStreamingConfig::new());
    }

    #[test]
    fn builder_from_preset() {
        let cfg = MediaStreamingBuilder::from_preset(Preset::VoiceOnly)
            .build()
            .unwrap();
        assert_eq!(cfg.max_tracks_per_session, 2);
        assert_eq!(cfg.max_bitrate_bps, 128_000);
        assert!(!cfg.enable_simulcast);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = MediaStreamingBuilder::new()
            .max_tracks(8)
            .default_bitrate(2_000_000)
            .min_bitrate(500_000)
            .max_bitrate(8_000_000)
            .abr_interval_ms(300)
            .simulcast(false)
            .build()
            .unwrap();
        assert_eq!(cfg.max_tracks_per_session, 8);
        assert_eq!(cfg.default_bitrate_bps, 2_000_000);
        assert_eq!(cfg.min_bitrate_bps, 500_000);
        assert_eq!(cfg.max_bitrate_bps, 8_000_000);
        assert_eq!(cfg.abr_interval_ms, 300);
        assert!(!cfg.enable_simulcast);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = MediaStreamingBuilder::new()
            .min_bitrate(0) // too small
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = MediaStreamingBuilder::new()
            .min_bitrate(crate::config::MIN_BITRATE_BPS)
            .max_bitrate(crate::config::MAX_BITRATE_BPS)
            .build()
            .unwrap();
        assert_eq!(cfg.min_bitrate_bps, crate::config::MIN_BITRATE_BPS);
        assert_eq!(cfg.max_bitrate_bps, crate::config::MAX_BITRATE_BPS);
    }

    #[test]
    fn builder_from_preset_preserves_overrides() {
        let cfg = MediaStreamingBuilder::from_preset(Preset::VideoCall)
            .max_tracks(8) // override
            .build()
            .unwrap();
        assert_eq!(cfg.max_tracks_per_session, 8);
        assert_eq!(cfg.default_bitrate_bps, 1_500_000); // from preset
    }

    #[test]
    fn builder_default() {
        let builder = MediaStreamingBuilder::default();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg, MediaStreamingConfig::new());
    }

    #[test]
    fn builder_clone() {
        let a = MediaStreamingBuilder::new().max_tracks(8);
        let b = a.clone();
        let cfg = b.build().unwrap();
        assert_eq!(cfg.max_tracks_per_session, 8);
    }

    #[test]
    fn builder_rejects_invalid_bitrate_range() {
        // min > default
        let res = MediaStreamingBuilder::new()
            .min_bitrate(2_000_000)
            .default_bitrate(1_000_000)
            .build();
        assert!(res.is_err());

        // default > max
        let res = MediaStreamingBuilder::new()
            .default_bitrate(10_000_000)
            .max_bitrate(5_000_000)
            .build();
        assert!(res.is_err());
    }
}

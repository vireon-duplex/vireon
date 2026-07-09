//! [`CodecType`], [`CodecConfig`] — codec enumeration and configuration
//! for media streaming.
//!
//! See `DESIGN.md` Part 5 (codec negotiation).

// ── CodecType ─────────────────────────────────────────────────────

/// Supported codec types for media streaming.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CodecType {
    /// Opus audio codec (48 kHz, stereo/mono).
    #[default]
    Opus = 0,
    /// H.264 video codec.
    H264 = 1,
    /// H.265/HEVC video codec.
    H265 = 2,
    /// VP8 video codec.
    VP8 = 3,
    /// VP9 video codec.
    VP9 = 4,
    /// AV1 video codec.
    AV1 = 5,
    /// PCMU audio codec (8 kHz, mono).
    PCMU = 6,
    /// PCMA audio codec (8 kHz, mono).
    PCMA = 7,
}

impl CodecType {
    /// Human-readable name for logging/metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Opus => "opus",
            Self::H264 => "h264",
            Self::H265 => "h265",
            Self::VP8 => "vp8",
            Self::VP9 => "vp9",
            Self::AV1 => "av1",
            Self::PCMU => "pcmu",
            Self::PCMA => "pcma",
        }
    }

    /// Whether this is a video codec.
    #[must_use]
    pub const fn is_video(self) -> bool {
        matches!(
            self,
            Self::H264 | Self::H265 | Self::VP8 | Self::VP9 | Self::AV1
        )
    }

    /// Whether this is an audio codec.
    #[must_use]
    pub const fn is_audio(self) -> bool {
        matches!(self, Self::Opus | Self::PCMU | Self::PCMA)
    }

    /// Whether this codec supports RTCP feedback messages.
    #[must_use]
    pub const fn rtcp_feedback_supported(self) -> bool {
        matches!(
            self,
            Self::H264 | Self::H265 | Self::VP8 | Self::VP9 | Self::AV1
        )
    }
}

// ── CodecConfig ───────────────────────────────────────────────────

/// Configuration for a specific codec instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CodecConfig {
    /// RTP payload type.
    pub payload_type: u8,
    /// Clock rate in Hz.
    pub clock_rate: u32,
    /// Number of audio channels (0 for video).
    pub channels: u8,
}

impl CodecConfig {
    /// Construct a new codec config.
    ///
    /// # Panics
    /// Panics in debug mode if payload_type > 127 (RTP payload types are 7-bit).
    #[must_use]
    pub const fn new(payload_type: u8, clock_rate: u32) -> Self {
        debug_assert!(payload_type <= 127, "RTP payload type must be ≤ 127");
        Self {
            payload_type,
            clock_rate,
            channels: 0,
        }
    }

    /// Construct an audio codec config.
    #[must_use]
    pub const fn for_audio(payload_type: u8, clock_rate: u32, channels: u8) -> Self {
        debug_assert!(payload_type <= 127, "RTP payload type must be ≤ 127");
        debug_assert!(
            channels >= 1 && channels <= 2,
            "Audio channels must be 1 or 2"
        );
        Self {
            payload_type,
            clock_rate,
            channels,
        }
    }

    /// Construct a video codec config.
    #[must_use]
    pub const fn for_video(payload_type: u8, clock_rate: u32) -> Self {
        debug_assert!(payload_type <= 127, "RTP payload type must be ≤ 127");
        Self {
            payload_type,
            clock_rate,
            channels: 0,
        }
    }

    /// Whether this is an audio codec config.
    #[must_use]
    pub const fn is_audio(self) -> bool {
        self.channels > 0
    }

    /// Whether this is a video codec config.
    #[must_use]
    pub const fn is_video(self) -> bool {
        self.channels == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_type_as_str() {
        assert_eq!(CodecType::Opus.as_str(), "opus");
        assert_eq!(CodecType::H264.as_str(), "h264");
        assert_eq!(CodecType::H265.as_str(), "h265");
        assert_eq!(CodecType::VP8.as_str(), "vp8");
        assert_eq!(CodecType::VP9.as_str(), "vp9");
        assert_eq!(CodecType::AV1.as_str(), "av1");
        assert_eq!(CodecType::PCMU.as_str(), "pcmu");
        assert_eq!(CodecType::PCMA.as_str(), "pcma");
    }

    #[test]
    fn codec_type_is_video() {
        assert!(CodecType::H264.is_video());
        assert!(CodecType::H265.is_video());
        assert!(CodecType::VP8.is_video());
        assert!(CodecType::VP9.is_video());
        assert!(CodecType::AV1.is_video());
        assert!(!CodecType::Opus.is_video());
        assert!(!CodecType::PCMU.is_video());
        assert!(!CodecType::PCMA.is_video());
    }

    #[test]
    fn codec_type_is_audio() {
        assert!(CodecType::Opus.is_audio());
        assert!(CodecType::PCMU.is_audio());
        assert!(CodecType::PCMA.is_audio());
        assert!(!CodecType::H264.is_audio());
        assert!(!CodecType::VP9.is_audio());
    }

    #[test]
    fn codec_type_rtcp_feedback_supported() {
        // Video codecs support RTCP feedback.
        assert!(CodecType::H264.rtcp_feedback_supported());
        assert!(CodecType::H265.rtcp_feedback_supported());
        assert!(CodecType::VP8.rtcp_feedback_supported());
        assert!(CodecType::VP9.rtcp_feedback_supported());
        assert!(CodecType::AV1.rtcp_feedback_supported());
        // Audio codecs do not.
        assert!(!CodecType::Opus.rtcp_feedback_supported());
        assert!(!CodecType::PCMU.rtcp_feedback_supported());
        assert!(!CodecType::PCMA.rtcp_feedback_supported());
    }

    #[test]
    fn codec_type_default() {
        assert_eq!(CodecType::default(), CodecType::Opus);
    }

    #[test]
    fn codec_type_repr() {
        assert_eq!(CodecType::Opus as u8, 0);
        assert_eq!(CodecType::H264 as u8, 1);
        assert_eq!(CodecType::H265 as u8, 2);
        assert_eq!(CodecType::VP8 as u8, 3);
        assert_eq!(CodecType::VP9 as u8, 4);
        assert_eq!(CodecType::AV1 as u8, 5);
        assert_eq!(CodecType::PCMU as u8, 6);
        assert_eq!(CodecType::PCMA as u8, 7);
    }

    #[test]
    fn codec_config_new() {
        let cfg = CodecConfig::new(96, 90_000);
        assert_eq!(cfg.payload_type, 96);
        assert_eq!(cfg.clock_rate, 90_000);
        assert_eq!(cfg.channels, 0);
    }

    #[test]
    fn codec_config_for_audio() {
        let cfg = CodecConfig::for_audio(111, 48_000, 2);
        assert_eq!(cfg.payload_type, 111);
        assert_eq!(cfg.clock_rate, 48_000);
        assert_eq!(cfg.channels, 2);
        assert!(cfg.is_audio());
        assert!(!cfg.is_video());
    }

    #[test]
    fn codec_config_for_video() {
        let cfg = CodecConfig::for_video(96, 90_000);
        assert_eq!(cfg.payload_type, 96);
        assert_eq!(cfg.clock_rate, 90_000);
        assert_eq!(cfg.channels, 0);
        assert!(cfg.is_video());
        assert!(!cfg.is_audio());
    }

    #[test]
    fn codec_config_equality() {
        let a = CodecConfig::new(96, 90_000);
        let b = CodecConfig::new(96, 90_000);
        assert_eq!(a, b);

        let c = CodecConfig::new(97, 90_000);
        assert_ne!(a, c);
    }

    #[test]
    fn codec_config_copy() {
        let a = CodecConfig::for_audio(111, 48_000, 2);
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn codec_config_clone() {
        let a = CodecConfig::for_video(96, 90_000);
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn codec_config_debug() {
        let cfg = CodecConfig::for_audio(111, 48_000, 2);
        let s = format!("{cfg:?}");
        assert!(s.contains("111"));
        assert!(s.contains("48000"));
        assert!(s.contains("2"));
    }

    #[test]
    fn codec_type_hashable() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(CodecType::Opus);
        set.insert(CodecType::H264);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn codec_type_equality() {
        assert_eq!(CodecType::Opus, CodecType::Opus);
        assert_ne!(CodecType::Opus, CodecType::H264);
    }

    #[test]
    fn codec_config_with_mono_audio() {
        let cfg = CodecConfig::for_audio(111, 48_000, 1);
        assert_eq!(cfg.channels, 1);
        assert!(cfg.is_audio());
    }

    #[test]
    fn codec_config_with_stereo_audio() {
        let cfg = CodecConfig::for_audio(111, 48_000, 2);
        assert_eq!(cfg.channels, 2);
        assert!(cfg.is_audio());
    }
}

impl std::fmt::Display for CodecType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

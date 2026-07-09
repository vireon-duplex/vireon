//! [`MediaError`] — the typed failure surface of the media streaming system.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12 (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`MediaError::is_fatal`] / [`MediaError::is_retryable`] /
//! [`MediaError::is_capacity`] predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `InvalidSdp`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `BufferUnderrun`, `BitrateUnavailable` | Back off + retry |
//! | Capacity | `BandwidthExceeded`, `SessionClosed` | Shed load / wait |

use crate::codec::CodecType;
use crate::track::MediaTrackId;
use thiserror::Error;

// ── MediaError ─────────────────────────────────────────────────────

/// Typed failure surface for the media streaming system.
///
/// Every fallible streaming API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum MediaError {
    /// A track with the given id was not found.
    #[error("track not found: {0}")]
    TrackNotFound(MediaTrackId),

    /// A track with the given id already exists.
    #[error("track already exists: {0}")]
    TrackExists(MediaTrackId),

    /// The requested codec is not supported.
    #[error("codec unsupported: {0}")]
    CodecUnsupported(CodecType),

    /// Bitrate information is currently unavailable.
    #[error("bitrate unavailable")]
    BitrateUnavailable,

    /// Media buffer underrun (no data to send).
    #[error("buffer underrun")]
    BufferUnderrun,

    /// The session is closed and cannot accept new operations.
    #[error("session closed")]
    SessionClosed,

    /// Available bandwidth is insufficient for requested quality.
    #[error("bandwidth exceeded")]
    BandwidthExceeded,

    /// Invalid SDP offer/answer or configuration.
    #[error("invalid SDP or config: {0}")]
    InvalidSdp(String),

    /// System is shutting down; reject new operations.
    #[error("media streaming is shutting down")]
    ShuttingDown,
}

impl MediaError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`InvalidSdp`)
    /// or terminal lifecycle states (`ShuttingDown`) that no amount
    /// of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::InvalidSdp(_) | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (buffer underrun,
    /// bitrate temporarily unavailable).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::BufferUnderrun | Self::BitrateUnavailable)
    }

    /// Whether the error reflects a capacity condition.
    ///
    /// Capacity errors signal load-shedding: callers should either
    /// reject new work or wait for bandwidth to become available.
    #[must_use]
    pub const fn is_capacity(&self) -> bool {
        matches!(self, Self::BandwidthExceeded | Self::SessionClosed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(MediaError::InvalidSdp("bad format".to_string()).is_fatal());
        assert!(MediaError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!MediaError::TrackNotFound(MediaTrackId::new(1)).is_fatal());
        assert!(!MediaError::BandwidthExceeded.is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(MediaError::BufferUnderrun.is_retryable());
        assert!(MediaError::BitrateUnavailable.is_retryable());
        // Non-retryable examples.
        assert!(!MediaError::InvalidSdp("bad".to_string()).is_retryable());
        assert!(!MediaError::ShuttingDown.is_retryable());
    }

    #[test]
    fn capacity_classification() {
        assert!(MediaError::BandwidthExceeded.is_capacity());
        assert!(MediaError::SessionClosed.is_capacity());
        // Non-capacity examples.
        assert!(!MediaError::ShuttingDown.is_capacity());
        assert!(!MediaError::TrackExists(MediaTrackId::new(1)).is_capacity());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = MediaError::TrackNotFound(MediaTrackId::new(42));
        let s = format!("{e}");
        assert!(s.contains("track not found"));
        assert!(s.contains("42"));

        let e = MediaError::InvalidSdp("missing m-line".to_string());
        assert!(format!("{e}").contains("missing m-line"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = MediaError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());
        assert!(!fatal.is_capacity());

        let retry = MediaError::BufferUnderrun;
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
        assert!(!retry.is_capacity());

        let cap = MediaError::BandwidthExceeded;
        assert!(!cap.is_fatal());
        assert!(!cap.is_retryable());
        assert!(cap.is_capacity());
    }

    #[test]
    fn invalid_sdp_with_string() {
        let e = MediaError::InvalidSdp("test error".to_string());
        assert!(e.is_fatal());
        assert!(format!("{e}").contains("test error"));
    }

    #[test]
    fn codec_type_equality() {
        assert_eq!(CodecType::Opus, CodecType::Opus);
        assert_ne!(CodecType::H264, CodecType::H265);
    }

    #[test]
    fn codec_type_hashable() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(CodecType::Opus);
        set.insert(CodecType::H264);
        assert_eq!(set.len(), 2);
        assert!(set.contains(&CodecType::Opus));
    }

    #[test]
    fn codec_unsupported_error_formatting() {
        let e = MediaError::CodecUnsupported(CodecType::VP9);
        assert!(format!("{e}").contains("vp9"));
        assert!(format!("{e}").contains("codec unsupported"));
    }

    #[test]
    fn track_not_found_error_formatting() {
        let id = MediaTrackId::new(123);
        let e = MediaError::TrackNotFound(id);
        assert!(format!("{e}").contains("123"));
    }

    #[test]
    fn track_exists_error_formatting() {
        let id = MediaTrackId::new(456);
        let e = MediaError::TrackExists(id);
        assert!(format!("{e}").contains("456"));
        assert!(format!("{e}").contains("already exists"));
    }
}

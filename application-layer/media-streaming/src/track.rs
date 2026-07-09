//! [`MediaTrackId`], [`TrackKind`], [`TrackDirection`] — track identifiers
//! and metadata for media streaming.
//!
//! See `DESIGN.md` Part 4 (track model).

// ── MediaTrackId ───────────────────────────────────────────────────

/// Unique identifier for a media track within a session.
///
/// Wraps a u64 for type safety and provides NIL sentinel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct MediaTrackId(u64);

impl MediaTrackId {
    /// NIL sentinel value (0).
    pub const NIL: Self = Self(0);

    /// Construct a new track ID from a raw u64.
    ///
    /// Returns NIL if the raw value is 0.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Construct a NIL track ID.
    #[must_use]
    pub const fn nil() -> Self {
        Self(0)
    }

    /// Whether this track ID is NIL.
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.0 == 0
    }

    /// Get the raw u64 value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl Default for MediaTrackId {
    fn default() -> Self {
        Self::NIL
    }
}

// ── TrackKind ─────────────────────────────────────────────────────

/// The kind of media a track represents.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TrackKind {
    /// Audio track (Opus, PCMU, PCMA, etc.).
    #[default]
    Audio = 0,
    /// Video track (H264, VP8, VP9, AV1, etc.).
    Video = 1,
    /// Screen share track (typically H264 or VP9).
    Screen = 2,
    /// Data track (application-specific).
    Data = 3,
}

impl TrackKind {
    /// Human-readable name for logging/metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Audio => "audio",
            Self::Video => "video",
            Self::Screen => "screen",
            Self::Data => "data",
        }
    }

    /// Whether this is a media track (audio/video/screen).
    #[must_use]
    pub const fn is_media(self) -> bool {
        matches!(self, Self::Audio | Self::Video | Self::Screen)
    }
}

// ── TrackDirection ────────────────────────────────────────────────

/// The direction a track can send/receive media.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TrackDirection {
    /// Bidirectional: send and receive.
    #[default]
    SendRecv = 0,
    /// Send only.
    SendOnly = 1,
    /// Receive only.
    RecvOnly = 2,
    /// Inactive: neither send nor receive.
    Inactive = 3,
}

impl TrackDirection {
    /// Whether this direction sends media.
    #[must_use]
    pub const fn is_sending(self) -> bool {
        matches!(self, Self::SendRecv | Self::SendOnly)
    }

    /// Whether this direction receives media.
    #[must_use]
    pub const fn is_receiving(self) -> bool {
        matches!(self, Self::SendRecv | Self::RecvOnly)
    }

    /// Human-readable name for logging/metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SendRecv => "sendrecv",
            Self::SendOnly => "sendonly",
            Self::RecvOnly => "recvonly",
            Self::Inactive => "inactive",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_id_new_works() {
        let id = MediaTrackId::new(42);
        assert_eq!(id.raw(), 42);
        assert!(!id.is_nil());
    }

    #[test]
    fn track_id_nil_sentinel() {
        let id = MediaTrackId::new(0);
        assert!(id.is_nil());
        assert_eq!(id, MediaTrackId::NIL);
        assert_eq!(id, MediaTrackId::nil());
    }

    #[test]
    fn track_id_default_is_nil() {
        let id = MediaTrackId::default();
        assert!(id.is_nil());
    }

    #[test]
    fn track_id_ordering() {
        let a = MediaTrackId::new(1);
        let b = MediaTrackId::new(2);
        assert!(a < b);
        assert!(b > a);
    }

    #[test]
    fn track_id_equality() {
        let a = MediaTrackId::new(100);
        let b = MediaTrackId::new(100);
        assert_eq!(a, b);
    }

    #[test]
    fn track_id_hashable() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(MediaTrackId::new(1));
        set.insert(MediaTrackId::new(2));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn track_kind_as_str() {
        assert_eq!(TrackKind::Audio.as_str(), "audio");
        assert_eq!(TrackKind::Video.as_str(), "video");
        assert_eq!(TrackKind::Screen.as_str(), "screen");
        assert_eq!(TrackKind::Data.as_str(), "data");
    }

    #[test]
    fn track_kind_is_media() {
        assert!(TrackKind::Audio.is_media());
        assert!(TrackKind::Video.is_media());
        assert!(TrackKind::Screen.is_media());
        assert!(!TrackKind::Data.is_media());
    }

    #[test]
    fn track_kind_default() {
        assert_eq!(TrackKind::default(), TrackKind::Audio);
    }

    #[test]
    fn track_kind_repr() {
        assert_eq!(TrackKind::Audio as u8, 0);
        assert_eq!(TrackKind::Video as u8, 1);
        assert_eq!(TrackKind::Screen as u8, 2);
        assert_eq!(TrackKind::Data as u8, 3);
    }

    #[test]
    fn track_direction_as_str() {
        assert_eq!(TrackDirection::SendRecv.as_str(), "sendrecv");
        assert_eq!(TrackDirection::SendOnly.as_str(), "sendonly");
        assert_eq!(TrackDirection::RecvOnly.as_str(), "recvonly");
        assert_eq!(TrackDirection::Inactive.as_str(), "inactive");
    }

    #[test]
    fn track_direction_is_sending() {
        assert!(TrackDirection::SendRecv.is_sending());
        assert!(TrackDirection::SendOnly.is_sending());
        assert!(!TrackDirection::RecvOnly.is_sending());
        assert!(!TrackDirection::Inactive.is_sending());
    }

    #[test]
    fn track_direction_is_receiving() {
        assert!(TrackDirection::SendRecv.is_receiving());
        assert!(!TrackDirection::SendOnly.is_receiving());
        assert!(TrackDirection::RecvOnly.is_receiving());
        assert!(!TrackDirection::Inactive.is_receiving());
    }

    #[test]
    fn track_direction_default() {
        assert_eq!(TrackDirection::default(), TrackDirection::SendRecv);
    }

    #[test]
    fn track_direction_repr() {
        assert_eq!(TrackDirection::SendRecv as u8, 0);
        assert_eq!(TrackDirection::SendOnly as u8, 1);
        assert_eq!(TrackDirection::RecvOnly as u8, 2);
        assert_eq!(TrackDirection::Inactive as u8, 3);
    }

    #[test]
    fn track_direction_hashable() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TrackDirection::SendOnly);
        set.insert(TrackDirection::RecvOnly);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn track_kind_hashable() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TrackKind::Audio);
        set.insert(TrackKind::Video);
        assert_eq!(set.len(), 2);
    }
}

impl std::fmt::Display for MediaTrackId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

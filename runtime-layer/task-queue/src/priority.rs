//! Priority bands (Layer 4).
//!
//! The Task Queue uses four priority bands. The choice of four (not
//! three, not eight) is deliberate — enough granularity for meaningful
//! differentiation, few enough to avoid scheduler overhead and to keep
//! the ready queue compact.

/// Task priority band. Lower numeric value = higher priority.
///
/// See the [`DESIGN.md`] admission table for which bands are rejected
/// under backpressure.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TaskPriority {
    /// Completion callbacks, timeout fires, connection events.
    /// Always admitted, even under backpressure.
    High = 0,
    /// Default application work, stream processing.
    /// Admitted up to the hard pool cap.
    Normal = 1,
    /// Deferred packet generation, non-critical processing.
    /// Rejected at the high watermark.
    Low = 2,
    /// Cleanup, statistics, maintenance. Dropped first under load.
    /// Rejected at the low watermark.
    Background = 3,
}

impl TaskPriority {
    /// All four bands in priority order (highest first).
    pub const ALL: [Self; 4] = [Self::High, Self::Normal, Self::Low, Self::Background];

    /// Returns the band as a small index (0..4) for array indexing.
    #[inline]
    #[must_use]
    pub const fn as_index(self) -> usize {
        self as u8 as usize
    }

    /// Returns the band from an index, or `None` if out of range.
    #[inline]
    #[must_use]
    pub const fn from_index(idx: usize) -> Option<Self> {
        match idx {
            0 => Some(Self::High),
            1 => Some(Self::Normal),
            2 => Some(Self::Low),
            3 => Some(Self::Background),
            _ => None,
        }
    }

    /// Returns `true` if this band is always admitted under pressure.
    ///
    /// `High` and `Normal` are admitted up to the pool's hard cap.
    #[inline]
    #[must_use]
    pub const fn always_admitted(self) -> bool {
        matches!(self, Self::High | Self::Normal)
    }

    /// Promotes to the next-higher band, or returns `High` if already there.
    #[must_use]
    pub const fn promote(self) -> Self {
        match self {
            Self::High => Self::High,
            Self::Normal => Self::High,
            Self::Low => Self::Normal,
            Self::Background => Self::Low,
        }
    }
}

impl Default for TaskPriority {
    fn default() -> Self {
        Self::Normal
    }
}

impl std::fmt::Display for TaskPriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::High => "high",
            Self::Normal => "normal",
            Self::Low => "low",
            Self::Background => "background",
        };
        f.write_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_high_is_least() {
        assert!(TaskPriority::High < TaskPriority::Normal);
        assert!(TaskPriority::Normal < TaskPriority::Low);
        assert!(TaskPriority::Low < TaskPriority::Background);
    }

    #[test]
    fn as_index_round_trip() {
        for band in TaskPriority::ALL {
            assert_eq!(TaskPriority::from_index(band.as_index()), Some(band));
        }
        assert_eq!(TaskPriority::from_index(4), None);
    }

    #[test]
    fn promote_saturation() {
        assert_eq!(TaskPriority::High.promote(), TaskPriority::High);
        assert_eq!(TaskPriority::Normal.promote(), TaskPriority::High);
        assert_eq!(TaskPriority::Low.promote(), TaskPriority::Normal);
        assert_eq!(TaskPriority::Background.promote(), TaskPriority::Low);
    }

    #[test]
    fn always_admitted_classification() {
        assert!(TaskPriority::High.always_admitted());
        assert!(TaskPriority::Normal.always_admitted());
        assert!(!TaskPriority::Low.always_admitted());
        assert!(!TaskPriority::Background.always_admitted());
    }

    #[test]
    fn display() {
        assert_eq!(TaskPriority::High.to_string(), "high");
        assert_eq!(TaskPriority::Background.to_string(), "background");
    }
}

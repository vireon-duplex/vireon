//! [`BackpressureLevel`] — resource pressure classification (Layer 11).
//!
//! The shard monitors its resource usage (connection count, queue depth,
//! memory pressure) and computes a backpressure level. The reactor uses
//! this to decide whether to accept new connections, shed load, or
//! throttle existing streams.

/// Three-level backpressure classification.
///
/// `None` is the steady state. `Soft` signals the shard is approaching
/// capacity — new connections should be throttled. `Hard` means the
/// shard is critically loaded — new connections are rejected and
/// existing traffic may be shed.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum BackpressureLevel {
    /// No pressure — all operations proceed normally.
    #[default]
    None = 0,
    /// Approaching capacity — throttle new connections.
    Soft = 1,
    /// Critically loaded — reject new connections, shed load.
    Hard = 2,
}

impl BackpressureLevel {
    /// Whether new connections should be rejected at this level.
    #[inline]
    #[must_use]
    pub const fn rejects_new(self) -> bool {
        matches!(self, Self::Hard)
    }

    /// Whether new connections should be throttled at this level.
    #[inline]
    #[must_use]
    pub const fn throttles_new(self) -> bool {
        matches!(self, Self::Soft | Self::Hard)
    }

    /// Computes the backpressure level from a fill ratio.
    ///
    /// `ratio` is `active / capacity` as a value in `[0.0, 1.0]`.
    /// `soft_threshold` and `hard_threshold` are also in `[0.0, 1.0]`
    /// with `soft_threshold < hard_threshold`.
    #[inline]
    #[must_use]
    pub fn from_ratio(ratio: f64, soft_threshold: f64, hard_threshold: f64) -> Self {
        if ratio >= hard_threshold {
            Self::Hard
        } else if ratio >= soft_threshold {
            Self::Soft
        } else {
            Self::None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_none() {
        assert_eq!(BackpressureLevel::default(), BackpressureLevel::None);
    }

    #[test]
    fn ordering() {
        assert!(BackpressureLevel::None < BackpressureLevel::Soft);
        assert!(BackpressureLevel::Soft < BackpressureLevel::Hard);
    }

    #[test]
    fn rejects_new_matrix() {
        assert!(!BackpressureLevel::None.rejects_new());
        assert!(!BackpressureLevel::Soft.rejects_new());
        assert!(BackpressureLevel::Hard.rejects_new());
    }

    #[test]
    fn throttles_new_matrix() {
        assert!(!BackpressureLevel::None.throttles_new());
        assert!(BackpressureLevel::Soft.throttles_new());
        assert!(BackpressureLevel::Hard.throttles_new());
    }

    #[test]
    fn from_ratio_below_thresholds() {
        assert_eq!(
            BackpressureLevel::from_ratio(0.5, 0.8, 0.95),
            BackpressureLevel::None
        );
    }

    #[test]
    fn from_ratio_at_soft() {
        assert_eq!(
            BackpressureLevel::from_ratio(0.80, 0.8, 0.95),
            BackpressureLevel::Soft
        );
    }

    #[test]
    fn from_ratio_between_thresholds() {
        assert_eq!(
            BackpressureLevel::from_ratio(0.88, 0.8, 0.95),
            BackpressureLevel::Soft
        );
    }

    #[test]
    fn from_ratio_at_hard() {
        assert_eq!(
            BackpressureLevel::from_ratio(0.95, 0.8, 0.95),
            BackpressureLevel::Hard
        );
    }

    #[test]
    fn from_ratio_above_hard() {
        assert_eq!(
            BackpressureLevel::from_ratio(0.99, 0.8, 0.95),
            BackpressureLevel::Hard
        );
    }
}

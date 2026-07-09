//! [`VersionRange`] — version range with min/max bounds.
//!
//! See `DESIGN.md` Part 4 (version encoding).

use crate::error::VersionNegotiationError;
use crate::version::ProtocolVersion;

// ── VersionRange ───────────────────────────────────────────────────

/// Version range with inclusive min/max bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VersionRange {
    min: ProtocolVersion,
    max: ProtocolVersion,
}

impl VersionRange {
    /// Construct a new range.
    ///
    /// # Errors
    /// Returns [`VersionNegotiationError::InvalidVersion`] if min > max.
    pub fn new(
        min: ProtocolVersion,
        max: ProtocolVersion,
    ) -> Result<Self, VersionNegotiationError> {
        if min.raw() > max.raw() {
            return Err(VersionNegotiationError::InvalidVersion(min.raw()));
        }
        Ok(Self { min, max })
    }

    /// Whether this range contains a version.
    #[must_use]
    pub fn contains(&self, version: ProtocolVersion) -> bool {
        // Compare raw values for efficiency.
        let raw = version.raw();
        raw >= self.min.raw() && raw <= self.max.raw()
    }

    /// Whether this range overlaps with another.
    #[must_use]
    pub fn overlaps(&self, other: &Self) -> bool {
        // Ranges overlap if: self.min <= other.max AND self.max >= other.min
        self.min.raw() <= other.max.raw() && self.max.raw() >= other.min.raw()
    }

    /// Get the minimum version.
    #[must_use]
    pub const fn min(&self) -> ProtocolVersion {
        self.min
    }

    /// Get the maximum version.
    #[must_use]
    pub const fn max(&self) -> ProtocolVersion {
        self.max
    }
}

impl Default for VersionRange {
    fn default() -> Self {
        Self {
            min: ProtocolVersion::STABLE_1_0,
            max: ProtocolVersion::STABLE_1_0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_contains_version() {
        let range = VersionRange::new(ProtocolVersion::new(1, 0, 0), ProtocolVersion::new(2, 0, 0))
            .unwrap();
        assert!(range.contains(ProtocolVersion::new(1, 5, 0)));
        assert!(range.contains(ProtocolVersion::new(1, 0, 0)));
        assert!(range.contains(ProtocolVersion::new(2, 0, 0)));
        assert!(!range.contains(ProtocolVersion::new(0, 9, 0)));
        assert!(!range.contains(ProtocolVersion::new(2, 1, 0)));
    }

    #[test]
    fn range_rejects_invalid_bounds() {
        let result =
            VersionRange::new(ProtocolVersion::new(2, 0, 0), ProtocolVersion::new(1, 0, 0));
        assert!(result.is_err());
    }

    #[test]
    fn range_overlaps() {
        let r1 = VersionRange::new(ProtocolVersion::new(1, 0, 0), ProtocolVersion::new(2, 0, 0))
            .unwrap();
        let r2 = VersionRange::new(ProtocolVersion::new(1, 5, 0), ProtocolVersion::new(3, 0, 0))
            .unwrap();
        assert!(r1.overlaps(&r2));

        let r3 = VersionRange::new(ProtocolVersion::new(3, 0, 0), ProtocolVersion::new(4, 0, 0))
            .unwrap();
        assert!(!r1.overlaps(&r3));
    }

    #[test]
    fn range_bounds() {
        let min = ProtocolVersion::new(1, 0, 0);
        let max = ProtocolVersion::new(3, 0, 0);
        let range = VersionRange::new(min, max).unwrap();
        assert_eq!(range.min(), min);
        assert_eq!(range.max(), max);
    }

    #[test]
    fn default_range_is_single_version() {
        let range = VersionRange::default();
        assert_eq!(range.min(), ProtocolVersion::STABLE_1_0);
        assert_eq!(range.max(), ProtocolVersion::STABLE_1_0);
        assert!(range.contains(ProtocolVersion::STABLE_1_0));
    }
}

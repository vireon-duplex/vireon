//! [`ProtocolVersion`] — semver-encoded protocol version.
//!
//! See `DESIGN.md` Part 4 (version encoding).

// ── ProtocolVersion ────────────────────────────────────────────────

/// Semver-encoded protocol version.
///
/// Encoding: `0xMMmmpp` where MM=major (16 bits), mm=minor (8 bits),
/// pp=patch (8 bits).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProtocolVersion(u32);

impl ProtocolVersion {
    /// Mask for major version (high 8 bits of upper 16 bits).
    pub const MAJOR_MASK: u32 = 0x00FF_0000;

    /// Mask for minor version (mid 8 bits).
    pub const MINOR_MASK: u32 = 0x0000_FF00;

    /// Mask for patch version (low 8 bits).
    pub const PATCH_MASK: u32 = 0x0000_00FF;

    /// Deprecated patch value (255 = deprecated).
    pub const DEPRECATED_PATCH: u8 = 255;

    /// Stable version 1.0.0.
    pub const STABLE_1_0: ProtocolVersion = ProtocolVersion(0x0001_0000);

    /// Construct a version from major, minor, patch components.
    ///
    /// Encoding: `MM` is major (8 bits), `mm` is minor (8 bits), `pp` is patch (8 bits).
    /// Stored as `0x00MMmmpp` where high byte is reserved.
    #[must_use]
    pub const fn new(major: u16, minor: u8, patch: u8) -> Self {
        let encoded = ((major as u32 & 0xFF) << 16) | ((minor as u32) << 8) | (patch as u32);
        Self(encoded)
    }

    /// Construct from raw encoded value.
    #[must_use]
    pub const fn from_raw(raw: u32) -> Self {
        Self(raw)
    }

    /// Get the raw encoded value.
    #[must_use]
    pub const fn raw(&self) -> u32 {
        self.0
    }

    /// Get the major version.
    #[must_use]
    pub const fn major(&self) -> u16 {
        ((self.0 & Self::MAJOR_MASK) >> 16) as u16
    }

    /// Get the minor version.
    #[must_use]
    pub const fn minor(&self) -> u8 {
        ((self.0 & Self::MINOR_MASK) >> 8) as u8
    }

    /// Get the patch version.
    #[must_use]
    pub const fn patch(&self) -> u8 {
        (self.0 & Self::PATCH_MASK) as u8
    }

    /// Whether this is a stable version (major > 0 and patch < 255).
    #[must_use]
    pub const fn is_stable(&self) -> bool {
        self.major() > 0 && self.patch() < Self::DEPRECATED_PATCH
    }

    /// Whether this is an experimental version (major == 0).
    #[must_use]
    pub const fn is_experimental(&self) -> bool {
        self.major() == 0
    }

    /// Whether this is a deprecated version (patch == 255).
    #[must_use]
    pub const fn is_deprecated(&self) -> bool {
        self.patch() == Self::DEPRECATED_PATCH
    }
}

impl Default for ProtocolVersion {
    fn default() -> Self {
        Self::STABLE_1_0
    }
}

impl PartialOrd for ProtocolVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProtocolVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare raw values (semantic version ordering).
        self.raw().cmp(&other.raw())
    }
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major(), self.minor(), self.patch())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_encoding() {
        let v = ProtocolVersion::new(1, 2, 3);
        assert_eq!(v.major(), 1);
        assert_eq!(v.minor(), 2);
        assert_eq!(v.patch(), 3);
    }

    #[test]
    fn raw_encoding() {
        let v = ProtocolVersion::new(1, 2, 3);
        assert_eq!(v.raw(), 0x0102_03);
    }

    #[test]
    fn from_raw() {
        let v = ProtocolVersion::from_raw(0x0102_03);
        assert_eq!(v.major(), 1);
        assert_eq!(v.minor(), 2);
        assert_eq!(v.patch(), 3);
    }

    #[test]
    fn stable_version_check() {
        let v = ProtocolVersion::new(1, 0, 0);
        assert!(v.is_stable());
        assert!(!v.is_experimental());
        assert!(!v.is_deprecated());
    }

    #[test]
    fn experimental_version_check() {
        let v = ProtocolVersion::new(0, 1, 0);
        assert!(!v.is_stable());
        assert!(v.is_experimental());
        assert!(!v.is_deprecated());
    }

    #[test]
    fn deprecated_version_check() {
        let v = ProtocolVersion::new(1, 0, 255);
        assert!(!v.is_stable());
        assert!(!v.is_experimental());
        assert!(v.is_deprecated());
    }

    #[test]
    fn version_ordering() {
        let v1 = ProtocolVersion::new(1, 0, 0);
        let v2 = ProtocolVersion::new(1, 0, 1);
        let v3 = ProtocolVersion::new(1, 1, 0);
        let v4 = ProtocolVersion::new(2, 0, 0);

        assert!(v1 < v2);
        assert!(v2 < v3);
        assert!(v3 < v4);
    }

    #[test]
    fn display_format() {
        let v = ProtocolVersion::new(1, 2, 3);
        assert_eq!(format!("{v}"), "1.2.3");
    }

    #[test]
    fn default_is_stable_1_0() {
        let v = ProtocolVersion::default();
        assert_eq!(v, ProtocolVersion::STABLE_1_0);
        assert_eq!(v.major(), 1);
        assert_eq!(v.minor(), 0);
        assert_eq!(v.patch(), 0);
    }
}

//! [`ApiVersion`] — API version encoding for compatibility checking.
//!
//! See `DESIGN.md` Part 4 (versioning) and Part 5 (API compatibility).
//!
//! # Design
//!
//! API versions are encoded as major.minor where:
//! - Major: incompatible changes (must match exactly)
//! - Minor: backwards-compatible additions (runtime must be >= plugin)
//!
//! The `CURRENT_API` constant defines the runtime's supported API version.

/// API version for plugin compatibility checking.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ApiVersion {
    /// Major version: incompatible API changes.
    pub major: u16,
    /// Minor version: backwards-compatible additions.
    pub minor: u8,
}

impl ApiVersion {
    /// Create a new API version.
    #[must_use]
    pub const fn new(major: u16, minor: u8) -> Self {
        Self { major, minor }
    }

    /// Check if this runtime version is backward compatible with a
    /// plugin's required version.
    ///
    /// Backward compatibility requires:
    /// - Same major version
    /// - Runtime minor >= plugin minor
    #[must_use]
    pub const fn is_backward_compatible_with(&self, required: &Self) -> bool {
        // Derived PartialEq is not const, so compare raw values.
        if self.major != required.major {
            return false;
        }
        self.minor >= required.minor
    }

    /// Pack the version into a u32 (major:16, minor:8, padding:8).
    #[must_use]
    pub const fn to_u32(&self) -> u32 {
        ((self.major as u32) << 16) | ((self.minor as u32) << 8)
    }

    /// Unpack a u32 into an ApiVersion (ignores lower 8 bits).
    #[must_use]
    pub const fn from_u32(v: u32) -> Self {
        Self {
            major: ((v >> 16) & 0xFFFF) as u16,
            minor: ((v >> 8) & 0xFF) as u8,
        }
    }
}

/// The current API version supported by this runtime.
pub const CURRENT_API: ApiVersion = ApiVersion::new(1, 0);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_version_new() {
        let v = ApiVersion::new(1, 5);
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 5);
    }

    #[test]
    fn api_version_default_is_zero() {
        let v = ApiVersion::default();
        assert_eq!(v, ApiVersion::new(0, 0));
    }

    #[test]
    fn compatible_same_version() {
        let runtime = ApiVersion::new(1, 0);
        let plugin = ApiVersion::new(1, 0);
        assert!(runtime.is_backward_compatible_with(&plugin));
    }

    #[test]
    fn compatible_runtime_higher_minor() {
        let runtime = ApiVersion::new(1, 5);
        let plugin = ApiVersion::new(1, 0);
        assert!(runtime.is_backward_compatible_with(&plugin));
    }

    #[test]
    fn incompatible_plugin_higher_minor() {
        let runtime = ApiVersion::new(1, 0);
        let plugin = ApiVersion::new(1, 5);
        assert!(!runtime.is_backward_compatible_with(&plugin));
    }

    #[test]
    fn incompatible_different_major() {
        let runtime = ApiVersion::new(1, 0);
        let plugin = ApiVersion::new(2, 0);
        assert!(!runtime.is_backward_compatible_with(&plugin));
        assert!(!plugin.is_backward_compatible_with(&runtime));
    }

    #[test]
    fn current_api_is_version_1_0() {
        assert_eq!(CURRENT_API.major, 1);
        assert_eq!(CURRENT_API.minor, 0);
    }

    #[test]
    fn api_version_to_u32_and_from_u32() {
        let v = ApiVersion::new(1, 5);
        let packed = v.to_u32();
        assert_eq!(packed, 0x010500); // 1.5
        let unpacked = ApiVersion::from_u32(packed);
        assert_eq!(unpacked, v);
    }

    #[test]
    fn api_version_roundtrip_u32() {
        for major in 0..3u16 {
            for minor in 0u8..10 {
                let v = ApiVersion::new(major, minor);
                let packed = v.to_u32();
                let unpacked = ApiVersion::from_u32(packed);
                assert_eq!(v, unpacked);
            }
        }
    }

    #[test]
    fn api_version_ordering() {
        let v1 = ApiVersion::new(1, 0);
        let v2 = ApiVersion::new(1, 5);
        let v3 = ApiVersion::new(2, 0);
        assert!(v1 < v2);
        assert!(v2 < v3);
        assert!(v1 <= v2);
        assert!(v2 >= v1);
    }

    #[test]
    fn api_version_partial_ord() {
        let v1 = ApiVersion::new(1, 5);
        let v2 = ApiVersion::new(1, 5);
        assert_eq!(v1.partial_cmp(&v2), Some(std::cmp::Ordering::Equal));
    }
}

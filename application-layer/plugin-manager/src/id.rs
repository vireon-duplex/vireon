//! [`PluginId`], [`PluginVersion`] — identifiers for plugins and their
//! versions.
//!
//! See `DESIGN.md` Part 3 (plugin model) and Part 4 (versioning).
//!
//! # Design
//!
//! - `PluginId` is a 32-byte identifier (like a hash) with an inline
//!   length prefix for flexible encoding schemes.
//! - `PluginVersion` follows semantic versioning with major, minor, and
//!   patch components, encoded as separate fields (not packed).

use std::fmt;

// ── PluginId ───────────────────────────────────────────────────────

/// A plugin identifier, 32 bytes like a hash digest.
///
/// Used as a stable key for plugin lookups regardless of where the
/// plugin was loaded from.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct PluginId {
    /// Raw bytes of the ID.
    bytes: [u8; 32],
    /// Number of valid bytes (for flexible encoding).
    len: u8,
}

impl PluginId {
    /// Create a PluginId from a u64 hash (deterministic).
    ///
    /// This is a convenience for testing; real plugins should use
    /// proper cryptographic hashes.
    #[must_use]
    pub const fn from_hash(hash: u64) -> Self {
        let mut bytes = [0u8; 32];
        let mut i = 0;
        while i < 8 {
            bytes[i] = ((hash >> (i * 8)) & 0xFF) as u8;
            i += 1;
        }
        Self { bytes, len: 8 }
    }

    /// The nil plugin ID (all zeros).
    #[must_use]
    pub const fn nil() -> Self {
        Self {
            bytes: [0u8; 32],
            len: 0,
        }
    }

    /// Whether this is the nil ID.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        if self.len == 0 {
            return true;
        }
        let mut i = 0;
        while i < 32 {
            if self.bytes[i] != 0 {
                return false;
            }
            i += 1;
        }
        true
    }

    /// Raw bytes of the ID.
    #[must_use]
    pub const fn raw(&self) -> &[u8] {
        // SAFETY: We return a slice of our own bytes.
        // The lifetime is tied to &self, so this is safe.
        let ptr = self.bytes.as_ptr();
        unsafe { std::slice::from_raw_parts(ptr, 32) }
    }

    /// Extract the first 8 bytes as a u64 (for hashing).
    #[must_use]
    pub const fn as_u64(&self) -> u64 {
        let mut result = 0u64;
        let mut i = 0u8;
        while i < 8 {
            result |= (self.bytes[i as usize] as u64) << (i as u64 * 8);
            i += 1;
        }
        result
    }
}

impl fmt::Display for PluginId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PluginId({})", self.as_u64())
    }
}

// ── PluginVersion ──────────────────────────────────────────────────

/// Semantic version for a plugin.
///
/// Encoded as three separate fields (not packed into u32) for clarity
/// and to avoid bit-twiddling in comparisons.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct PluginVersion {
    /// Major version: incompatible API changes.
    pub major: u16,
    /// Minor version: backwards-compatible additions.
    pub minor: u8,
    /// Patch version: backwards-compatible fixes.
    pub patch: u8,
}

impl PluginVersion {
    /// Create a new plugin version.
    #[must_use]
    pub const fn new(major: u16, minor: u8, patch: u8) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Check if this version is compatible with another.
    ///
    /// Compatibility requires the same major version, and this
    /// version's minor must be <= the other's minor (we support
    /// any patch level >= our own).
    #[must_use]
    pub const fn is_compatible_with(&self, other: &Self) -> bool {
        if self.major != other.major {
            return false;
        }
        // Same major, and our minor <= other minor.
        // Derived PartialEq is not const, so compare raw values.
        self.minor <= other.minor
    }

    /// Pack the version into a u32 (major:16, minor:8, patch:8).
    #[must_use]
    pub const fn to_u32(&self) -> u32 {
        ((self.major as u32) << 16) | ((self.minor as u32) << 8) | (self.patch as u32)
    }

    /// Unpack a u32 into a PluginVersion.
    #[must_use]
    pub const fn from_u32(v: u32) -> Self {
        Self {
            major: ((v >> 16) & 0xFFFF) as u16,
            minor: ((v >> 8) & 0xFF) as u8,
            patch: (v & 0xFF) as u8,
        }
    }
}

impl fmt::Display for PluginVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_id_is_nil() {
        let nil = PluginId::nil();
        assert!(nil.is_nil());
        assert_eq!(nil.as_u64(), 0);
    }

    #[test]
    fn id_from_hash_is_deterministic() {
        let id1 = PluginId::from_hash(42);
        let id2 = PluginId::from_hash(42);
        assert_eq!(id1, id2);
        assert_eq!(id2.as_u64(), 42);
    }

    #[test]
    fn id_from_hash_different_hashes_differ() {
        let id1 = PluginId::from_hash(1);
        let id2 = PluginId::from_hash(2);
        assert_ne!(id1, id2);
    }

    #[test]
    fn non_nil_id_is_not_nil() {
        let id = PluginId::from_hash(123);
        assert!(!id.is_nil());
    }

    #[test]
    fn id_raw_returns_32_bytes() {
        let id = PluginId::from_hash(456);
        assert_eq!(id.raw().len(), 32);
    }

    #[test]
    fn plugin_version_new() {
        let v = PluginVersion::new(1, 2, 3);
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 3);
    }

    #[test]
    fn plugin_version_default_is_zero() {
        let v = PluginVersion::default();
        assert_eq!(v, PluginVersion::new(0, 0, 0));
    }

    #[test]
    fn compatible_version_same_major_same_minor() {
        let v1 = PluginVersion::new(1, 2, 3);
        let v2 = PluginVersion::new(1, 2, 4);
        assert!(v1.is_compatible_with(&v2));
        assert!(v2.is_compatible_with(&v1));
    }

    #[test]
    fn compatible_version_same_major_lower_minor() {
        let v1 = PluginVersion::new(1, 2, 0);
        let v2 = PluginVersion::new(1, 5, 0);
        assert!(v1.is_compatible_with(&v2)); // 1.2 is compatible with 1.5
        assert!(!v2.is_compatible_with(&v1)); // 1.5 is not compatible with 1.2
    }

    #[test]
    fn incompatible_version_different_major() {
        let v1 = PluginVersion::new(1, 0, 0);
        let v2 = PluginVersion::new(2, 0, 0);
        assert!(!v1.is_compatible_with(&v2));
        assert!(!v2.is_compatible_with(&v1));
    }

    #[test]
    fn plugin_version_to_u32_and_from_u32() {
        let v = PluginVersion::new(1, 5, 10);
        let packed = v.to_u32();
        assert_eq!(packed, 0x01050A); // 1.5.10
        let unpacked = PluginVersion::from_u32(packed);
        assert_eq!(unpacked, v);
    }

    #[test]
    fn plugin_version_roundtrip_u32() {
        for major in 0..3u16 {
            for minor in 0..5u8 {
                for patch in 0..5u8 {
                    let v = PluginVersion::new(major, minor, patch);
                    let packed = v.to_u32();
                    let unpacked = PluginVersion::from_u32(packed);
                    assert_eq!(v, unpacked);
                }
            }
        }
    }

    #[test]
    fn plugin_version_equality() {
        let v1 = PluginVersion::new(1, 2, 3);
        let v2 = PluginVersion::new(1, 2, 3);
        let v3 = PluginVersion::new(1, 2, 4);
        assert_eq!(v1, v2);
        assert_ne!(v1, v3);
    }
}

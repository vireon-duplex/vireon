//! [`CompatibilityMatrix`], [`CompatEntry`] — version compatibility
//! tracking.
//!
//! See `DESIGN.md` Part 5 (compatibility matrix).

use crate::error::VersionNegotiationError;
use crate::version::ProtocolVersion;

// ── CompatEntry ───────────────────────────────────────────────────

/// Single compatibility entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompatEntry {
    /// Local version.
    pub local: ProtocolVersion,
    /// Remote version.
    pub remote: ProtocolVersion,
    /// Whether compatible.
    pub compatible: bool,
}

impl CompatEntry {
    /// Create a new compatibility entry.
    #[must_use]
    pub const fn new(local: ProtocolVersion, remote: ProtocolVersion, compatible: bool) -> Self {
        Self {
            local,
            remote,
            compatible,
        }
    }
}

// ── CompatibilityMatrix ──────────────────────────────────────────

/// Fixed-size compatibility matrix.
///
/// Stores up to 64 compatibility entries without heap allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompatibilityMatrix {
    entries: [Option<CompatEntry>; 64],
    len: u8,
}

impl Default for CompatibilityMatrix {
    fn default() -> Self {
        Self::new()
    }
}

impl CompatibilityMatrix {
    /// Maximum number of entries.
    pub const MAX_ENTRIES: usize = 64;

    /// Construct a new empty matrix.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: [None; Self::MAX_ENTRIES],
            len: 0,
        }
    }

    /// Add a compatibility entry.
    ///
    /// # Errors
    /// Returns [`VersionNegotiationError::InvalidVersion`] if the matrix
    /// is full.
    pub fn add(&mut self, entry: CompatEntry) -> Result<(), VersionNegotiationError> {
        if self.len as usize >= Self::MAX_ENTRIES {
            return Err(VersionNegotiationError::InvalidVersion(0));
        }
        self.entries[self.len as usize] = Some(entry);
        self.len += 1;
        Ok(())
    }

    /// Check compatibility between local and remote versions.
    ///
    /// Returns None if no entry matches, Some(bool) with compatibility.
    #[must_use]
    pub fn check(&self, local: ProtocolVersion, remote: ProtocolVersion) -> Option<bool> {
        for i in 0..self.len as usize {
            if let Some(entry) = self.entries[i] {
                if entry.local.raw() == local.raw() && entry.remote.raw() == remote.raw() {
                    return Some(entry.compatible);
                }
            }
        }
        None
    }

    /// Get the number of entries.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    /// Whether the matrix is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.entries = [None; Self::MAX_ENTRIES];
        self.len = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_entry_increments_length() {
        let mut matrix = CompatibilityMatrix::new();
        let entry = CompatEntry::new(
            ProtocolVersion::new(1, 0, 0),
            ProtocolVersion::new(1, 0, 0),
            true,
        );
        matrix.add(entry).unwrap();
        assert_eq!(matrix.len(), 1);
        assert!(!matrix.is_empty());
    }

    #[test]
    fn check_returns_correct_compatibility() {
        let mut matrix = CompatibilityMatrix::new();
        let entry = CompatEntry::new(
            ProtocolVersion::new(1, 0, 0),
            ProtocolVersion::new(1, 0, 0),
            true,
        );
        matrix.add(entry).unwrap();

        let compat = matrix.check(ProtocolVersion::new(1, 0, 0), ProtocolVersion::new(1, 0, 0));
        assert_eq!(compat, Some(true));
    }

    #[test]
    fn check_returns_none_for_unknown_versions() {
        let matrix = CompatibilityMatrix::new();
        let compat = matrix.check(ProtocolVersion::new(1, 0, 0), ProtocolVersion::new(1, 0, 0));
        assert_eq!(compat, None);
    }

    #[test]
    fn matrix_respects_max_entries() {
        let mut matrix = CompatibilityMatrix::new();
        for i in 0..64 {
            let entry = CompatEntry::new(
                ProtocolVersion::new(1, 0, i as u8),
                ProtocolVersion::new(1, 0, i as u8),
                true,
            );
            matrix.add(entry).unwrap();
        }
        // Next add should fail.
        let entry = CompatEntry::new(
            ProtocolVersion::new(1, 0, 64),
            ProtocolVersion::new(1, 0, 64),
            true,
        );
        assert!(matrix.add(entry).is_err());
    }

    #[test]
    fn clear_removes_all_entries() {
        let mut matrix = CompatibilityMatrix::new();
        let entry = CompatEntry::new(
            ProtocolVersion::new(1, 0, 0),
            ProtocolVersion::new(1, 0, 0),
            true,
        );
        matrix.add(entry).unwrap();
        matrix.clear();
        assert_eq!(matrix.len(), 0);
        assert!(matrix.is_empty());
    }

    #[test]
    fn default_matrix_is_empty() {
        let matrix = CompatibilityMatrix::default();
        assert!(matrix.is_empty());
        assert_eq!(matrix.len(), 0);
    }
}

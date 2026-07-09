//! [`ConfigKey`] — fixed-size configuration key.
//!
//! See `DESIGN.md` Part 4 (key/value types).

use crate::error::ConfigError;

// ── Constants ───────────────────────────────────────────────────

/// Maximum key length in bytes (one byte reserved for length).
pub const MAX_LEN: usize = 63;

// ── ConfigKey ────────────────────────────────────────────────────

/// Fixed-size configuration key.
///
/// Keys are stored inline as a 64-byte array with the first byte
/// storing the length. This makes keys Copy and avoids heap allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConfigKey {
    bytes: [u8; Self::SIZE],
}

impl ConfigKey {
    /// Size of the key in bytes (fixed).
    const SIZE: usize = 64;

    /// Construct a key from a string.
    ///
    /// # Errors
    /// Returns [`ConfigError::InvalidKey`] if the string is too long
    /// or contains invalid characters (only alphanumeric, underscore,
    /// hyphen, and dot allowed).
    pub fn new(s: &str) -> Result<Self, ConfigError> {
        let len = s.len();
        if len == 0 {
            return Err(ConfigError::InvalidKey("key cannot be empty".to_string()));
        }
        if len > MAX_LEN {
            return Err(ConfigError::InvalidKey(format!(
                "key length {len} exceeds maximum {MAX_LEN}"
            )));
        }
        // Validate characters: alphanumeric, underscore, hyphen, dot only.
        if !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.')
        {
            return Err(ConfigError::InvalidKey(format!(
                "key contains invalid characters: {s}"
            )));
        }

        let mut bytes = [0u8; Self::SIZE];
        bytes[0] = len as u8;
        bytes[1..=len].copy_from_slice(s.as_bytes());

        Ok(Self { bytes })
    }

    /// Construct a nil (zero) key.
    #[must_use]
    pub const fn nil() -> Self {
        Self {
            bytes: [0u8; Self::SIZE],
        }
    }

    /// Whether this key is nil.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        self.bytes[0] == 0
    }

    /// View the key as a string slice.
    ///
    /// # Safety
    /// The key bytes must be valid UTF-8. This is guaranteed by
    /// [`ConfigKey::new`] which validates input.
    #[must_use]
    pub fn as_str(&self) -> &str {
        let len = self.bytes[0] as usize;
        // Safety: new() ensures bytes[1..=len] is valid UTF-8.
        unsafe {
            let slice = self.bytes.get_unchecked(1..=len);
            std::str::from_utf8_unchecked(slice)
        }
    }

    /// View the raw bytes (including length prefix).
    #[must_use]
    pub const fn raw(&self) -> &[u8; Self::SIZE] {
        &self.bytes
    }
}

impl Default for ConfigKey {
    fn default() -> Self {
        Self::nil()
    }
}

impl PartialOrd for ConfigKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ConfigKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by length first, then by bytes.
        let len_a = self.bytes[0];
        let len_b = other.bytes[0];
        match len_a.cmp(&len_b) {
            std::cmp::Ordering::Equal => {
                let len = len_a as usize;
                self.bytes[1..=len].cmp(&other.bytes[1..=len])
            }
            ord => ord,
        }
    }
}

impl std::fmt::Display for ConfigKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_key_is_nil() {
        let key = ConfigKey::nil();
        assert!(key.is_nil());
        assert_eq!(key.as_str(), "");
    }

    #[test]
    fn valid_key_constructs() {
        let key = ConfigKey::new("server.port").unwrap();
        assert!(!key.is_nil());
        assert_eq!(key.as_str(), "server.port");
    }

    #[test]
    fn empty_key_is_invalid() {
        assert!(ConfigKey::new("").is_err());
    }

    #[test]
    fn key_too_long_is_invalid() {
        let long_key = "a".repeat(64);
        assert!(ConfigKey::new(&long_key).is_err());
    }

    #[test]
    fn max_length_key_constructs() {
        let max_key = "a".repeat(MAX_LEN);
        let key = ConfigKey::new(&max_key).unwrap();
        assert_eq!(key.as_str(), max_key);
    }

    #[test]
    fn invalid_characters_are_rejected() {
        assert!(ConfigKey::new("server port").is_err()); // space
        assert!(ConfigKey::new("server/port").is_err()); // slash
        assert!(ConfigKey::new("server:port").is_err()); // colon
    }

    #[test]
    fn keys_compare_by_length_then_bytes() {
        let a = ConfigKey::new("a").unwrap();
        let b = ConfigKey::new("ab").unwrap();
        let c = ConfigKey::new("ab").unwrap();
        let d = ConfigKey::new("ac").unwrap();

        // Shorter keys come first.
        assert!(a < b);
        // Same length, same bytes → equal.
        assert_eq!(b, c);
        // Same length, different bytes → lexical.
        assert!(b < d);
    }

    #[test]
    fn key_hashes_differently() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let a = ConfigKey::new("server.port").unwrap();
        let b = ConfigKey::new("server.host").unwrap();

        let mut h1 = DefaultHasher::new();
        a.hash(&mut h1);
        let hash1 = h1.finish();

        let mut h2 = DefaultHasher::new();
        b.hash(&mut h2);
        let hash2 = h2.finish();

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn key_display_shows_content() {
        let key = ConfigKey::new("test.key").unwrap();
        assert_eq!(format!("{key}"), "test.key");
    }

    #[test]
    fn default_is_nil() {
        let key = ConfigKey::default();
        assert!(key.is_nil());
    }
}

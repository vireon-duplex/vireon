//! [`FlagKey`], [`FlagValue`], [`FlagState`] — typed identifiers and
//! values for feature flags.
//!
//! # Design
//!
//! - `FlagKey`: Fixed-size 63-byte ASCII identifier (mirrors `ConfigKey`).
//! - `FlagValue`: Enum of Bool, Percentage (0..=100), and String (up to 127 bytes).
//! - `FlagState`: Complete flag state including enabled status, value, freeze status,
//!   and last-updated timestamp.

use crate::error::FlagError;

// ── FlagKey ───────────────────────────────────────────────────────

/// A typed feature flag key.
///
/// Fixed-size 63-byte ASCII identifier (maximum length is 63 characters).
/// Stored as a `[u8; 64]` with length prefix for efficient copying.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FlagKey {
    bytes: [u8; Self::LEN],
    len: u8,
}

impl FlagKey {
    /// Maximum length of a flag key in bytes.
    pub const MAX_LEN: u8 = 63;

    /// Internal buffer length (includes length byte).
    const LEN: usize = 64;

    /// Construct a new flag key from a string slice.
    ///
    /// # Errors
    /// Returns `FlagError::InvalidValue` if the string:
    /// - Exceeds 63 bytes
    /// - Contains non-ASCII characters
    pub fn new(s: &str) -> Result<Self, FlagError> {
        if s.len() > Self::MAX_LEN as usize {
            return Err(FlagError::InvalidValue(format!(
                "flag key too long: {} > {}",
                s.len(),
                Self::MAX_LEN
            )));
        }
        if !s.is_ascii() {
            return Err(FlagError::InvalidValue(
                "flag key must be ASCII".to_string(),
            ));
        }

        let mut bytes = [0u8; Self::LEN];
        bytes[..s.len()].copy_from_slice(s.as_bytes());

        Ok(Self {
            bytes,
            len: s.len() as u8,
        })
    }

    /// The nil flag key (all zeros, length 0).
    #[must_use]
    pub const fn nil() -> Self {
        Self {
            bytes: [0u8; Self::LEN],
            len: 0,
        }
    }

    /// Whether this is the nil flag key.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        self.len == 0
    }

    /// View the flag key as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        // SAFETY: We ensure only ASCII is stored in `new`.
        unsafe { std::str::from_utf8_unchecked(&self.bytes[..self.len as usize]) }
    }

    /// Raw bytes (including trailing zeros).
    #[must_use]
    pub const fn raw(&self) -> &[u8; Self::LEN] {
        &self.bytes
    }
}

// ── FlagValue ─────────────────────────────────────────────────────

/// A typed feature flag value.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlagValue {
    /// Boolean flag value.
    Bool(bool),
    /// Percentage rollout value (0..=100).
    Percentage(u8),
    /// String value (up to 127 bytes).
    Str([u8; 128]),
}

impl FlagValue {
    /// Maximum length for string values.
    pub const MAX_STR_LEN: usize = 127;

    /// Create a boolean flag value.
    #[must_use]
    pub const fn bool(b: bool) -> Self {
        Self::Bool(b)
    }

    /// Create a percentage flag value.
    ///
    /// # Panics
    /// Panics in debug mode if percentage > 100.
    #[must_use]
    pub const fn percentage(p: u8) -> Self {
        debug_assert!(p <= 100, "percentage must be <= 100");
        Self::Percentage(p)
    }

    /// Create a string flag value.
    ///
    /// # Errors
    /// Returns `FlagError::InvalidValue` if the string:
    /// - Exceeds 127 bytes
    /// - Contains non-ASCII characters
    pub fn str(s: &str) -> Result<Self, FlagError> {
        if s.len() > Self::MAX_STR_LEN {
            return Err(FlagError::InvalidValue(format!(
                "flag value too long: {} > {}",
                s.len(),
                Self::MAX_STR_LEN
            )));
        }
        if !s.is_ascii() {
            return Err(FlagError::InvalidValue(
                "flag value must be ASCII".to_string(),
            ));
        }

        let mut bytes = [0u8; 128];
        bytes[..s.len()].copy_from_slice(s.as_bytes());

        Ok(Self::Str(bytes))
    }

    /// Extract the boolean value, if present.
    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match *self {
            Self::Bool(b) => Some(b),
            _ => None,
        }
    }

    /// Extract the percentage value, if present.
    #[must_use]
    pub const fn as_percentage(&self) -> Option<u8> {
        match *self {
            Self::Percentage(p) => Some(p),
            _ => None,
        }
    }

    /// Extract the string value, if present.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match *self {
            Self::Str(ref bytes) => {
                // Find null terminator.
                let len = bytes.iter().position(|&b| b == 0).unwrap_or(127);
                // SAFETY: We ensure only ASCII is stored in `str`.
                unsafe { Some(std::str::from_utf8_unchecked(&bytes[..len])) }
            }
            _ => None,
        }
    }

    /// Get the type name of this value.
    #[must_use]
    pub const fn type_name(&self) -> &'static str {
        match *self {
            Self::Bool(_) => "bool",
            Self::Percentage(_) => "percentage",
            Self::Str(_) => "string",
        }
    }
}

// ── FlagState ─────────────────────────────────────────────────────

/// Complete state of a feature flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlagState {
    /// Whether the flag is enabled.
    pub enabled: bool,
    /// The flag value.
    pub value: FlagValue,
    /// Whether the flag is frozen (immutable).
    pub frozen: bool,
    /// Last updated timestamp (nanoseconds since epoch).
    pub updated_ns: u64,
}

impl FlagState {
    /// Create a new flag state.
    #[must_use]
    pub const fn new(enabled: bool, value: FlagValue, frozen: bool, updated_ns: u64) -> Self {
        Self {
            enabled,
            value,
            frozen,
            updated_ns,
        }
    }

    /// Whether the flag is enabled.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get the flag value.
    #[must_use]
    pub const fn value(&self) -> FlagValue {
        self.value
    }

    /// Whether the flag is frozen.
    #[must_use]
    pub const fn is_frozen(&self) -> bool {
        self.frozen
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_key_new_valid_ascii() {
        let key = FlagKey::new("my_feature").unwrap();
        assert_eq!(key.as_str(), "my_feature");
        assert_eq!(key.len, 10);
        assert!(!key.is_nil());
    }

    #[test]
    fn flag_key_rejects_too_long() {
        let long = "a".repeat(64);
        assert!(FlagKey::new(&long).is_err());
    }

    #[test]
    fn flag_key_rejects_non_ascii() {
        assert!(FlagKey::new("héllo").is_err());
        assert!(FlagKey::new("你好").is_err());
    }

    #[test]
    fn flag_key_nil() {
        let nil = FlagKey::nil();
        assert!(nil.is_nil());
        assert_eq!(nil.len, 0);
        assert_eq!(nil.as_str(), "");
    }

    #[test]
    fn flag_key_max_length() {
        let max_len = "a".repeat(63);
        let key = FlagKey::new(&max_len).unwrap();
        assert_eq!(key.len, 63);
        assert_eq!(key.as_str(), &max_len);
    }

    #[test]
    fn flag_key_copy_and_clone() {
        let key1 = FlagKey::new("test").unwrap();
        let key2 = key1;
        assert_eq!(key1, key2);

        let key3 = key1;
        assert_eq!(key3.as_str(), "test");
    }

    #[test]
    fn flag_value_bool() {
        let v = FlagValue::bool(true);
        assert_eq!(v.type_name(), "bool");
        assert_eq!(v.as_bool(), Some(true));
        assert_eq!(v.as_percentage(), None);
        assert_eq!(v.as_str(), None);
    }

    #[test]
    fn flag_value_percentage() {
        let v = FlagValue::percentage(75);
        assert_eq!(v.type_name(), "percentage");
        assert_eq!(v.as_percentage(), Some(75));
        assert_eq!(v.as_bool(), None);
        assert_eq!(v.as_str(), None);
    }

    #[test]
    fn flag_value_percentage_valid_range() {
        assert_eq!(FlagValue::percentage(0).as_percentage(), Some(0));
        assert_eq!(FlagValue::percentage(100).as_percentage(), Some(100));
    }

    #[test]
    fn flag_value_str() {
        let v = FlagValue::str("hello").unwrap();
        assert_eq!(v.type_name(), "string");
        assert_eq!(v.as_str(), Some("hello"));
        assert_eq!(v.as_bool(), None);
        assert_eq!(v.as_percentage(), None);
    }

    #[test]
    fn flag_value_str_too_long() {
        let long = "x".repeat(128);
        assert!(FlagValue::str(&long).is_err());
    }

    #[test]
    fn flag_value_str_non_ascii() {
        assert!(FlagValue::str("héllo").is_err());
    }

    #[test]
    fn flag_value_str_max_length() {
        let max_len = "y".repeat(127);
        let v = FlagValue::str(&max_len).unwrap();
        assert_eq!(v.as_str(), Some(&max_len[..]));
    }

    #[test]
    fn flag_state_accessors() {
        let state = FlagState::new(true, FlagValue::bool(true), false, 12345);
        assert!(state.is_enabled());
        assert_eq!(state.value(), FlagValue::bool(true));
        assert!(!state.is_frozen());
        assert_eq!(state.updated_ns, 12345);
    }

    #[test]
    fn flag_state_frozen() {
        let state = FlagState::new(false, FlagValue::percentage(50), true, 999);
        assert!(!state.is_enabled());
        assert!(state.is_frozen());
    }

    #[test]
    fn flag_value_string_with_null_terminator() {
        let mut bytes = [0u8; 128];
        bytes[..5].copy_from_slice(b"hello");
        bytes[7] = b'x'; // past null terminator
        let v = FlagValue::Str(bytes);
        // Should only read up to null terminator.
        assert_eq!(v.as_str(), Some("hello"));
    }
}

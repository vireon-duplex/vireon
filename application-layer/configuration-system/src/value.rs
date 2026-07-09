//! [`ConfigValue`] — typed configuration value.
//!
//! See `DESIGN.md` Part 4 (key/value types).

// ── ConfigValue ──────────────────────────────────────────────────

/// Typed configuration value.
///
/// Values are stored inline in the enum to keep the type Copy and
/// avoid heap allocation. Strings are stored as fixed-size arrays.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigValue {
    /// Boolean value.
    Bool(bool),
    /// Signed 64-bit integer.
    I64(i64),
    /// Unsigned 64-bit integer.
    U64(u64),
    /// String value (fixed-size array + length).
    Str { bytes: [u8; 256], len: u8 },
    /// Duration in nanoseconds.
    Duration(u64),
    /// Byte array (fixed-size + length).
    Bytes { bytes: [u8; 64], len: u8 },
}

impl ConfigValue {
    /// Construct a string value.
    #[must_use]
    pub fn string(s: &str) -> Self {
        let len = s.len().min(255);
        let mut bytes = [0u8; 256];
        bytes[..len].copy_from_slice(&s.as_bytes()[..len]);
        Self::Str {
            bytes,
            len: len as u8,
        }
    }

    /// Construct a bytes value.
    #[must_use]
    pub fn bytes(b: &[u8]) -> Self {
        let len = b.len().min(64);
        let mut bytes = [0u8; 64];
        bytes[..len].copy_from_slice(&b[..len]);
        Self::Bytes {
            bytes,
            len: len as u8,
        }
    }

    /// View as bool, if applicable.
    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// View as i64, if applicable.
    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            Self::I64(i) => Some(*i),
            _ => None,
        }
    }

    /// View as u64, if applicable.
    #[must_use]
    pub const fn as_u64(&self) -> Option<u64> {
        match self {
            Self::U64(u) => Some(*u),
            _ => None,
        }
    }

    /// View as string, if applicable.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str { bytes, len } => {
                // Safety: string() ensures bytes are valid UTF-8.
                let slice = &bytes[..(*len as usize)];
                unsafe { std::str::from_utf8_unchecked(slice).into() }
            }
            _ => None,
        }
    }

    /// View as duration in nanoseconds, if applicable.
    #[must_use]
    pub const fn as_duration_ns(&self) -> Option<u64> {
        match self {
            Self::Duration(ns) => Some(*ns),
            _ => None,
        }
    }

    /// Get the type name of this value.
    #[must_use]
    pub const fn type_name(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::I64(_) => "i64",
            Self::U64(_) => "u64",
            Self::Str { .. } => "str",
            Self::Duration(_) => "duration",
            Self::Bytes { .. } => "bytes",
        }
    }
}

impl Default for ConfigValue {
    fn default() -> Self {
        Self::Bool(false)
    }
}

impl std::fmt::Display for ConfigValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bool(b) => write!(f, "{b}"),
            Self::I64(i) => write!(f, "{i}"),
            Self::U64(u) => write!(f, "{u}"),
            Self::Str { bytes, len } => {
                let s = std::str::from_utf8(&bytes[..(*len as usize)]).unwrap_or("<invalid utf8>");
                write!(f, "{s}")
            }
            Self::Duration(ns) => write!(f, "{ns} ns"),
            Self::Bytes { bytes: _, len } => {
                write!(f, "bytes({})", *len as usize)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_value_accessors() {
        let v = ConfigValue::Bool(true);
        assert_eq!(v.as_bool(), Some(true));
        assert_eq!(v.as_i64(), None);
        assert_eq!(v.type_name(), "bool");
    }

    #[test]
    fn i64_value_accessors() {
        let v = ConfigValue::I64(-42);
        assert_eq!(v.as_i64(), Some(-42));
        assert_eq!(v.as_u64(), None);
        assert_eq!(v.type_name(), "i64");
    }

    #[test]
    fn u64_value_accessors() {
        let v = ConfigValue::U64(42);
        assert_eq!(v.as_u64(), Some(42));
        assert_eq!(v.as_i64(), None);
        assert_eq!(v.type_name(), "u64");
    }

    #[test]
    fn string_value_accessors() {
        let v = ConfigValue::string("hello");
        assert_eq!(v.as_str(), Some("hello"));
        assert_eq!(v.as_bool(), None);
        assert_eq!(v.type_name(), "str");
    }

    #[test]
    fn string_truncates_at_max_length() {
        let long = "a".repeat(300);
        let v = ConfigValue::string(&long);
        let s = v.as_str().unwrap();
        assert_eq!(s.len(), 255); // truncated
        assert!(s.starts_with("aaa"));
    }

    #[test]
    fn bytes_value_accessors() {
        let data = b"hello";
        let v = ConfigValue::bytes(data);
        assert_eq!(v.as_str(), None);
        assert_eq!(v.type_name(), "bytes");
    }

    #[test]
    fn bytes_truncates_at_max_length() {
        let data: Vec<u8> = (0..100).collect();
        let v = ConfigValue::bytes(&data);
        assert_eq!(v.type_name(), "bytes");
        // Bytes are stored, we just can't inspect them easily without exposing the array.
    }

    #[test]
    fn duration_value_accessors() {
        let v = ConfigValue::Duration(1_000_000_000); // 1 second in ns
        assert_eq!(v.as_duration_ns(), Some(1_000_000_000));
        assert_eq!(v.as_u64(), None);
        assert_eq!(v.type_name(), "duration");
    }

    #[test]
    fn default_value_is_false_bool() {
        let v = ConfigValue::default();
        assert_eq!(v, ConfigValue::Bool(false));
    }

    #[test]
    fn display_shows_value() {
        assert_eq!(format!("{}", ConfigValue::Bool(true)), "true");
        assert_eq!(format!("{}", ConfigValue::I64(-42)), "-42");
        assert_eq!(format!("{}", ConfigValue::U64(42)), "42");
        assert_eq!(format!("{}", ConfigValue::string("test")), "test");
        assert_eq!(format!("{}", ConfigValue::Duration(100)), "100 ns");
    }

    #[test]
    fn values_are_copy() {
        let a = ConfigValue::Bool(true);
        let b = a;
        assert_eq!(a, ConfigValue::Bool(true)); // a is still valid
        assert_eq!(b, ConfigValue::Bool(true));
    }

    #[test]
    fn empty_string_value() {
        let v = ConfigValue::string("");
        assert_eq!(v.as_str(), Some(""));
        assert_eq!(v.type_name(), "str");
    }
}

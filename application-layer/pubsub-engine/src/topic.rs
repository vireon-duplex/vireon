//! [`Topic`] — a validated concrete topic name (no wildcards).
//!
//! Distinct from [`crate::TopicPattern`], which may contain `*` wildcards
//! for subscription matching. A `Topic` is what a published message
//! carries; a `TopicPattern` is what a subscriber registers. Validation
//! rejects wildcard segments so a pattern can never be accidentally
//! used where a concrete topic is expected.
//!
//! ## Validation
//!
//! | Rule | Rationale |
//! |------|-----------|
//! | non-empty | an empty topic has no routing meaning |
//! | ≤ 255 bytes | mirrors `wal_store::validate_topic` so any `Topic` is WAL-eligible |
//! | no leading/trailing whitespace | catches accidental `" foo"` from sloppy clients |
//! | no segment equal to `*` | wildcards belong in `TopicPattern`, not `Topic` |
//! | no NUL, `/`, `\` | mirrors `wal_store` filesystem rules — see below |
//!
//! `foo*bar` is **not** rejected: under the matcher at
//! [`crate::TopicPattern::matches`] the `*` is a wildcard only when it is
//! a full segment, so `foo*bar` is a literal. Rejecting it would forbid
//! legitimate topic names containing an asterisk.
//!
//! ## Why mirror `wal_store`'s filesystem rules?
//!
//! `wal_store` is a separate crate that does not depend on
//! `pubsub-engine`; adding a dependency just to share a string newtype
//! would invert the layering (the WAL sits below the engine). Mirroring
//! the filesystem rules — NUL, `/`, `\`, 255-byte cap — at construction
//! time means any `Topic` is always a valid WAL key without needing a
//! second validation pass at the persistence boundary. This is a
//! documented invariant coincidence, not a dependency.

use std::fmt;

use crate::error::PubSubError;

/// A concrete, publishable topic name.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Topic {
    /// The validated topic name.
    name: String,
}

impl Topic {
    /// Maximum encoded length in bytes.
    ///
    /// Set to 255 (not 256 like [`crate::TopicPattern::new`]) so that any
    /// valid `Topic` is also a valid key for `wal_store`, which enforces
    /// a 255-byte filesystem limit. See the module docs for the rationale.
    pub const MAX_LEN: usize = 255;

    /// Construct a new topic.
    ///
    /// # Errors
    /// Returns [`PubSubError::PublishFailed`] when `name`:
    /// * is empty,
    /// * exceeds [`Self::MAX_LEN`] bytes,
    /// * has leading or trailing whitespace,
    /// * contains a wildcard segment `*`,
    /// * contains a NUL, `/`, or `\`.
    pub fn new(name: &str) -> Result<Self, PubSubError> {
        if name.is_empty() {
            return Err(PubSubError::PublishFailed("topic name cannot be empty"));
        }
        if name.len() > Self::MAX_LEN {
            return Err(PubSubError::PublishFailed(
                "topic name exceeds 255 bytes",
            ));
        }
        if name.starts_with(' ') || name.ends_with(' ') {
            return Err(PubSubError::PublishFailed(
                "topic name has leading or trailing whitespace",
            ));
        }
        // Reject only wildcard *segments* — `foo*bar` is a literal under
        // the matcher (the `*` is a wildcard only when it is a full
        // segment between dots). This mirror's TopicPattern's semantics.
        for seg in name.split('.') {
            if seg == "*" {
                return Err(PubSubError::PublishFailed(
                    "topic name must not contain wildcard segment '*'",
                ));
            }
        }
        // Filesystem-safe: a valid Topic is always a valid WAL key.
        for b in name.bytes() {
            if b == 0 || b == b'/' || b == b'\\' {
                return Err(PubSubError::PublishFailed(
                    "topic name contains NUL or path separator",
                ));
            }
        }
        Ok(Self {
            name: name.to_owned(),
        })
    }

    /// Get the topic as a `&str`. Cheap; no allocation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

impl std::str::FromStr for Topic {
    type Err = PubSubError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl AsRef<str> for Topic {
    fn as_ref(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    // ── accepted names ──────────────────────────────────────────────

    #[test]
    fn valid_simple() {
        assert!(Topic::new("sensor.temp").is_ok());
    }

    #[test]
    fn valid_single_segment() {
        assert!(Topic::new("alerts").is_ok());
    }

    #[test]
    fn valid_dashes_and_dots() {
        assert!(Topic::new("with.dots.and-dashes").is_ok());
    }

    #[test]
    fn valid_max_length() {
        let s = "a".repeat(255);
        assert!(Topic::new(&s).is_ok());
    }

    #[test]
    fn literal_star_inside_segment_ok() {
        // "foo*bar" is NOT a wildcard under the matcher — it is a literal.
        assert!(Topic::new("foo*bar").is_ok());
    }

    // ── rejected names ──────────────────────────────────────────────

    #[test]
    fn empty_rejected() {
        assert!(Topic::new("").is_err());
    }

    #[test]
    fn too_long_rejected() {
        let s = "a".repeat(256);
        assert!(Topic::new(&s).is_err());
    }

    #[test]
    fn wildcard_segment_rejected() {
        assert!(Topic::new("sensor.*").is_err());
    }

    #[test]
    fn wildcard_only_rejected() {
        assert!(Topic::new("*").is_err());
    }

    #[test]
    fn wildcard_first_segment_rejected() {
        assert!(Topic::new("*.temp").is_err());
    }

    #[test]
    fn wildcard_middle_segment_rejected() {
        assert!(Topic::new("a.*.b").is_err());
    }

    #[test]
    fn path_sep_rejected() {
        assert!(Topic::new("a/b").is_err());
    }

    #[test]
    fn backslash_rejected() {
        assert!(Topic::new("a\\b").is_err());
    }

    #[test]
    fn nul_rejected() {
        assert!(Topic::new("a\0b").is_err());
    }

    #[test]
    fn leading_space_rejected() {
        assert!(Topic::new(" t").is_err());
    }

    #[test]
    fn trailing_space_rejected() {
        assert!(Topic::new("t ").is_err());
    }

    // ── accessors / traits ─────────────────────────────────────────

    #[test]
    fn as_str_roundtrips() {
        let t = Topic::new("sensor.temp").unwrap();
        assert_eq!(t.as_str(), "sensor.temp");
    }

    #[test]
    fn display_matches_inner() {
        let t = Topic::new("sensor.temp").unwrap();
        assert_eq!(format!("{t}"), "sensor.temp");
    }

    #[test]
    fn as_ref_str() {
        let t = Topic::new("x.y").unwrap();
        let s: &str = t.as_ref();
        assert_eq!(s, "x.y");
    }

    #[test]
    fn from_str_ok() {
        use std::str::FromStr;
        let t = Topic::from_str("x.y").unwrap();
        assert_eq!(t.as_str(), "x.y");
    }

    #[test]
    fn from_str_err_propagates() {
        use std::str::FromStr;
        assert!(Topic::from_str("").is_err());
        assert!(Topic::from_str("a.*").is_err());
    }

    #[test]
    fn equality() {
        assert_eq!(Topic::new("a").unwrap(), Topic::new("a").unwrap());
        assert_ne!(Topic::new("a").unwrap(), Topic::new("b").unwrap());
    }

    #[test]
    fn hashes_consistently() {
        use std::collections::HashSet;
        let mut s: HashSet<Topic> = HashSet::new();
        s.insert(Topic::new("x").unwrap());
        s.insert(Topic::new("x").unwrap());
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn clone_is_equal() {
        let a = Topic::new("sensor.temp").unwrap();
        let b = a.clone();
        assert_eq!(a, b);
    }
}

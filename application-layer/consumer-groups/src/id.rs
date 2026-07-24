//! Identifiers: [`ConsumerId`], [`GroupId`], [`Partition`].
//!
//! All three are newtypes around `String` (or `u32`) so they cannot be
//! confused with each other or with raw strings in API signatures. The
//! string-based ids validate on construction: 1..=255 bytes of UTF-8
//! with no NUL or whitespace.

use std::fmt;

use crate::error::GroupError;

/// Consumer (subscriber) identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConsumerId(String);

impl ConsumerId {
    /// Construct after validating.
    ///
    /// # Errors
    /// Returns [`GroupError::InvalidName`] for empty, oversized,
    /// whitespace-bearing, or NUL-containing ids.
    pub fn new(s: &str) -> Result<Self, GroupError> {
        validate_name(s)?;
        Ok(Self(s.to_owned()))
    }

    /// Borrowed view of the underlying string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Owned inner string. Useful when handing the id across a
    /// serialization boundary.
    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for ConsumerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Consumer-group identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GroupId(String);

impl GroupId {
    /// Construct after validating.
    ///
    /// # Errors
    /// Returns [`GroupError::InvalidName`] for empty, oversized,
    /// whitespace-bearing, or NUL-containing ids.
    pub fn new(s: &str) -> Result<Self, GroupError> {
        validate_name(s)?;
        Ok(Self(s.to_owned()))
    }

    /// Borrowed view of the underlying string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Partition index within a topic. `0..partitions`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Partition(pub u32);

impl Partition {
    /// Construct, validating that `p` is a real index (not a sentinel).
    /// Partition values are unbounded at construction — the consumer
    /// group further checks that the partition fits the topic's
    /// declared partition count.
    #[inline]
    #[must_use]
    pub const fn new(p: u32) -> Self {
        Self(p)
    }

    /// Raw underlying index.
    #[inline]
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for Partition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "p{}", self.0)
    }
}

/// Validate a consumer/group name. Rules are tighter than for raw
/// topic names (see [`wal_store::store::validate_topic`]) because ids
/// often appear in protocol frames and operator-facing tooling.
fn validate_name(s: &str) -> Result<(), GroupError> {
    if s.is_empty() {
        return Err(GroupError::InvalidName {
            reason: "name is empty",
        });
    }
    if s.len() > 255 {
        return Err(GroupError::InvalidName {
            reason: "name exceeds 255 bytes",
        });
    }
    if s.chars().any(char::is_whitespace) {
        return Err(GroupError::InvalidName {
            reason: "name contains whitespace",
        });
    }
    if s.bytes().any(|b| b == 0) {
        return Err(GroupError::InvalidName {
            reason: "name contains NUL",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn consumer_id_valid() {
        let c = ConsumerId::new("consumer-1").unwrap();
        assert_eq!(c.as_str(), "consumer-1");
        assert_eq!(format!("{c}"), "consumer-1");
    }

    #[test]
    fn consumer_id_rejects_invalid() {
        assert!(ConsumerId::new("").is_err());
        assert!(ConsumerId::new("with space").is_err());
        assert!(ConsumerId::new("with\0nul").is_err());
        let long = "a".repeat(256);
        assert!(ConsumerId::new(&long).is_err());
    }

    #[test]
    fn group_id_valid() {
        let g = GroupId::new("group-A").unwrap();
        assert_eq!(g.as_str(), "group-A");
    }

    #[test]
    fn partition_format() {
        let p = Partition::new(7);
        assert_eq!(p.get(), 7);
        assert_eq!(format!("{p}"), "p7");
    }
}

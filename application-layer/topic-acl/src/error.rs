//! Errors returned by the topic-acl crate.

use thiserror::Error;

/// All errors emitted by [`crate::AclTable`] and friends.
#[derive(Debug, Error)]
pub enum AclError {
    /// A rule was constructed with an empty pattern. Patterns must
    /// be 1..=65535 bytes so they cannot be confused with "missing".
    #[error("ACL pattern must be non-empty")]
    EmptyPattern,

    /// A rule pattern exceeded the 65535-byte limit (fits the
    /// `u16` length prefix used on the wire).
    #[error("ACL pattern exceeds 65535 bytes")]
    OverlongPattern,

    /// A rule was constructed with an empty [`crate::ActionSet`]. An
    /// empty set would match no actions, making the rule a no-op —
    /// almost always a caller bug.
    #[error("ACL rule must grant at least one action")]
    EmptyActionSet,
}

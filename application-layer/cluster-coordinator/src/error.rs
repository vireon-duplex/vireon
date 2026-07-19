//! Errors returned by the cluster-coordinator crate.

use thiserror::Error;

/// All errors emitted by [`crate::ClusterCoordinator`] and friends.
#[derive(Debug, Error)]
pub enum ClusterError {
    /// The supplied [`crate::ClusterConfig`] was malformed — duplicate
    /// node ids, missing self, empty peer list, etc.
    #[error("invalid cluster config: {reason}")]
    InvalidConfig {
        /// Human-readable reason.
        reason: &'static str,
    },

    /// An operation referenced a [`session_registry::NodeId`] that is
    /// not in the membership list.
    #[error("unknown node: {0}")]
    UnknownNode(String),

    /// [`crate::InterNodeMessage::decode`] failed — buffer was too
    /// short, contained an unknown discriminant, or carried an
    /// over-length field.
    #[error("inter-node message decode failed: {reason}")]
    DecodeFailed {
        /// Human-readable reason.
        reason: &'static str,
    },

    /// An [`crate::InterNodeMessage`] was constructed or decoded with
    /// an empty topic. The wire format reserves 0-length topics as
    /// illegal so they cannot be confused with "missing field".
    #[error("inter-node message topic must be non-empty")]
    TopicEmpty,
}

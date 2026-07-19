//! Errors returned by the consumer-groups crate.

use thiserror::Error;

/// All errors emitted by [`crate::GroupCoordinator`] and
/// [`crate::ConsumerGroup`].
#[derive(Debug, Error)]
pub enum GroupError {
    /// The group name (or consumer id) failed validation. Names must
    /// be 1..=255 bytes of UTF-8 with no NUL or whitespace.
    #[error("invalid name: {reason}")]
    InvalidName {
        /// Human-readable reason.
        reason: &'static str,
    },

    /// A partition index was passed that is out of range for the
    /// topic's declared partition count.
    #[error("partition {requested} out of range [0..{partition_count})")]
    PartitionOutOfRange {
        /// The partition the caller asked for.
        requested: u32,
        /// Number of partitions the topic actually has.
        partition_count: u32,
    },

    /// A member tried to heartbeat or commit with a stale generation.
    /// The current generation is included so the caller can refresh
    /// and retry.
    #[error("stale generation {provided}; current is {current}")]
    StaleGeneration {
        /// The generation the caller supplied.
        provided: u32,
        /// The group's current generation.
        current: u32,
    },

    /// An operation targeted a group/topic that has no registered
    /// members.
    #[error("group {group} is empty")]
    EmptyGroup {
        /// Group id.
        group: String,
    },

    /// An operation required the partition count to be ≥ 1.
    #[error("partition count must be ≥ 1 (got {0})")]
    ZeroPartitions(u32),
}

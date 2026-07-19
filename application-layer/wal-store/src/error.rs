//! Errors returned by the wal-store crate.

use thiserror::Error;

/// All errors emitted by [`crate::WalStore`] implementations and the
/// supporting types.
#[derive(Debug, Error)]
pub enum WalError {
    /// The topic name was empty, contained characters disallowed by the
    /// filesystem layout (path separators, NUL), or exceeded the
    /// configured maximum length.
    #[error("invalid topic name: {reason}")]
    InvalidTopic {
        /// Human-readable reason.
        reason: &'static str,
    },

    /// An offset was passed that does not correspond to any retained
    /// entry. The caller is told the current valid range so it can
    /// retry with `oldest..=head`.
    #[error("offset {requested} out of range [oldest={oldest}, head={head}]")]
    OffsetOutOfRange {
        /// The offset the caller asked for.
        requested: u64,
        /// Oldest retained offset for the topic.
        oldest: u64,
        /// Newest appended offset for the topic (= last entry's offset).
        head: u64,
    },

    /// The topic has never been appended to. Returned when a caller asks
    /// for a range or offset on a topic the store has no record of.
    #[error("topic {topic:?} is empty or unknown")]
    TopicEmpty {
        /// Topic name.
        topic: String,
    },

    /// I/O error from the file backend. Wraps [`std::io::Error`].
    #[error("wal i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// Entry wire format was malformed — wrong magic, truncated header,
    /// or unsupported version. Surfaces during segment scan on open.
    #[error("malformed wal entry: {reason}")]
    MalformedEntry {
        /// Human-readable reason.
        reason: &'static str,
    },
}

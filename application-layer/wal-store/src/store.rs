//! [`WalStore`] — the backend-agnostic surface consumed by the
//! pub/sub engine.
//!
//! Two reference implementations live alongside this trait:
//!
//! * [`crate::MemoryStore`] — fully in-memory, intended for tests and
//!   ephemeral workloads.
//! * [`crate::FileStore`] — real persistence with rotating segments.
//!
//! Both implementations are single-threaded by design — Vireon is a
//! thread-per-core runtime, and each reactor owns its own WAL. The
//! trait is therefore `!Sync`-friendly (no `&self` locks required for
//! the hot path).

use crate::entry::{WalEntry, WalRecord};
use crate::error::WalError;
use crate::offset::Offset;

/// Append-only write-ahead log keyed by topic.
///
/// # Topic naming rules
///
/// Backends validate topic names on every call via
/// [`crate::error::WalError::InvalidTopic`]. The rules are:
///
/// * 1..=255 bytes once UTF-8 encoded.
/// * No path separators (`/`, `\`).
/// * No NUL bytes.
/// * No leading or trailing whitespace.
///
/// These rules keep the file backend's directory layout simple — every
/// accepted topic name maps to a single filesystem path component.
pub trait WalStore {
    /// Append `record` to the log for `topic`. Returns the offset the
    /// store assigned.
    ///
    /// Errors:
    /// * [`WalError::InvalidTopic`] — name failed validation.
    /// * [`WalError::Io`] — backing store write failed.
    fn append(&mut self, topic: &str, record: WalRecord) -> Result<Offset, WalError>;

    /// Read a single entry at `offset`. Returns
    /// [`WalError::OffsetOutOfRange`] when the offset is below the
    /// oldest retained entry or above the current head.
    fn read(&self, topic: &str, offset: Offset) -> Result<WalEntry, WalError>;

    /// Read entries in `[from, until)` and append them to `out` in
    /// ascending offset order. `until` of `Offset::NONE` means "to the
    /// current head inclusive".
    ///
    /// Returns the number of entries appended.
    fn read_range(
        &self,
        topic: &str,
        from: Offset,
        until: Offset,
        out: &mut Vec<WalEntry>,
    ) -> Result<usize, WalError>;

    /// Oldest retained offset for `topic`. `Offset::NONE` if the topic
    /// has never been appended to.
    fn oldest(&self, topic: &str) -> Offset;

    /// Offset of the most-recently appended entry for `topic` — i.e.
    /// the offset that will be assigned to the *next* append minus one.
    /// `Offset::NONE` if the topic is empty.
    fn head(&self, topic: &str) -> Offset;

    /// Number of retained entries for `topic`.
    fn len(&self, topic: &str) -> usize;

    /// `true` when the topic has zero retained entries.
    fn is_empty(&self, topic: &str) -> bool {
        self.len(topic) == 0
    }

    /// Delete every entry whose offset is strictly less than
    /// `before_offset`. Backends advance their `oldest` cursor
    /// accordingly; subsequent `read` calls for the deleted offsets
    /// return [`WalError::OffsetOutOfRange`].
    ///
    /// Returns the number of entries removed.
    fn truncate(&mut self, topic: &str, before_offset: Offset) -> Result<usize, WalError>;

    /// Total bytes currently retained across all topics. Used by the
    /// retention sweeper to decide whether to fire.
    fn retained_bytes(&self) -> u64;
}

/// Validate a topic name. Backends call this on every public API entry
/// point so the rules stay in lockstep across implementations.
pub(crate) fn validate_topic(topic: &str) -> Result<(), WalError> {
    if topic.is_empty() {
        return Err(WalError::InvalidTopic {
            reason: "topic name is empty",
        });
    }
    if topic.len() > 255 {
        return Err(WalError::InvalidTopic {
            reason: "topic name exceeds 255 bytes",
        });
    }
    if topic.starts_with(' ') || topic.ends_with(' ') {
        return Err(WalError::InvalidTopic {
            reason: "topic name has leading or trailing whitespace",
        });
    }
    for b in topic.bytes() {
        if b == b'/' || b == b'\\' || b == 0 {
            return Err(WalError::InvalidTopic {
                reason: "topic name contains a path separator or NUL",
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_topics() {
        assert!(validate_topic("sensor.temp").is_ok());
        assert!(validate_topic("a").is_ok());
        assert!(validate_topic("with.dots.and-dashes").is_ok());
    }

    #[test]
    fn invalid_topics() {
        assert!(validate_topic("").is_err());
        assert!(validate_topic("path/sep").is_err());
        assert!(validate_topic("back\\slash").is_err());
        assert!(validate_topic("with\0nul").is_err());
        assert!(validate_topic(" leading").is_err());
        assert!(validate_topic("trailing ").is_err());
        let long = "a".repeat(256);
        assert!(validate_topic(&long).is_err());
    }
}

//! [`ShardError`] — error types (Layer 1).
//!
//! All errors produced by the connection-shard module. The enum is
//! `#[non_exhaustive]` to allow future variants without breaking
//! downstream matches.

use crate::backpressure::BackpressureLevel;
use crate::state::ConnectionState;
use thiserror::Error;

/// Errors returned by [`ConnectionShard`](crate::shard::ConnectionShard) operations.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ShardError {
    /// Configuration validation failed (capacity, thresholds, etc.).
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// The slab has no free slots.
    #[error("shard is full (capacity {capacity})")]
    ShardFull { capacity: usize },

    /// The connection id was not found or has a stale generation.
    ///
    /// This is returned when a caller passes an id whose slot has been
    /// recycled and now holds a different generation.
    #[error("connection not found or stale generation")]
    NotFound,

    /// The requested state transition is not valid for the current state.
    #[error("illegal state transition: {from:?} -> {to:?}")]
    IllegalTransition {
        from: ConnectionState,
        to: ConnectionState,
    },

    /// A connection with the same `stream_id` already exists in this shard.
    #[error("stream_id {0} already exists in this shard")]
    DuplicateStream(u64),

    /// A migration was requested for a connection that is already migrating.
    #[error("migration conflict: connection {0} already migrating")]
    MigrationConflict(u64),

    /// Connection creation rejected due to backpressure.
    #[error("connection rejected by backpressure (level {level:?})")]
    RejectedByBackpressure { level: BackpressureLevel },

    /// The shard is shutting down and cannot accept new operations.
    #[error("shard is shutting down")]
    ShuttingDown,
}

impl ShardError {
    /// Whether this error represents a structural problem that cannot be
    /// recovered from by retrying.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::InvalidConfig(_) | Self::ShuttingDown)
    }

    /// Whether this error is transient and the operation may succeed if
    /// retried after a short delay.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::ShardFull { .. } | Self::RejectedByBackpressure { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_config_is_fatal() {
        assert!(ShardError::InvalidConfig("test".into()).is_fatal());
        assert!(ShardError::ShuttingDown.is_fatal());
    }

    #[test]
    fn shard_full_is_retryable() {
        assert!(ShardError::ShardFull { capacity: 1000 }.is_retryable());
    }

    #[test]
    fn rejected_by_bp_is_retryable() {
        assert!(
            ShardError::RejectedByBackpressure {
                level: BackpressureLevel::Hard
            }
            .is_retryable()
        );
    }

    #[test]
    fn not_found_is_neither() {
        let e = ShardError::NotFound;
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn illegal_transition_is_neither() {
        let e = ShardError::IllegalTransition {
            from: ConnectionState::Closed,
            to: ConnectionState::Active,
        };
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn duplicate_stream_display() {
        let e = ShardError::DuplicateStream(42);
        assert!(format!("{e}").contains("42"));
    }
}

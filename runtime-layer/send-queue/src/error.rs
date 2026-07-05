//! Typed errors for the `send-queue` crate (Layer 1).
//!
//! Every fallible public operation returns [`SendQueueError`]; no site in
//! non-test code panics (per workspace lint rules).

/// Errors returned by [`SendQueue`](crate::SendQueue) operations.
#[derive(Debug, thiserror::Error)]
pub enum SendQueueError {
    /// The ingress ring is at capacity and cannot accept another packet.
    ///
    /// The caller should shed load, retry with backoff, or raise
    /// `capacity`.
    #[error("send queue is full (capacity {capacity})")]
    QueueFull {
        /// Configured ring capacity.
        capacity: usize,
    },

    /// The supplied configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// A packet id did not resolve to a live slot (already reclaimed,
    /// cancelled, or stale generation).
    #[error("packet id {0} is unknown or already reclaimed")]
    UnknownPacket(u64),

    /// The queue has been shut down and refuses new submissions.
    #[error("send queue is shutting down")]
    ShuttingDown,

    /// The underlying submission sink rejected the batch.
    #[error("submission sink error: {0}")]
    SubmitFailed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_full_formats_capacity() {
        let err = SendQueueError::QueueFull { capacity: 4096 };
        assert_eq!(err.to_string(), "send queue is full (capacity 4096)");
    }

    #[test]
    fn invalid_config_carries_context() {
        let err = SendQueueError::InvalidConfig("capacity must be > 0".into());
        assert_eq!(
            err.to_string(),
            "invalid configuration: capacity must be > 0"
        );
    }

    #[test]
    fn unknown_packet_formats_id() {
        let err = SendQueueError::UnknownPacket(42);
        assert_eq!(
            err.to_string(),
            "packet id 42 is unknown or already reclaimed"
        );
    }

    #[test]
    fn shutting_down_is_static() {
        assert_eq!(
            SendQueueError::ShuttingDown.to_string(),
            "send queue is shutting down"
        );
    }

    #[test]
    fn submit_failed_carries_context() {
        let err = SendQueueError::SubmitFailed("SQ ring full".into());
        assert_eq!(err.to_string(), "submission sink error: SQ ring full");
    }
}

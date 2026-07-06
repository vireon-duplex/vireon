//! [`StreamManagerError`] — unified error surface for the stream
//! manager.
//!
//! See `DESIGN.md` Part 12 (failure handling).

use thiserror::Error;

/// Unified stream-manager error.
///
/// `#[non_exhaustive]` reserves the right to add new variants in
/// future phases without breaking match arms.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum StreamManagerError {
    /// Configuration validation failed.
    #[error("invalid stream-manager configuration: {0}")]
    InvalidConfig(String),

    /// No stream with the given id is known to this manager.
    #[error("stream {0} not found")]
    StreamNotFound(u64),

    /// A stream with the given id already exists.
    #[error("stream {0} already exists")]
    StreamExists(u64),

    /// Stream is closed (SEND / RECV half-closed or fully closed).
    #[error("stream {0} is closed")]
    StreamClosed(u64),

    /// Stream count budget exhausted (RFC 9000 §4.6: MAX_STREAMS).
    #[error("stream count exhausted (type={0:?})")]
    StreamExhausted(crate::id::StreamType),

    /// Peer violated flow-control credit (RFC 9000 §4.5).
    /// Always a connection-level error per the RFC.
    #[error("flow control violation on stream {0}: {1}")]
    FlowControlViolation(u64, &'static str),

    /// Stream id is malformed / out of band for this peer.
    #[error("invalid stream id {0}")]
    InvalidStreamId(u64),

    /// Slab / table / out-of-memory.
    #[error("resource exhausted: {0}")]
    ResourceExhausted(&'static str),

    /// Manager is shutting down; no new streams.
    #[error("stream-manager is shutting down")]
    ShuttingDown,
}

impl StreamManagerError {
    /// Fatal errors abort the connection (per RFC 9000 §11,
    /// `APPLICATION_ERROR` or transport-level close).
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::InvalidConfig(_)
                | Self::FlowControlViolation(_, _)
                | Self::InvalidStreamId(_)
                | Self::ResourceExhausted(_)
                | Self::ShuttingDown
        )
    }

    /// Retryable errors are logged and the caller continues.
    /// Examples: `StreamNotFound` (retry with create), `StreamClosed`
    /// (return to caller as "no more data").
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        !self.is_fatal()
    }

    /// Whether the error indicates a peer protocol violation
    /// (RFC 9000 §11, `STREAM_STATE_ERROR` / `FLOW_CONTROL_ERROR` /
    /// `FRAME_ENCODING_ERROR`).
    #[must_use]
    pub const fn is_protocol_violation(&self) -> bool {
        matches!(
            self,
            Self::FlowControlViolation(_, _) | Self::InvalidStreamId(_)
        )
    }

    /// The RFC 9000 §20 error code that should be signalled to the
    /// peer if this error causes a CONNECTION_CLOSE. Returns `None`
    /// for purely local errors (no peer-visible signal).
    #[must_use]
    pub const fn rfc9000_error_code(&self) -> Option<u64> {
        match self {
            // No transport error for config / shutdown — these are local.
            Self::InvalidConfig(_) | Self::ShuttingDown | Self::ResourceExhausted(_) => None,
            // Peer sent data on a stream we don't have open.
            Self::StreamNotFound(_) => Some(0x0a), // STREAM_STATE_ERROR
            // Local tried to create a stream already present.
            Self::StreamExists(_) => None,
            // Local tried to use a closed stream.
            Self::StreamClosed(_) => Some(0x0a), // STREAM_STATE_ERROR
            // Local exhausted its stream budget.
            Self::StreamExhausted(_) => None,
            // Peer exceeded flow-control credit.
            Self::FlowControlViolation(_, _) => Some(0x03), // FLOW_CONTROL_ERROR
            // Peer's stream id is malformed.
            Self::InvalidStreamId(_) => Some(0x02), // STREAM_STATE_ERROR per impl — see DESIGN.md Part 12
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::StreamType;

    #[test]
    fn classify_fatal() {
        assert!(StreamManagerError::ShuttingDown.is_fatal());
        assert!(StreamManagerError::InvalidStreamId(99).is_fatal());
        assert!(StreamManagerError::FlowControlViolation(7, "credit exceeded").is_fatal());
    }

    #[test]
    fn classify_retryable() {
        assert!(StreamManagerError::StreamNotFound(7).is_retryable());
        assert!(StreamManagerError::StreamClosed(7).is_retryable());
        assert!(StreamManagerError::StreamExhausted(StreamType::BidiClient).is_retryable());
    }

    #[test]
    fn protocol_violations() {
        assert!(StreamManagerError::FlowControlViolation(1, "x").is_protocol_violation());
        assert!(StreamManagerError::InvalidStreamId(1).is_protocol_violation());
        assert!(!StreamManagerError::StreamNotFound(1).is_protocol_violation());
    }

    #[test]
    fn rfc9000_error_code_for_flow_control() {
        let e = StreamManagerError::FlowControlViolation(7, "exceeded");
        assert_eq!(e.rfc9000_error_code(), Some(0x03));
    }

    #[test]
    fn rfc9000_error_code_for_local_only() {
        assert_eq!(StreamManagerError::ShuttingDown.rfc9000_error_code(), None);
        assert_eq!(
            StreamManagerError::InvalidConfig("x".into()).rfc9000_error_code(),
            None
        );
    }

    #[test]
    fn display_includes_id() {
        let e = StreamManagerError::StreamNotFound(42);
        let s = format!("{e}");
        assert!(s.contains('4') && s.contains('2'));
    }
}

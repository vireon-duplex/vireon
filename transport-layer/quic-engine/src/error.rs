//! [`QuicError`] — error surface for the QUIC engine.
//!
//! Mirrors the pattern established by `connection_shard::ShardError`:
//! a `#[non_exhaustive]` thiserror enum with `is_fatal()` and
//! `is_retryable()` predicates so callers can decide whether to drop
//! a connection, throttle, or surface the failure to the operator.
//!
//! See `DESIGN.md` §8 (Failure Handling & DOS Resistance) for the
//! full taxonomy and recovery policy per variant.

use crate::state::QuicConnState;

/// Errors emitted by the QUIC engine.
///
/// `#[non_exhaustive]` allows new variants to be added in future
/// phases without breaking downstream `match` arms.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum QuicError {
    /// Configuration validation failed (capacity out of range,
    /// conflicting presets, malformed peer address, etc.).
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// `quiche` returned an error from packet processing, stream
    /// I/O, or connection management. Always recoverable at the
    /// connection level unless it maps to `Done` (which is not an
    /// error in our model — see `WouldBlock` below).
    #[error("quiche error: {0}")]
    Quiche(#[from] quiche::Error),

    /// No connection registered for the given CID. Caused by either
    /// a routing race (post-migration) or a stateless-reset / garbage
    /// packet from an unknown peer.
    #[error("connection not found for CID {0:?}")]
    ConnectionNotFound(Vec<u8>),

    /// A new connection arrived with a CID already in the table.
    /// Should be impossible given CID length / random entropy; a
    /// sighting indicates either a bug in CID minting or a
    /// malicious peer.
    #[error("CID collision: {0:?}")]
    CidCollision(Vec<u8>),

    /// The connection was asked to transition between two states
    /// that have no legal path in the [`QuicConnState`] machine.
    #[error("illegal state transition: {from:?} -> {to:?}")]
    IllegalTransition {
        from: QuicConnState,
        to: QuicConnState,
    },

    /// Packet failed structural validation (truncated header,
    /// unsupported version, bad header-protection sample,
    /// inconsistent length). Per RFC 9000 §17, the packet MUST be
    /// dropped silently.
    #[error("packet malformed")]
    MalformedPacket,

    /// Long-header packet from a peer requesting a version we do
    /// not support. RFC 9000 §17.2.1 — a Version Negotiation packet
    /// MUST be sent in response.
    #[error("version negotiation required")]
    VersionNegotiation,

    /// Initial packet arrived without address validation; a Retry
    /// packet MUST be sent (RFC 9000 §17.2.5.2).
    #[error("retry required")]
    RetryRequired,

    /// Sending the next packet would exceed the 3× anti-amplification
    /// limit on bytes sent to an unvalidated peer (RFC 9000 §8.1).
    #[error("anti-amplification limit hit")]
    AntiAmplification,

    /// The engine rejected work due to resource exhaustion (slab
    /// full, buffer pool dry, too many concurrent streams, etc.).
    /// Always retryable after backoff.
    #[error("resource exhausted: {0}")]
    ResourceExhausted(&'static str),

    /// The engine has been told to shut down; no new connections
    /// or packets will be processed.
    #[error("engine is shutting down")]
    ShuttingDown,
}

impl QuicError {
    /// Whether the error indicates a fatal, non-recoverable
    /// condition. Fatal errors should terminate the engine (not
    /// just the offending connection).
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::InvalidConfig(_) | Self::ShuttingDown)
    }

    /// Whether the caller may legitimately retry the operation
    /// after a backoff. Non-retryable errors should be reported and
    /// the offending connection torn down.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::ResourceExhausted(_) | Self::AntiAmplification)
    }

    /// Whether the error resulted from a packet that should be
    /// silently dropped per the RFC (no response, no log beyond
    /// debug). Used by the packet router to suppress noisy logs
    /// during scanning / port-sweep traffic.
    #[must_use]
    pub const fn is_silent_drop(&self) -> bool {
        matches!(self, Self::ConnectionNotFound(_) | Self::MalformedPacket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_config_is_fatal() {
        assert!(QuicError::InvalidConfig("x".into()).is_fatal());
        assert!(!QuicError::InvalidConfig("x".into()).is_retryable());
    }

    #[test]
    fn exhausted_is_retryable() {
        assert!(QuicError::ResourceExhausted("slab").is_retryable());
        assert!(!QuicError::ResourceExhausted("slab").is_fatal());
    }

    #[test]
    fn connection_not_found_is_silent_drop() {
        assert!(QuicError::ConnectionNotFound(vec![1, 2, 3]).is_silent_drop());
        assert!(QuicError::MalformedPacket.is_silent_drop());
    }

    #[test]
    fn shutting_down_is_fatal() {
        assert!(QuicError::ShuttingDown.is_fatal());
    }
}

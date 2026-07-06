//! [`StreamEvent`] — events the Stream Manager emits to the
//! application / session layer.
//!
//! See `DESIGN.md` Part 1 (responsibilities) and Part 15
//! (observability).
//!
//! The Stream Manager is the single source of truth for "what
//! happened to this stream". It emits a typed event whenever the
//! application needs to react (data arrived, peer reset, flow
//! control unblocked, …). Wire-level frames (STREAM,
//! RESET_STREAM, MAX_STREAM_DATA, STOP_SENDING) arrive from the
//! transport layer and are translated into these high-level
//! events.
//!
//! # Phase 0 status
//!
//! Phase 0 ships the event taxonomy. Functional emission (queueing
//! to the session layer's `mpsc`) lands in Phase 1 once
//! [`crate::manager::StreamManager`] is wired up.

use crate::id::StreamId;
use crate::state::StreamState;

/// Reason the peer or local side closed a stream. Carried on
/// [`StreamEvent::Closed`] and surfaced to the application.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseReason {
    /// FIN sent / received; clean close.
    Fin,
    /// `RESET_STREAM` received from the peer.
    PeerReset(u64),
    /// `RESET_STREAM` sent (locally initiated close).
    LocalReset,
    /// `STOP_SENDING` received (peer asked us to stop).
    StopSending(u64),
    /// Idle timeout fired (RFC 9000 §3.3 / §10.2).
    IdleTimeout,
    /// Connection-closing error cascaded down to the stream.
    ConnectionError,
}

/// An event on a stream, emitted to the application layer.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamEvent {
    /// A new stream was opened (locally or by the peer).
    Created {
        /// The new stream's id.
        id: StreamId,
    },
    /// The send half is ready for more bytes (flow-control credit
    /// arrived, or prior bytes were ack'd).
    ReadySend {
        /// The stream that became ready.
        id: StreamId,
        /// New send credit (bytes the peer will accept).
        credit: u64,
    },
    /// The send half is blocked on flow control.
    BlockedSend {
        /// The stream that is blocked.
        id: StreamId,
        /// Credit at the time of block.
        available_credit: u64,
    },
    /// Data arrived on the recv half (contiguous bytes ready to
    /// deliver).
    RecvData {
        /// The stream with data.
        id: StreamId,
        /// Number of contiguous bytes available.
        len: u64,
    },
    /// FIN received; no more data will arrive.
    FinReceived {
        /// The stream that signalled FIN.
        id: StreamId,
        /// Final stream size (total bytes received).
        final_size: u64,
    },
    /// Peer sent RESET_STREAM or STOP_SENDING.
    ResetReceived {
        /// The stream that was reset.
        id: StreamId,
        /// Application error code from the peer.
        error_code: u64,
        /// Final size declared by the peer.
        final_size: u64,
    },
    /// The stream has fully closed (both halves terminal).
    Closed {
        /// The stream that closed.
        id: StreamId,
        /// Why it closed.
        reason: CloseReason,
        /// Combined state at close time (for diagnostics).
        final_state: StreamState,
    },
}

impl StreamEvent {
    /// The stream id this event refers to.
    #[must_use]
    pub const fn stream_id(&self) -> StreamId {
        match self {
            Self::Created { id }
            | Self::ReadySend { id, .. }
            | Self::BlockedSend { id, .. }
            | Self::RecvData { id, .. }
            | Self::FinReceived { id, .. }
            | Self::ResetReceived { id, .. }
            | Self::Closed { id, .. } => *id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_id_extractor() {
        let id = StreamId::new_unchecked(7);
        assert_eq!(StreamEvent::Created { id }.stream_id(), id);
        assert_eq!(StreamEvent::RecvData { id, len: 100 }.stream_id(), id);
        assert_eq!(
            StreamEvent::Closed {
                id,
                reason: CloseReason::Fin,
                final_state: StreamState::Closed
            }
            .stream_id(),
            id
        );
    }

    #[test]
    fn close_reason_variants() {
        assert_eq!(CloseReason::Fin, CloseReason::Fin);
        assert_ne!(CloseReason::PeerReset(1), CloseReason::PeerReset(2));
    }
}

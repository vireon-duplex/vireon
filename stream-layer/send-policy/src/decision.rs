//! Outcome types produced by [`SendPolicyTable::enqueue`](crate::SendPolicyTable::enqueue)
//! and [`SendPolicyTable::dequeue`](crate::SendPolicyTable::dequeue).

use bytes::Bytes;
use frame::header::{FrameFlags, MessageType};

/// Owned payload bytes plus the parts of the frame header the scheduler
/// needs in order to emit a wire-format frame when the payload leaves the
/// send queue.
///
/// Mirrors `delivery_policy::FramePayload` so callers that already build
/// a [`FramePayload`] for the receive side can reuse the same construction
/// site (although the two types are intentionally distinct — receive-side
/// payloads carry `quic_stream_id` for re-emission through `FrameReceived`,
/// while send-side payloads carry the data the scheduler will turn into
/// outgoing bytes).
#[derive(Debug, Clone)]
pub struct FramePayload {
    /// Original `MessageType` of the data frame. Preserved so the
    /// scheduler can encode the outgoing frame with the correct type
    /// rather than a placeholder.
    pub msg_type: MessageType,
    /// Original `FrameFlags` of the data frame.
    pub flags: FrameFlags,
    /// Payload bytes.
    pub bytes: Bytes,
}

impl FramePayload {
    /// Construct from the three pieces.
    #[inline]
    #[must_use]
    pub fn new(msg_type: MessageType, flags: FrameFlags, bytes: Bytes) -> Self {
        Self {
            msg_type,
            flags,
            bytes,
        }
    }
}

/// What the policy decided for a frame the caller asked to enqueue.
///
/// Returned by [`SendPolicyTable::enqueue`](crate::SendPolicyTable::enqueue).
/// On `Queue` the caller has nothing further to do — the bytes are now
/// owned by the table and will leave via `dequeue`. On `Drop`, the caller
/// may wish to log or count; on `Replace`, the prior pending payload is
/// returned to the caller so it can be freed outside the table's hot path
/// (useful for pool recycling).
#[derive(Debug)]
pub enum EnqueueDecision {
    /// Frame was appended to the per-stream queue. Standard outcome for
    /// `ReliableOrdered`, `ReliableUnordered`, fresh `RealtimeDropOld`,
    /// and the first `LatestOnly` frame on an idle stream.
    Queue,

    /// Frame replaced a still-pending older payload. Only produced by
    /// `LatestOnly`. The displaced payload is handed back to the caller
    /// so any pool slot can be recycled — the table no longer references
    /// it.
    Replace(Box<FramePayload>),

    /// Frame was discarded without being stored. Produced by
    /// `RealtimeDropOld` (older than the newest already queued) and by
    /// `LatestOnly` (older than the pending slot). The payload is dropped
    /// and its refcount freed as part of the call.
    Drop,
}

/// What [`SendPolicyTable::dequeue`](crate::SendPolicyTable::dequeue)
/// returns for one stream.
#[derive(Debug)]
pub enum DequeueOutcome {
    /// A frame was emitted. The payload is now owned by the caller.
    Some(FramePayload),

    /// The stream's queue is empty (or, for `LatestOnly`, the pending slot
    /// is vacant). The caller should not call `dequeue` again on this
    /// stream until a subsequent `enqueue` reports `Queue`.
    Empty,

    /// The stream is unknown to the table — either never bound, or
    /// already removed via `remove_stream` / `remove_connection`.
    ///
    /// Surfaced as a distinct variant rather than collapsing into `Empty`
    /// so the scheduler can distinguish "no work" from "stream gone" and
    /// clean up its ready set accordingly.
    Unknown,
}

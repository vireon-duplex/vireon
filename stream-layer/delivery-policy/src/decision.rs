//! Outcome types produced by [`DeliveryPolicyTable::apply`](crate::DeliveryPolicyTable::apply).

use bytes::Bytes;
use frame::header::{FrameFlags, MessageType};

/// Owned payload bytes plus the parts of the frame header the engine
/// needs to reconstruct a `FrameReceived` event when the frame is later
/// drained out of a reorder / pending buffer.
///
/// Stored as `Bytes` (not `buffer_pool::Buffer`) because the pool guard
/// is not `Clone` and recycles on drop — it cannot be retained across the
/// buffer/reorder window. The engine converts its pool buffer to `Bytes`
/// once via [`buffer_pool::Buffer::into_bytes`] before invoking the policy.
#[derive(Debug, Clone)]
pub struct FramePayload {
    /// Original `MessageType` of the data frame. Preserved so drained
    /// frames can be emitted with the correct type (not the placeholder
    /// type the engine would otherwise have to invent).
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

/// What the policy decided for an incoming frame.
///
/// Returned by [`DeliveryPolicyTable::apply`](crate::DeliveryPolicyTable::apply).
/// The caller is responsible for matching on this and either emitting a
/// `QuicEvent::FrameReceived` (`Deliver`) or doing nothing (`Buffer` / `Drop`).
#[derive(Debug)]
pub enum Decision {
    /// Forward the payload to the upper layer immediately.
    ///
    /// Ownership of the payload is transferred back to the caller — the
    /// policy no longer holds any reference to it.
    Deliver(FramePayload),

    /// Hold the frame inside the policy's reorder / pending buffer.
    ///
    /// The caller must **not** emit a `FrameReceived` event. The bytes are
    /// now owned by the policy and will resurface via
    /// [`DeliveryPolicyTable::drain_ready_for_connection`](crate::DeliveryPolicyTable::drain_ready_for_connection)
    /// once they become deliverable.
    Buffer,

    /// Discard the frame silently.
    ///
    /// Reasons include: stale sequence (older than `last_delivered`),
    /// duplicate sequence, or displaced by a newer frame under
    /// [`LatestOnly`](crate::DeliveryPolicy::LatestOnly). The payload is
    /// dropped and its refcount freed.
    Drop,
}

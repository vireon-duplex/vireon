//! [`PacketSlot`] — the per-packet record stored in the ingress ring.
//!
//! Each slot is cache-aligned (`#[repr(C, align(64))]`) to prevent false
//! sharing between adjacent slots during concurrent producer/consumer
//! access.

use std::net::SocketAddr;

/// Per-packet state within the send-queue lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SlotState {
    /// Slot is empty; available for reuse.
    #[default]
    Empty,
    /// Packet is in the ingress ring, waiting to be scheduled.
    Queued,
    /// Packet has been submitted to the IO Layer; awaiting `Send` CQE.
    InFlight,
    /// `Send` CQE received; awaiting `SendBufferReclaim` CQE.
    Accepted,
    /// Packet is being retried; re-queued for transmission.
    Retrying,
}

/// A single packet record stored in the ingress ring.
///
/// The slot holds metadata about the packet (destination, priority,
/// deadline, stream id) plus the registered-buffer index and payload
/// length that the IO Layer needs to submit the `SEND_ZC` SQE.
///
/// `tracking_id` is the encoded `(tracking_slot, generation)` pair
/// assigned by the send queue at submit time. It uniquely identifies
/// the packet through the two-CQE lifecycle and is passed as `user_data`
/// to the IO Layer.
#[repr(C, align(64))]
#[derive(Clone, Debug)]
pub struct PacketSlot {
    /// Encoded `(tracking_slot, generation)` — the stable packet id used
    /// for CQE matching.
    pub tracking_id: u64,
    /// io_uring registered-buffer index (for `SEND_ZC` `buf_index`).
    pub buf_index: u16,
    /// Payload length in bytes within the registered buffer.
    pub len: u32,
    /// Destination address.
    pub dest: SocketAddr,
    /// Application-supplied priority (lower = higher priority).
    pub priority: u8,
    /// Absolute deadline in monotonic nanos. `0` means "no deadline".
    pub deadline_ns: u64,
    /// Application stream / frame id (for WFQ and retry correlation).
    pub stream_id: u64,
    /// Current lifecycle state.
    pub state: SlotState,
}

impl PacketSlot {
    /// Creates a new empty slot with zero tracking id.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            tracking_id: 0,
            buf_index: 0,
            len: 0,
            dest: SocketAddr::from(([0, 0, 0, 0], 0)),
            priority: 0,
            deadline_ns: 0,
            stream_id: 0,
            state: SlotState::Empty,
        }
    }

    /// Fills the slot with packet data and transitions to `Queued`.
    ///
    /// `tracking_id` is the encoded packet identifier from the send
    /// queue's tracking slot allocator.
    pub fn fill(
        &mut self,
        tracking_id: u64,
        buf_index: u16,
        len: u32,
        dest: SocketAddr,
        priority: u8,
        deadline_ns: u64,
        stream_id: u64,
    ) {
        self.tracking_id = tracking_id;
        self.buf_index = buf_index;
        self.len = len;
        self.dest = dest;
        self.priority = priority;
        self.deadline_ns = deadline_ns;
        self.stream_id = stream_id;
        self.state = SlotState::Queued;
    }

    /// Resets the slot to `Empty`.
    pub fn clear(&mut self) {
        self.state = SlotState::Empty;
        self.len = 0;
        self.tracking_id = 0;
    }
}

impl Default for PacketSlot {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_slot_has_empty_state() {
        let slot = PacketSlot::empty();
        assert_eq!(slot.state, SlotState::Empty);
        assert_eq!(slot.tracking_id, 0);
    }

    #[test]
    fn fill_transitions_to_queued() {
        let mut slot = PacketSlot::empty();
        slot.fill(
            42,
            7,
            1024,
            "10.0.0.1:8080".parse().unwrap(),
            3,
            1_000_000,
            99,
        );
        assert_eq!(slot.state, SlotState::Queued);
        assert_eq!(slot.tracking_id, 42);
        assert_eq!(slot.buf_index, 7);
        assert_eq!(slot.len, 1024);
        assert_eq!(slot.priority, 3);
        assert_eq!(slot.stream_id, 99);
    }

    #[test]
    fn fill_after_clear_uses_new_tracking_id() {
        let mut slot = PacketSlot::empty();
        slot.fill(10, 1, 64, "0.0.0.0:0".parse().unwrap(), 0, 0, 0);
        slot.clear();
        assert_eq!(slot.tracking_id, 0);
        slot.fill(20, 2, 128, "0.0.0.0:0".parse().unwrap(), 0, 0, 1);
        assert_eq!(slot.tracking_id, 20);
        assert_eq!(slot.buf_index, 2);
    }

    #[test]
    fn clear_resets_state_and_len() {
        let mut slot = PacketSlot::empty();
        slot.fill(5, 5, 512, "10.0.0.1:80".parse().unwrap(), 0, 0, 0);
        slot.clear();
        assert_eq!(slot.state, SlotState::Empty);
        assert_eq!(slot.len, 0);
        assert_eq!(slot.tracking_id, 0);
    }
}

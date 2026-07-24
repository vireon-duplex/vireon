//! Per-stream send-policy state — one enum variant per [`DeliveryPolicy`].
//!
//! Mirrors `delivery_policy::state` but flipped to the egress side. The
//! variants are narrower than the receive-side ones because send-side
//! frames do not need gap-fill (the sender is the source of order, so it
//! always knows what to emit next). Each variant only needs to answer:
//!
//! * [`PerStreamSendState::enqueue`] — what to do with a frame the caller
//!   just handed us.
//! * [`PerStreamSendState::dequeue`] — what to hand back when the
//!   scheduler asks for the next frame to send.
//! * [`PerStreamSendState::flush_for_upgrade`] — empty the state so the
//!   table can `mem::replace` it on a re-bind.

use crate::decision::{DequeueOutcome, EnqueueDecision, FramePayload};
use crate::queue::StreamQueue;
use delivery_policy::DeliveryPolicy;

/// The per-stream send-policy state.
///
/// Constructed by [`SendPolicyTable::bind`](crate::SendPolicyTable::bind)
/// and never re-typed after creation — upgrades happen by `mem::replace`
/// at the table level (see [`Self::flush_for_upgrade`]).
#[derive(Debug)]
pub enum PerStreamSendState {
    /// FIFO egress queue. Used for both `ReliableOrdered` and
    /// `ReliableUnordered` — they differ only in receive-side semantics.
    Fifo(FifoState),
    /// Drop stale frames before they enter the queue.
    RealtimeDropOld(RealtimeDropOldState),
    /// Single-slot pending frame. Older payloads are displaced.
    LatestOnly(LatestOnlyState),
}

impl PerStreamSendState {
    /// Construct an empty state for the given policy.
    #[inline]
    #[must_use]
    pub fn new(policy: DeliveryPolicy) -> Self {
        match policy {
            DeliveryPolicy::ReliableOrdered | DeliveryPolicy::ReliableUnordered => {
                Self::Fifo(FifoState::new())
            }
            DeliveryPolicy::RealtimeDropOld => Self::RealtimeDropOld(RealtimeDropOldState::new()),
            DeliveryPolicy::LatestOnly => Self::LatestOnly(LatestOnlyState::new()),
        }
    }

    /// Which policy this state is configured for. Returns
    /// [`DeliveryPolicy::ReliableOrdered`] for both Fifo-backed variants
    /// — callers that need to distinguish `ReliableUnordered` from
    /// `ReliableOrdered` should track that out-of-band, since the
    /// send-side storage for the two is identical.
    #[inline]
    #[must_use]
    pub fn policy(&self) -> DeliveryPolicy {
        match self {
            Self::Fifo(_) => DeliveryPolicy::ReliableOrdered,
            Self::RealtimeDropOld(_) => DeliveryPolicy::RealtimeDropOld,
            Self::LatestOnly(_) => DeliveryPolicy::LatestOnly,
        }
    }

    /// Apply the policy to one outgoing frame. Consumes `payload`.
    ///
    /// The frame's `seq` lives inside `payload` so it survives any
    /// send-side reordering or drop and reaches the receiver's policy
    /// unchanged.
    ///
    /// Returns [`EnqueueDecision::Queue`] when the frame was stored,
    /// [`EnqueueDecision::Drop`] when the policy discarded it, or
    /// [`EnqueueDecision::Replace`] when an older pending payload was
    /// displaced (only `LatestOnly`).
    #[inline]
    pub fn enqueue(&mut self, payload: FramePayload) -> EnqueueDecision {
        match self {
            Self::Fifo(s) => s.enqueue(payload),
            Self::RealtimeDropOld(s) => s.enqueue(payload),
            Self::LatestOnly(s) => s.enqueue(payload),
        }
    }

    /// Take the next frame to send from this stream. Returns
    /// [`DequeueOutcome::Empty`] when there is nothing to send.
    #[inline]
    pub fn dequeue(&mut self) -> DequeueOutcome {
        match self {
            Self::Fifo(s) => s.dequeue(),
            Self::RealtimeDropOld(s) => s.dequeue(),
            Self::LatestOnly(s) => s.dequeue(),
        }
    }

    /// Drain **every** queued frame, regardless of policy, returning them
    /// in send order.
    ///
    /// Used by [`SendPolicyTable::bind`](crate::SendPolicyTable::bind)
    /// when a state is being replaced — the caller is responsible for
    /// emitting the drained frames before the new state takes effect, so
    /// in-flight data is not silently lost.
    pub fn flush_for_upgrade(&mut self, out: &mut Vec<FramePayload>) {
        match self {
            Self::Fifo(s) => s.drain_into(out),
            Self::RealtimeDropOld(s) => s.drain_into(out),
            Self::LatestOnly(s) => s.drain_into(out),
        }
    }

    /// `true` if [`dequeue`](Self::dequeue) would emit at least one
    /// frame right now. Used by the table to skip idle streams during the
    /// per-tick scheduler scan.
    #[inline]
    #[must_use]
    pub fn is_ready(&self) -> bool {
        match self {
            Self::Fifo(s) => s.is_ready(),
            Self::RealtimeDropOld(s) => s.is_ready(),
            Self::LatestOnly(s) => s.is_ready(),
        }
    }
}

// ---------------------------------------------------------------------------
// Fifo — ReliableOrdered & ReliableUnordered
// ---------------------------------------------------------------------------

/// State for [`DeliveryPolicy::ReliableOrdered`] and
/// [`DeliveryPolicy::ReliableUnordered`].
///
/// On the send side both policies are identical: append to a FIFO, drain
/// head-first. The receive-side distinction (dedup vs. reorder window)
/// is irrelevant here because the sender is the source of order.
#[derive(Debug)]
pub struct FifoState {
    queue: StreamQueue,
}

impl FifoState {
    #[must_use]
    fn new() -> Self {
        Self {
            queue: StreamQueue::new(),
        }
    }

    fn enqueue(&mut self, payload: FramePayload) -> EnqueueDecision {
        let seq = payload.seq;
        if self.queue.push_back(payload).is_some() {
            // The queue was full; the head was displaced to make room.
            // Receive-side policy will tolerate the gap; we count and
            // log at the table level.
            tracing::warn!(
                seq = seq.get(),
                queue_len = self.queue.len(),
                "send-policy FIFO queue overflowed — displaced oldest frame"
            );
        }
        EnqueueDecision::Queue
    }

    fn dequeue(&mut self) -> DequeueOutcome {
        match self.queue.pop_front() {
            Some(p) => DequeueOutcome::Some(p),
            None => DequeueOutcome::Empty,
        }
    }

    fn drain_into(&mut self, out: &mut Vec<FramePayload>) {
        out.extend(self.queue.drain_all());
    }

    fn is_ready(&self) -> bool {
        !self.queue.is_empty()
    }
}

// ---------------------------------------------------------------------------
// RealtimeDropOld
// ---------------------------------------------------------------------------

/// State for [`DeliveryPolicy::RealtimeDropOld`].
///
/// A frame whose `Seq` is older than the newest frame ever queued is
/// discarded — the receive-side counterpart will drop it on arrival
/// anyway, so spending a slot on it would only waste send bandwidth.
#[derive(Debug)]
pub struct RealtimeDropOldState {
    queue: StreamQueue,
}

impl RealtimeDropOldState {
    #[must_use]
    fn new() -> Self {
        Self {
            queue: StreamQueue::new(),
        }
    }

    fn enqueue(&mut self, payload: FramePayload) -> EnqueueDecision {
        let seq = payload.seq;
        if let Some(newest) = self.queue.newest_seq() {
            if seq <= newest {
                // Stale or duplicate — drop before it consumes a slot.
                return EnqueueDecision::Drop;
            }
        }
        if self.queue.push_back(payload).is_some() {
            tracing::warn!(
                seq = seq.get(),
                queue_len = self.queue.len(),
                "send-policy RealtimeDropOld queue overflowed — displaced oldest frame"
            );
        }
        EnqueueDecision::Queue
    }

    fn dequeue(&mut self) -> DequeueOutcome {
        match self.queue.pop_front() {
            Some(p) => DequeueOutcome::Some(p),
            None => DequeueOutcome::Empty,
        }
    }

    fn drain_into(&mut self, out: &mut Vec<FramePayload>) {
        out.extend(self.queue.drain_all());
    }

    fn is_ready(&self) -> bool {
        !self.queue.is_empty()
    }
}

// ---------------------------------------------------------------------------
// LatestOnly
// ---------------------------------------------------------------------------

/// State for [`DeliveryPolicy::LatestOnly`].
///
/// Single-slot pending frame. A newer arrival displaces the older one
/// (returned to the caller as `Replace`); an older arrival is dropped.
/// `dequeue` emits and clears the slot.
#[derive(Debug)]
pub struct LatestOnlyState {
    pending: Option<FramePayload>,
}

impl LatestOnlyState {
    #[must_use]
    fn new() -> Self {
        Self { pending: None }
    }

    fn enqueue(&mut self, payload: FramePayload) -> EnqueueDecision {
        let seq = payload.seq;
        // `take()` to avoid borrowing `self.pending` across the match —
        // FramePayload is not Copy, so a by-value match on `&mut` would
        // try to move it out. We own the taken Option and put the right
        // value back in each arm.
        match self.pending.take() {
            None => {
                self.pending = Some(payload);
                EnqueueDecision::Queue
            }
            Some(cur) if seq > cur.seq => {
                // Newer frame — replace the older one. The displaced
                // payload is handed back to the caller for recycling.
                self.pending = Some(payload);
                EnqueueDecision::Replace(Box::new(cur))
            }
            Some(cur) => {
                // Older or equal — keep the existing pending frame,
                // drop the new arrival.
                self.pending = Some(cur);
                EnqueueDecision::Drop
            }
        }
    }

    fn dequeue(&mut self) -> DequeueOutcome {
        match self.pending.take() {
            Some(p) => DequeueOutcome::Some(p),
            None => DequeueOutcome::Empty,
        }
    }

    fn drain_into(&mut self, out: &mut Vec<FramePayload>) {
        if let Some(p) = self.pending.take() {
            out.push(p);
        }
    }

    fn is_ready(&self) -> bool {
        self.pending.is_some()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use bytes::Bytes;
    use frame::header::{FrameFlags, MessageType, Seq};

    fn payload(seq: u64, byte: u8) -> FramePayload {
        FramePayload::new(
            MessageType::TextToken,
            FrameFlags::NONE,
            Seq::new(seq),
            Bytes::from(vec![byte]),
        )
    }

    // ── Fifo ───────────────────────────────────────────────────────

    #[test]
    fn fifo_enqueue_then_dequeue_in_order() {
        let mut s = FifoState::new();
        assert!(matches!(s.enqueue(payload(0, 1)), EnqueueDecision::Queue));
        assert!(matches!(s.enqueue(payload(1, 2)), EnqueueDecision::Queue));

        match s.dequeue() {
            DequeueOutcome::Some(p) => assert_eq!(p.bytes.as_ref(), &[1]),
            _ => panic!("expected Some"),
        }
        match s.dequeue() {
            DequeueOutcome::Some(p) => assert_eq!(p.bytes.as_ref(), &[2]),
            _ => panic!("expected Some"),
        }
        assert!(matches!(s.dequeue(), DequeueOutcome::Empty));
    }

    #[test]
    fn fifo_is_ready_reflects_queue_state() {
        let mut s = FifoState::new();
        assert!(!s.is_ready());
        let _ = s.enqueue(payload(0, 1));
        assert!(s.is_ready());
    }

    // ── RealtimeDropOld ────────────────────────────────────────────

    #[test]
    fn realtime_drops_older_than_newest_queued() {
        let mut s = RealtimeDropOldState::new();
        assert!(matches!(s.enqueue(payload(5, 1)), EnqueueDecision::Queue));
        // Older seq → Drop.
        assert!(matches!(s.enqueue(payload(3, 2)), EnqueueDecision::Drop));
        // Equal seq → Drop.
        assert!(matches!(s.enqueue(payload(5, 3)), EnqueueDecision::Drop));
        // Newer seq → Queue.
        assert!(matches!(s.enqueue(payload(7, 4)), EnqueueDecision::Queue));

        // Dequeue preserves arrival order of accepted frames.
        match s.dequeue() {
            DequeueOutcome::Some(p) => assert_eq!(p.bytes.as_ref(), &[1]),
            _ => panic!("expected seq=5 first"),
        }
        match s.dequeue() {
            DequeueOutcome::Some(p) => assert_eq!(p.bytes.as_ref(), &[4]),
            _ => panic!("expected seq=7 second"),
        }
        assert!(matches!(s.dequeue(), DequeueOutcome::Empty));
    }

    #[test]
    fn realtime_keeps_newest_seq_after_drain() {
        let mut s = RealtimeDropOldState::new();
        let _ = s.enqueue(payload(5, 1));
        let _ = s.enqueue(payload(9, 2));
        // Drain both.
        let _ = s.dequeue();
        let _ = s.dequeue();
        // Now an older seq arrives — should still be dropped because
        // newest_seq is 9.
        assert!(matches!(s.enqueue(payload(7, 3)), EnqueueDecision::Drop));
        // And a newer one is accepted.
        assert!(matches!(s.enqueue(payload(10, 4)), EnqueueDecision::Queue));
    }

    // ── LatestOnly ─────────────────────────────────────────────────

    #[test]
    fn latest_only_first_frame_is_queued() {
        let mut s = LatestOnlyState::new();
        assert!(matches!(s.enqueue(payload(0, 1)), EnqueueDecision::Queue));
        match s.dequeue() {
            DequeueOutcome::Some(p) => assert_eq!(p.bytes.as_ref(), &[1]),
            _ => panic!("expected the one pending"),
        }
    }

    #[test]
    fn latest_only_newer_replaces_older() {
        let mut s = LatestOnlyState::new();
        let _ = s.enqueue(payload(0, 1));
        match s.enqueue(payload(1, 2)) {
            EnqueueDecision::Replace(p) => assert_eq!(p.bytes.as_ref(), &[1]),
            _ => panic!("expected Replace"),
        }
        match s.dequeue() {
            DequeueOutcome::Some(p) => assert_eq!(p.bytes.as_ref(), &[2]),
            _ => panic!("expected newer payload"),
        }
    }

    #[test]
    fn latest_only_older_dropped() {
        let mut s = LatestOnlyState::new();
        let _ = s.enqueue(payload(5, 1));
        assert!(matches!(s.enqueue(payload(3, 2)), EnqueueDecision::Drop));
        match s.dequeue() {
            DequeueOutcome::Some(p) => assert_eq!(p.bytes.as_ref(), &[1]),
            _ => panic!("older should not have replaced pending"),
        }
    }

    // ── Variant dispatch via PerStreamSendState ────────────────────

    #[test]
    fn per_stream_state_dispatches_by_variant() {
        let mut s = PerStreamSendState::new(DeliveryPolicy::LatestOnly);
        assert!(matches!(s.enqueue(payload(0, 1)), EnqueueDecision::Queue));
        assert!(s.is_ready());
        assert!(matches!(s.dequeue(), DequeueOutcome::Some(_)));
        assert!(!s.is_ready());
    }

    #[test]
    fn flush_for_upgrade_emits_all_pending_fifo() {
        let mut s = PerStreamSendState::new(DeliveryPolicy::ReliableOrdered);
        let _ = s.enqueue(payload(0, 1));
        let _ = s.enqueue(payload(1, 2));
        let mut out = Vec::new();
        s.flush_for_upgrade(&mut out);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].bytes.as_ref(), &[1]);
        assert_eq!(out[1].bytes.as_ref(), &[2]);
    }

    #[test]
    fn flush_for_upgrade_emits_realtime_queue() {
        let mut s = PerStreamSendState::new(DeliveryPolicy::RealtimeDropOld);
        let _ = s.enqueue(payload(0, 1));
        let _ = s.enqueue(payload(2, 3));
        let mut out = Vec::new();
        s.flush_for_upgrade(&mut out);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn flush_for_upgrade_emits_latest_only_pending() {
        let mut s = PerStreamSendState::new(DeliveryPolicy::LatestOnly);
        let _ = s.enqueue(payload(0, 7));
        let mut out = Vec::new();
        s.flush_for_upgrade(&mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].bytes.as_ref(), &[7]);
    }
}

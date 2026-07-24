//! [`StreamQueue`] — the bounded FIFO used by the in-order send policies.
//!
//! `ReliableOrdered` and `ReliableUnordered` differ only in receive-side
//! semantics; on the send side both append to a FIFO and the scheduler
//! drains it head-first. Sharing one queue type keeps the per-stream
//! memory cost uniform and lets the scheduler select across streams
//! without caring which of the two policies a stream declared.

use std::collections::VecDeque;

use frame::header::Seq;

use crate::decision::FramePayload;

/// Maximum frames buffered in a single stream's FIFO.
///
/// Bounds peak memory: 1024 frames × ~16 KiB payload ≈ 16 MiB per stream.
/// When exceeded, [`StreamQueue::push_back`] returns the displaced frame
/// to the caller — the table logs and counts the event but does not
/// silently drop the new payload (it displaces the oldest instead, on the
/// grounds that a fresher frame further back in the queue is preferable
/// to a stale one at the head). The receive-side `ReliableOrdered` policy
/// tolerates the resulting gap via its reorder window; the receive-side
/// `ReliableUnordered` policy tolerates it via dedup; the higher layer
/// is responsible for any retry.
pub const MAX_STREAM_QUEUE: usize = 1024;

/// Bounded FIFO of pending egress frames for one stream.
///
/// Stored as `VecDeque<FramePayload>` keyed by arrival order. Each
/// `FramePayload` carries its own `seq`, so the queue maintains
/// `newest_seq` for `RealtimeDropOld`'s stale check by reading
/// `payload.seq` on every push — no separate seq argument needed.
#[derive(Debug, Default)]
pub struct StreamQueue {
    buf: VecDeque<FramePayload>,
    /// Highest `Seq` ever pushed to this queue. Maintained across pops so
    /// a stream's "newest seen" survives queue drain — required for
    /// `RealtimeDropOld`'s "older than newest" check on a re-fill after
    /// the queue has been emptied by the scheduler.
    newest_seq: Option<Seq>,
}

impl StreamQueue {
    /// Construct an empty queue.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of frames currently buffered.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// `true` when no frames are buffered.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Highest `Seq` ever observed by `push_back`. Used by the table to
    /// implement `RealtimeDropOld` after the queue drains.
    #[inline]
    #[must_use]
    pub fn newest_seq(&self) -> Option<Seq> {
        self.newest_seq
    }

    /// Append a frame to the tail. Returns `Some(displaced)` when the
    /// queue was at capacity — the caller is expected to log/count and
    /// then drop the displaced payload. `payload.seq` is recorded as
    /// `newest_seq` regardless of capacity outcome (it represents the
    /// freshest arrival the stream has seen).
    #[inline]
    pub fn push_back(&mut self, payload: FramePayload) -> Option<FramePayload> {
        self.newest_seq = Some(payload.seq);
        let displaced = if self.buf.len() >= MAX_STREAM_QUEUE {
            self.buf.pop_front()
        } else {
            None
        };
        self.buf.push_back(payload);
        displaced
    }

    /// Pop the head of the queue. Returns `None` when empty.
    #[inline]
    pub fn pop_front(&mut self) -> Option<FramePayload> {
        self.buf.pop_front()
    }

    /// Peek-only view of the head — used by the scheduler's selection
    /// pass to size its packet without consuming the frame.
    #[inline]
    #[must_use]
    pub fn peek_front(&self) -> Option<&FramePayload> {
        self.buf.front()
    }

    /// Drop every queued frame and hand them to the caller. Used by
    /// `remove_stream` so the table can route displaced payloads through
    /// the same pool-recycle path that `enqueue` uses on `Replace`.
    #[inline]
    pub fn drain_all(&mut self) -> Vec<FramePayload> {
        self.buf.drain(..).collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use bytes::Bytes;
    use frame::header::{FrameFlags, MessageType};

    fn p(seq: u64, byte: u8) -> FramePayload {
        FramePayload::new(
            MessageType::TextToken,
            FrameFlags::NONE,
            Seq::new(seq),
            Bytes::from(vec![byte]),
        )
    }

    #[test]
    fn empty_queue_is_empty() {
        let mut q = StreamQueue::new();
        assert!(q.is_empty());
        assert_eq!(q.len(), 0);
        assert!(q.pop_front().is_none());
        assert!(q.peek_front().is_none());
    }

    #[test]
    fn push_pop_round_trip_preserves_order() {
        let mut q = StreamQueue::new();
        q.push_back(p(0, 1));
        q.push_back(p(1, 2));
        q.push_back(p(2, 3));
        assert_eq!(q.len(), 3);

        assert_eq!(q.pop_front().unwrap().bytes.as_ref(), &[1]);
        assert_eq!(q.pop_front().unwrap().bytes.as_ref(), &[2]);
        assert_eq!(q.pop_front().unwrap().bytes.as_ref(), &[3]);
        assert!(q.pop_front().is_none());
    }

    #[test]
    fn newest_seq_remains_after_drain() {
        let mut q = StreamQueue::new();
        q.push_back(p(5, 0));
        q.push_back(p(9, 0));
        let _ = q.pop_front();
        let _ = q.pop_front();
        assert!(q.is_empty());
        assert_eq!(q.newest_seq(), Some(Seq::new(9)));
    }

    #[test]
    fn push_at_capacity_displaces_oldest() {
        let mut q = StreamQueue::new();
        for i in 0..MAX_STREAM_QUEUE as u64 {
            q.push_back(p(i, i as u8));
        }
        assert_eq!(q.len(), MAX_STREAM_QUEUE);
        // One more push should displace the oldest (seq=0) and stay at cap.
        let displaced = q.push_back(p(MAX_STREAM_QUEUE as u64, 0xAA));
        assert_eq!(q.len(), MAX_STREAM_QUEUE);
        let d = displaced.unwrap();
        assert_eq!(d.bytes.as_ref(), &[0]);
    }

    #[test]
    fn drain_all_empties_and_returns_in_order() {
        let mut q = StreamQueue::new();
        q.push_back(p(0, 1));
        q.push_back(p(1, 2));
        let drained = q.drain_all();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].bytes.as_ref(), &[1]);
        assert_eq!(drained[1].bytes.as_ref(), &[2]);
        assert!(q.is_empty());
    }
}

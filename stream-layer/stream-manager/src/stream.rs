//! [`Stream`] — the per-stream metadata record owned by the
//! Stream Manager.
//!
//! See `DESIGN.md` Part 2 (stream object) and Part 5 (FSM).
//!
//! # Phase 0 status
//!
//! This module ships the field shape + accessors. The actual
//! transition methods (`start`, `fin`, `ack`, `reset`, …) land in
//! Phase 1 when the FSM controller ([`crate::lifecycle`]) is wired
//! up. Phase 0 exposes the struct so other modules can reference
//! it in their skeletons.

use crate::id::StreamId;
use crate::priority::StreamPriority;
use crate::slab::SlotHandle;
use crate::state::{RecvState, SendState, StreamState};

/// Nanoseconds since manager start (monotonic — `Instant::now()` minus
/// the manager's `start_instant`). `u64` is enough for ~584 years at
/// nanosecond resolution.
pub type StreamInstantNs = u64;

/// Per-stream metadata. One instance per live stream; the
/// [`crate::table::StreamTable`] owns these and hands out
/// [`SlotHandle`]s for O(1) access.
///
/// # Layout note
///
/// Fields are ordered for cache-line friendliness on the hot path:
/// the FSM + offsets (touched every packet) come first; the
/// priority + timestamps (touched once per scheduling decision)
/// come last. Phase 1 may split this into hot/cold halves if
/// profiles justify it.
#[derive(Clone, Debug)]
pub struct Stream {
    /// RFC 9000 §3.2 stream id.
    pub id: StreamId,
    /// RFC 9218 priority (urgency + incremental).
    pub priority: StreamPriority,
    /// Send-half FSM (RFC 9000 §3.1).
    pub send_state: SendState,
    /// Receive-half FSM (RFC 9000 §3.1).
    pub recv_state: RecvState,
    /// Slab slot the stream occupies in the [`crate::table::StreamTable`].
    pub slot: SlotHandle,
    /// Next byte offset to send (cumulative).
    pub send_offset: u64,
    /// Next byte offset the application has queued for send.
    pub queued_offset: u64,
    /// Highest byte offset the peer has acknowledged.
    pub acked_offset: u64,
    /// Final offset declared by FIN (RFC 9000 §4.5). `None` until
    /// the stream knows its final size.
    pub final_size: Option<u64>,
    /// Next byte offset to deliver to the application (recv side).
    pub recv_offset: u64,
    /// Nanoseconds since manager start when the stream was created.
    pub created_ns: StreamInstantNs,
    /// Nanoseconds since manager start of the last activity (any
    /// direction). Used by idle-timeout GC (RFC 9000 §3.3).
    pub last_active_ns: StreamInstantNs,
    /// Whether the application has explicitly signalled "no more
    /// data" (FIN queued for send).
    pub fin_queued: bool,
}

impl Stream {
    /// Construct a new stream in the `Idle / Recv` initial state.
    ///
    /// Phase 0 only records the metadata; Phase 1's
    /// [`crate::lifecycle::StreamLifecycle`] will own creation.
    #[must_use]
    pub fn new(
        id: StreamId,
        priority: StreamPriority,
        slot: SlotHandle,
        now_ns: StreamInstantNs,
    ) -> Self {
        Self {
            id,
            priority,
            send_state: SendState::default(),
            recv_state: RecvState::default(),
            slot,
            send_offset: 0,
            queued_offset: 0,
            acked_offset: 0,
            final_size: None,
            recv_offset: 0,
            created_ns: now_ns,
            last_active_ns: now_ns,
            fin_queued: false,
        }
    }

    /// Coarse combined state derived from the two halves. See
    /// [`StreamState::from_halves`].
    #[must_use]
    pub const fn combined_state(&self) -> StreamState {
        StreamState::from_halves(self.send_state, self.recv_state)
    }

    /// Whether the stream is fully closed and can be GC'd.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.combined_state().is_closed()
    }

    /// Mark the stream as having seen activity (resets idle GC).
    pub fn touch(&mut self, now_ns: StreamInstantNs) {
        self.last_active_ns = now_ns;
    }

    /// Bytes queued for send but not yet transmitted.
    #[must_use]
    pub const fn send_backlog(&self) -> u64 {
        self.queued_offset.saturating_sub(self.send_offset)
    }

    /// Bytes sent but not yet acknowledged.
    #[must_use]
    pub const fn send_in_flight(&self) -> u64 {
        self.send_offset.saturating_sub(self.acked_offset)
    }
}

impl Default for Stream {
    fn default() -> Self {
        Self::new(
            StreamId::default(),
            StreamPriority::default(),
            SlotHandle::nil(),
            0,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stream_starts_in_open_state() {
        let s = Stream::new(
            StreamId::default(),
            StreamPriority::default(),
            SlotHandle::nil(),
            0,
        );
        assert_eq!(s.send_state, SendState::Idle);
        assert_eq!(s.recv_state, RecvState::Recv);
        assert_eq!(s.combined_state(), StreamState::Open);
        assert!(!s.is_closed());
    }

    #[test]
    fn backlog_and_in_flight_track_offsets() {
        let s = Stream {
            queued_offset: 100,
            send_offset: 40,
            acked_offset: 10,
            ..Stream::default()
        };
        assert_eq!(s.send_backlog(), 60);
        assert_eq!(s.send_in_flight(), 30);
    }

    #[test]
    fn touch_updates_last_active() {
        let mut s = Stream::default();
        s.touch(1_000);
        assert_eq!(s.last_active_ns, 1_000);
    }
}

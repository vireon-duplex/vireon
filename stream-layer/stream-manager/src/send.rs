//! [`SendBuffer`] — per-stream outbound byte buffer.
//!
//! See `DESIGN.md` Part 7 (send side).
//!
//! # Responsibilities (Phase 2)
//!
//! - Hold queued bytes the application has written but the wire
//!   has not yet taken.
//! - Track the cumulative send offset so retransmission and ack
//!   can index into the buffer.
//! - Surface the FIN flag once the application signals
//!   "no more data".
//! - Apply per-stream flow-control credit before exposing bytes to
//!   the scheduler.
//!
//! The buffer's backing store is [`buffer_pool::Buffer`] slices
//! (Phase 1+), chained for writes larger than one slab chunk. Phase
//! 0 ships only the field shape.
//!
//! # Phase 0 status
//!
//! Field shape + accessor stubs. Functional `write` / `peek` /
//! `consume` land in Phase 2.

use crate::error::StreamManagerError;

/// Maximum bytes a single stream may buffer (send side). 1 MiB
/// matches the [`crate::config::MAX_INITIAL_WINDOW`] hard cap — a
/// stream can never have more outstanding data than its window.
pub const SEND_BUFFER_MAX_BYTES: u64 = 1024 * 1024;

/// Per-stream send buffer. Phase 0 placeholder.
// TODO(phase-2): back this with `BufferPool` slabs.
#[derive(Clone, Debug, Default)]
pub struct SendBuffer {
    /// Total bytes the application has written.
    queued: u64,
    /// Total bytes the wire has accepted (acked offsets are tracked
    /// separately on the [`Stream`](crate::stream::Stream)).
    sent: u64,
    /// Whether the application has signalled FIN.
    fin_queued: bool,
}

impl SendBuffer {
    /// Construct an empty buffer.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            queued: 0,
            sent: 0,
            fin_queued: false,
        }
    }

    /// Bytes written by the application but not yet transmitted.
    #[must_use]
    pub const fn backlog(&self) -> u64 {
        self.queued.saturating_sub(self.sent)
    }

    /// Whether the application has queued FIN.
    #[must_use]
    pub const fn fin_queued(&self) -> bool {
        self.fin_queued
    }

    /// Phase-2 `write` placeholder.
    ///
    /// # Errors
    /// Always returns [`StreamManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn write(&mut self, _bytes: &[u8]) -> Result<u64, StreamManagerError> {
        // TODO(phase-2): append to backing slab, bump queued.
        Err(StreamManagerError::ResourceExhausted(
            "send buffer phase-2 stub",
        ))
    }

    /// Mark that the application has queued FIN (no more writes).
    pub fn mark_fin(&mut self) {
        self.fin_queued = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_empty() {
        let b = SendBuffer::default();
        assert_eq!(b.backlog(), 0);
        assert!(!b.fin_queued());
    }

    #[test]
    fn new_matches_default() {
        assert_eq!(SendBuffer::new().backlog(), SendBuffer::default().backlog());
    }

    #[test]
    fn mark_fin_sets_flag() {
        let mut b = SendBuffer::new();
        b.mark_fin();
        assert!(b.fin_queued());
    }
}

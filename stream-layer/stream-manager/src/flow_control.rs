//! [`StreamFlowControl`] — per-stream flow-control credit (RFC 9000 §4.1).
//!
//! See `DESIGN.md` Part 8 (flow control).
//!
//! # Overview
//!
//! Each stream has two independent credit windows:
//!
//! - **Send credit** — how many more bytes the peer will accept
//!   (governed by the peer's `MAX_STREAM_DATA` frames).
//! - **Recv credit** — how many more bytes we will accept from the
//!   peer (governed by our `MAX_STREAM_DATA` frames).
//!
//! When the send window is exhausted, the scheduler must skip this
//! stream until the peer bumps credit. When the recv window is
//! nearly exhausted, we proactively send a new `MAX_STREAM_DATA` to
//! keep the stream moving.
//!
//! # Phase 0 status
//!
//! Field shape + accessor stubs. Functional credit accounting,
//! `STREAM_DATA_BLOCKED` emission, and auto-tuning of recv window
//! land in Phase 2.

use crate::error::StreamManagerError;

/// Threshold below which we proactively refresh recv credit (fraction
/// of `max`). When the remaining recv credit drops below this, we
/// send a fresh `MAX_STREAM_DATA`.
pub const RECV_REFRESH_THRESHOLD_FRACTION: u32 = 4; // refresh at <25% remaining

/// Per-stream flow-control state.
#[derive(Clone, Copy, Debug)]
pub struct StreamFlowControl {
    /// Maximum bytes the peer permits us to send (their
    /// `MAX_STREAM_DATA`).
    send_max: u64,
    /// Bytes we have sent so far.
    send_consumed: u64,
    /// Maximum bytes we will accept from the peer (our
    /// `MAX_STREAM_DATA`).
    recv_max: u64,
    /// Bytes the peer has sent so far.
    recv_consumed: u64,
}

impl StreamFlowControl {
    /// Construct with initial windows (from
    /// [`crate::config::StreamManagerConfig`]).
    #[must_use]
    pub const fn new(initial_send_max: u64, initial_recv_max: u64) -> Self {
        Self {
            send_max: initial_send_max,
            send_consumed: 0,
            recv_max: initial_recv_max,
            recv_consumed: 0,
        }
    }

    /// Remaining send credit.
    #[must_use]
    pub const fn send_credit(&self) -> u64 {
        self.send_max.saturating_sub(self.send_consumed)
    }

    /// Remaining recv credit.
    #[must_use]
    pub const fn recv_credit(&self) -> u64 {
        self.recv_max.saturating_sub(self.recv_consumed)
    }

    /// Whether the send side is blocked (zero credit).
    #[must_use]
    pub const fn send_blocked(&self) -> bool {
        self.send_credit() == 0
    }

    /// Whether the recv side should refresh its advertised window.
    #[must_use]
    pub const fn recv_needs_refresh(&self) -> bool {
        self.recv_credit() < self.recv_max / RECV_REFRESH_THRESHOLD_FRACTION as u64
    }

    /// Phase-2 `consume_send` placeholder. Phase 1+ will debit
    /// credit and emit `STREAM_DATA_BLOCKED` on exhaustion.
    ///
    /// # Errors
    /// Returns [`StreamManagerError::FlowControlViolation`] if the
    /// caller attempts to consume more than the available credit
    /// (a local bug — the peer exceeding credit is detected in
    /// `ingest_recv`).
    pub fn consume_send(&mut self, _bytes: u64) -> Result<(), StreamManagerError> {
        // TODO(phase-2): real debit.
        Err(StreamManagerError::ResourceExhausted(
            "flow-control phase-2 stub",
        ))
    }

    /// Phase-2 `ingest_recv` placeholder. Returns the new
    /// `max_stream_data` to advertise if a refresh is warranted.
    ///
    /// # Errors
    /// Returns [`StreamManagerError::FlowControlViolation`] if the
    /// peer exceeded advertised credit (RFC 9000 §4.5: connection
    /// error).
    pub fn ingest_recv(&mut self, _new_max: u64) -> Result<Option<u64>, StreamManagerError> {
        // TODO(phase-2): real accounting + refresh.
        Err(StreamManagerError::ResourceExhausted(
            "flow-control phase-2 stub",
        ))
    }
}

impl Default for StreamFlowControl {
    fn default() -> Self {
        Self::new(
            crate::config::DEFAULT_INITIAL_MAX_STREAM_DATA_BIDI,
            crate::config::DEFAULT_INITIAL_MAX_STREAM_DATA_BIDI,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_has_full_credit() {
        let fc = StreamFlowControl::default();
        assert_eq!(
            fc.send_credit(),
            crate::config::DEFAULT_INITIAL_MAX_STREAM_DATA_BIDI
        );
        assert_eq!(
            fc.recv_credit(),
            crate::config::DEFAULT_INITIAL_MAX_STREAM_DATA_BIDI
        );
        assert!(!fc.send_blocked());
    }

    #[test]
    fn blocked_when_credit_zero() {
        let fc = StreamFlowControl::new(0, 0);
        assert!(fc.send_blocked());
        assert_eq!(fc.recv_credit(), 0);
    }

    #[test]
    fn recv_needs_refresh_below_threshold() {
        // max = 1000, refresh when credit < 250
        let mut fc = StreamFlowControl::new(1000, 1000);
        fc.recv_consumed = 800; // credit = 200 < 250
        assert!(fc.recv_needs_refresh());
        fc.recv_consumed = 700; // credit = 300 > 250
        assert!(!fc.recv_needs_refresh());
    }
}

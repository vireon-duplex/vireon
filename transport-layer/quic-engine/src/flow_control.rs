//! [`FlowController`] — connection + stream flow control (RFC 9000 §4).
//!
//! Phase 7 — Flow Control. See `DESIGN.md` §4.7.
//!
//! Vireon delegates the wire encoding of MAX_DATA / MAX_STREAM_DATA
//! / DATA_BLOCKED / STREAM_DATA_BLOCKED / STOP_SENDING to quiche.
//! This module exposes:
//!
//! - The per-connection credit pool (Vireon can apply its own
//!   budget policy on top of RFC 9000 defaults)
//! - Per-stream credit pools (one per stream id)
//! - `Blocked` signals — surfaced as backpressure to the
//!   application so it can stop producing data
//! - Window-update policy (when to grant more credit — conservative
//!   vs aggressive, per RFC 9000 §4.2.2 recommendations)
//!
//! Integrates with [`buffer-pool`](../../buffer-pool) — outstanding
//! credit bounds buffer-pool usage, preventing a misbehaving peer
//! from causing OOM.

/// Phase-0 skeleton. Phase 7 will hold per-stream credit tables and
/// the per-connection aggregate.
#[derive(Default)]
pub struct FlowController {
    /// Total bytes we are willing to buffer for the connection.
    pub max_data: u64,
    /// Bytes currently buffered (sum across all streams).
    pub buffered: u64,
    /// Total bytes the peer has authorized us to send.
    pub peer_max_data: u64,
    /// Bytes we have sent that count against `peer_max_data`.
    pub sent: u64,
}

impl FlowController {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_data: 0,
            buffered: 0,
            peer_max_data: 0,
            sent: 0,
        }
    }

    /// TODO(phase-7): per-stream credit, `send_blocked()` hook,
    /// `update_window()` policy.
    pub fn _placeholder(&self) {}
}

/// Phase-7 hook fired when the peer reports we are blocked (sent
/// DATA_BLOCKED or STREAM_DATA_BLOCKED).
#[derive(Debug)]
#[non_exhaustive]
pub struct BlockedEvent {
    /// Stream ID (None for connection-level).
    pub stream_id: Option<u64>,
    /// Current credit limit at the time of block.
    pub limit: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let _f = FlowController::new();
        // TODO(phase-7): per-stream tables, credit grant policy,
        // buffer-pool integration.
    }
}

//! [`QuicConnection`] — per-connection state, wrapping
//! `quiche::Connection`.
//!
//! Phase 1 — Connection Manager. See `DESIGN.md` §4.1.
//!
//! This is the per-connection entry stored in the engine's slab.
//! It owns:
//! - The underlying `quiche::Connection` (protocol state machine,
//!   loss recovery, congestion control, flow control, streams)
//! - The active CID pair
//! - The [`QuicConnState`] (handshake / connected / closing / etc.)
//! - The path tuple (Phase 12 — Path Manager)
//! - Per-connection stats snapshot (Phase 14 — Observability)
//! - The `connection_shard::ConnectionId` cross-reference (Phase 13)
//!
//! Cache-aligned (`#[repr(C, align(64))]`) to avoid false sharing
//! between adjacent slab entries — same convention as
//! [`connection_shard::connection::ConnectionEntry`](../../connection-shard/src/connection.rs).

use std::net::SocketAddr;

use crate::cid::QuicCid;
use crate::state::QuicConnState;

/// Cache-aligned connection entry in the per-shard slab.
#[repr(C, align(64))]
#[derive(Debug)]
pub struct QuicConnection {
    /// Current lifecycle state.
    pub state: QuicConnState,
    /// Local CID we have advertised (8 bytes).
    pub local_cid: QuicCid,
    /// Remote CID we are sending to.
    pub remote_cid: QuicCid,
    /// 4-tuple of the active path.
    pub peer: SocketAddr,
    /// Monotonic nanos when the connection was created.
    pub created_at_ns: u64,
    /// Monotonic nanos of the last packet sent or received.
    pub last_active_ns: u64,
    /// Total bytes received on this connection.
    pub bytes_recv: u64,
    /// Total bytes sent on this connection.
    pub bytes_sent: u64,
    /// Reserved padding to keep the entry within a single cache line
    /// (Phase 1 will tune this once `quiche::Connection` is wrapped).
    _padding: [u64; 4],
}

impl QuicConnection {
    /// TODO(phase-1): replace with real constructor that wraps a
    /// `quiche::Connection` produced from accept / connect paths.
    #[must_use]
    pub fn placeholder() -> Self {
        Self {
            state: QuicConnState::default(),
            local_cid: QuicCid::empty(),
            remote_cid: QuicCid::empty(),
            peer: SocketAddr::from(([0u8, 0, 0, 0], 0)),
            created_at_ns: 0,
            last_active_ns: 0,
            bytes_recv: 0,
            bytes_sent: 0,
            _padding: [0; 4],
        }
    }
}

impl Default for QuicConnection {
    fn default() -> Self {
        Self::placeholder()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let c = QuicConnection::placeholder();
        assert_eq!(c.state, QuicConnState::Handshaking);
        // TODO(phase-1): wrap quiche::Connection, wire accept/
        // connect, expose stream_send / stream_recv, drive timeout.
    }

    #[test]
    fn entry_is_cache_aligned() {
        assert_eq!(std::mem::align_of::<QuicConnection>(), 64);
    }
}

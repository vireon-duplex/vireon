//! [`QuicConnState`] — per-connection lifecycle state machine.
//!
//! Phase 1 — Connection Manager. See `DESIGN.md` §4.1.
//!
//! RFC 9000 §10 maps connection lifecycle into a small set of
//! discrete states. The engine drives transitions through packet
//! events (Initial / Handshake done → Connected; CONNECTION_CLOSE
//! → Closing / Draining) and timer events (idle timeout → Closed).
//!
//! Distinct from
//! [`connection_shard::ConnectionState`](../../connection-shard/src/state.rs)
//! which tracks Vireon-internal ownership (Init / Active /
//! Backpressured / Migrating / Closing / Closed). The two state
//! machines run in parallel and are reconciled by the
//! [`crate::shard_binding`] module (Phase 13).

/// Lifecycle state of a [`crate::connection::QuicConnection`].
#[non_exhaustive]
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QuicConnState {
    /// Initial received or sent; TLS 1.3 handshake in progress.
    /// RFC 9000 §7.1.
    #[default]
    Handshaking = 0,
    /// Handshake complete; 1-RTT keys available. RFC 9000 §7.2.
    Connected = 1,
    /// Peer-initiated close in flight (CONNECTION_CLOSE sent, waiting
    /// for the draining period to elapse). RFC 9000 §10.2.1.
    Closing = 2,
    /// Peer sent CONNECTION_CLOSE; we are draining (no new packets
    /// sent). RFC 9000 §10.2.2.
    Draining = 3,
    /// Draining complete or idle timer fired; slot may be reclaimed.
    Closed = 4,
}

impl QuicConnState {
    /// Whether the connection is mid-handshake.
    #[must_use]
    pub const fn is_handshaking(self) -> bool {
        matches!(self, Self::Handshaking)
    }

    /// Whether the connection is in the post-handshake steady state.
    #[must_use]
    pub const fn is_established(self) -> bool {
        matches!(self, Self::Connected)
    }

    /// Whether the connection is past the point of accepting new
    /// application data (Closing / Draining / Closed).
    #[must_use]
    pub const fn is_terminating(self) -> bool {
        matches!(self, Self::Closing | Self::Draining | Self::Closed)
    }

    /// Whether the slot can be reclaimed.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        // Phase 1 will populate this module with the full transition
        // matrix + tests. Phase 0 only needs the type to exist so
        // that `QuicError::IllegalTransition` can reference it.
        let s = QuicConnState::default();
        assert_eq!(s, QuicConnState::Handshaking);
        assert!(s.is_handshaking());
        assert!(!s.is_terminal());
    }
}

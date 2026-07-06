//! [`SendState`] / [`RecvState`] / [`StreamState`] — RFC 9000 §3.1
//! + §3.3 stream FSM states.
//!
//! QUIC streams have **two independent halves** (send + receive),
//! each with its own FSM. The combined [`StreamState`] is a
//! convenient summary used by the Stream Manager for lifecycle /
//! GC decisions; the canonical state lives in the per-half
//! enums.
//!
//! # Send FSM (RFC 9000 §3.1)
//!
//! ```text
//! Idle ──start()──► Ready ──send_data()──► Send ──fin()──► DataSent
//!                                                     │
//!                                                     └──ack()──► DataRecvd
//!
//! Any state ──reset()──► ResetSent ──ack()──► ResetRecvd
//! ```
//!
//! # Recv FSM (RFC 9000 §3.1)
//!
//! ```text
//! Recv ──size_known()──► SizeKnown ──all_data()──► DataRcvd
//!                                                       │
//!                                                       └──read()──► DataRead
//!
//! Recv ──all_data()──► DataRcvd ──read()──► DataRead
//!
//! Any state ──reset()──► ResetRcvd ──read()──► ResetRead
//! ```

/// Send-half state of a stream. RFC 9000 §3.1.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SendState {
    /// No data queued.
    #[default]
    Idle,
    /// Data queued, ready to send.
    Ready,
    /// STREAM frame(s) sent, FIN not yet sent.
    Send,
    /// FIN sent, awaiting ACK.
    DataSent,
    /// FIN acknowledged by peer.
    DataRecvd,
    /// RESET_STREAM sent, awaiting ACK.
    ResetSent,
    /// RESET_STREAM acknowledged by peer.
    ResetRecvd,
}

impl SendState {
    /// Whether the local side can still write (i.e. has not yet
    /// sent FIN or RESET_STREAM).
    #[must_use]
    pub const fn can_send(self) -> bool {
        matches!(self, Self::Idle | Self::Ready | Self::Send)
    }

    /// Whether the local side has finished all send activity
    /// (terminal state).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::DataRecvd | Self::ResetRecvd)
    }

    /// Whether RESET_STREAM has been sent (locally or ack'd).
    #[must_use]
    pub const fn is_reset(self) -> bool {
        matches!(self, Self::ResetSent | Self::ResetRecvd)
    }
}

/// Receive-half state of a stream. RFC 9000 §3.1.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum RecvState {
    /// Receiving; final size not yet known.
    #[default]
    Recv,
    /// Final size known (FIN or RESET_STREAM received); not all
    /// data delivered yet.
    SizeKnown,
    /// All data received + FIN processed; awaiting consumption.
    DataRcvd,
    /// All data delivered to the application; stream recyclable.
    DataRead,
    /// RESET_STREAM received; awaiting consumption.
    ResetRcvd,
    /// RESET_STREAM delivered to the application; stream recyclable.
    ResetRead,
}

impl RecvState {
    /// Whether the local side can still receive STREAM frames.
    /// `false` once the final size is known and data is complete.
    #[must_use]
    pub const fn can_recv(self) -> bool {
        matches!(self, Self::Recv | Self::SizeKnown)
    }

    /// Whether the receive half is in a terminal state
    /// (application has consumed everything or the reset).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::DataRead | Self::ResetRead)
    }

    /// Whether RESET_STREAM was received from the peer.
    #[must_use]
    pub const fn is_reset(self) -> bool {
        matches!(self, Self::ResetRcvd | Self::ResetRead)
    }
}

/// Combined stream state — a coarse classification used for
/// lifecycle / GC decisions. Per-half detail lives in
/// [`SendState`] / [`RecvState`].
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum StreamState {
    /// Both halves open.
    #[default]
    Open,
    /// Local half closed (FIN or RESET ack'd); remote still open.
    HalfClosedLocal,
    /// Remote half closed (FIN or RESET received); local still open.
    HalfClosedRemote,
    /// Both halves closed; stream awaiting GC.
    Closed,
}

impl StreamState {
    /// Derive the combined state from per-half states.
    #[must_use]
    pub const fn from_halves(send: SendState, recv: RecvState) -> Self {
        let local_closed = send.is_terminal();
        let remote_closed = recv.is_terminal();
        match (local_closed, remote_closed) {
            (true, true) => Self::Closed,
            (true, false) => Self::HalfClosedLocal,
            (false, true) => Self::HalfClosedRemote,
            (false, false) => Self::Open,
        }
    }

    /// Whether the stream is fully closed and can be GC'd.
    #[must_use]
    pub const fn is_closed(self) -> bool {
        matches!(self, Self::Closed)
    }

    /// Whether at least one half is still active.
    #[must_use]
    pub const fn is_active(self) -> bool {
        !self.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_state_can_send_predicate() {
        assert!(SendState::Idle.can_send());
        assert!(SendState::Ready.can_send());
        assert!(SendState::Send.can_send());
        assert!(!SendState::DataSent.can_send());
        assert!(!SendState::ResetSent.can_send());
    }

    #[test]
    fn send_state_terminal() {
        assert!(SendState::DataRecvd.is_terminal());
        assert!(SendState::ResetRecvd.is_terminal());
        assert!(!SendState::DataSent.is_terminal());
    }

    #[test]
    fn recv_state_can_recv() {
        assert!(RecvState::Recv.can_recv());
        assert!(RecvState::SizeKnown.can_recv());
        assert!(!RecvState::DataRcvd.can_recv());
    }

    #[test]
    fn combined_open_when_both_active() {
        let s = StreamState::from_halves(SendState::Send, RecvState::Recv);
        assert_eq!(s, StreamState::Open);
    }

    #[test]
    fn combined_half_closed_local() {
        let s = StreamState::from_halves(SendState::DataRecvd, RecvState::Recv);
        assert_eq!(s, StreamState::HalfClosedLocal);
    }

    #[test]
    fn combined_half_closed_remote() {
        let s = StreamState::from_halves(SendState::Send, RecvState::DataRead);
        assert_eq!(s, StreamState::HalfClosedRemote);
    }

    #[test]
    fn combined_closed_when_both_terminal() {
        let s = StreamState::from_halves(SendState::ResetRecvd, RecvState::ResetRead);
        assert_eq!(s, StreamState::Closed);
        assert!(s.is_closed());
    }

    #[test]
    fn reset_predicates() {
        assert!(SendState::ResetSent.is_reset());
        assert!(RecvState::ResetRcvd.is_reset());
        assert!(!SendState::Idle.is_reset());
        assert!(!RecvState::Recv.is_reset());
    }
}

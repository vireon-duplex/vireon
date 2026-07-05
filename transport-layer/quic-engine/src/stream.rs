//! [`Stream`] + [`StreamId`] — bidirectional / unidirectional streams.
//!
//! Phase 8 — Stream Manager. See `DESIGN.md` §4.8.
//!
//! RFC 9000 §2 / §3:
//! - Streams are ordered, reliable, flow-controlled byte streams
//! - 4 types: client-initiated bidi/uni, server-initiated bidi/uni
//! - Identified by a 62-bit StreamId (RFC 9000 §3.2)
//!
//! Vireon delegates the wire protocol (STREAM frame encoding,
//! RESET_STREAM, STOP_SENDING, MAX_STREAM_DATA) to quiche. This
//! module wraps the quiche stream API into a Vireon-style handle
//! that the application can drive with `send`, `recv`, `close`,
//! `reset`, and a priority field.

/// 62-bit stream identifier (RFC 9000 §3.2).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamId(pub u64);

impl StreamId {
    /// Whether the stream was initiated by the client (bit 0 clear).
    #[must_use]
    pub const fn is_client_initiated(self) -> bool {
        self.0 & 0x1 == 0
    }

    /// Whether the stream is bidirectional (bit 1 clear).
    #[must_use]
    pub const fn is_bidi(self) -> bool {
        self.0 & 0x2 == 0
    }

    /// TODO(phase-8): `is_server_initiated`, `is_uni`, `index()`.
    pub const fn _placeholder(self) {}
}

/// Phase-0 skeleton. Phase 8 will own:
/// - `state: StreamState` (Open / HalfClosed / Closed)
/// - `priority: StreamPriority`
/// - `bytes_sent`, `bytes_recv`
/// - `fin_sent`, `fin_received`
/// - Backed by `quiche::Connection::stream_send` / `stream_recv`
#[derive(Clone, Copy, Debug)]
pub struct Stream {
    /// Stream identifier.
    pub id: StreamId,
    /// Scheduling priority.
    pub priority: StreamPriority,
}

impl Stream {
    /// TODO(phase-8): full stream wrapper.
    #[must_use]
    pub const fn placeholder(id: StreamId) -> Self {
        Self {
            id,
            priority: StreamPriority::Normal,
        }
    }
}

/// Placeholder — moved to its own `priority.rs` if it grows.
pub mod priority {
    /// Stream scheduling priority. Lower = higher priority.
    #[repr(u8)]
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
    pub enum StreamPriority {
        /// Real-time streams (audio, control plane).
        RealTime = 0,
        /// Default interactive priority.
        #[default]
        Normal = 1,
        /// Background transfers (telemetry, logs).
        Background = 2,
    }
}

pub use priority::StreamPriority;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let s = Stream::placeholder(StreamId(0));
        assert_eq!(s.priority, StreamPriority::Normal);
        assert!(s.id.is_client_initiated());
        assert!(s.id.is_bidi());
        // TODO(phase-8): wrap quiche stream API, integrate with
        // stream_scheduler, expose async send/recv.
    }
}

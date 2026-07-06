//! [`BackpressureTracker`] — end-to-end backpressure propagation.
//!
//! See `DESIGN.md` Part 12 (backpressure).
//!
//! # Propagation chain
//!
//! ```text
//! Application ─► Session ─► Stream ─► Transport ─► Runtime ─► IO
//!      ◄────────── ◄────────── ◄────────── ◄────────── ◄─────
//! ```
//!
//! When any layer signals backpressure (e.g. recv buffer full), the
//! upstream layers must also slow down. The tracker propagates the
//! signal across layer boundaries:
//!
//! - **Session → Application** — pause new stream opens.
//! - **Stream → Session** — pause new sessions for that tenant.
//! - **Transport → Stream** — pause sends (RFC 9000 §4.1).
//!
//! # Phase 0 status
//!
//! Phase 0 ships the type signature + signal shape. Functional
//! propagation lands in Phase 3+.

use crate::id::SessionId;

/// Source of a backpressure signal.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackpressureSource {
    /// Application layer (consumer is slow).
    Application,
    /// Session layer (quota exceeded).
    Session,
    /// Stream layer (flow control).
    Stream,
    /// Transport layer (congestion window).
    Transport,
    /// Runtime layer (reactor saturation).
    Runtime,
    /// IO layer (kernel buffer pressure).
    Io,
}

/// A backpressure signal for a specific session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackpressureSignal {
    /// The session under pressure.
    pub session_id: SessionId,
    /// Source of the signal.
    pub source: BackpressureSource,
    /// Recommended pause duration (ns). 0 = "pause indefinitely".
    pub pause_ns: u64,
}

/// Backpressure tracker.
///
/// Phase 0 holds no state; Phase 3 will add per-session signal
/// queues and the propagation logic.
// TODO(phase-3): per-session signal queues + propagation.
#[derive(Clone, Debug, Default)]
pub struct BackpressureTracker;

impl BackpressureTracker {
    /// Construct a new tracker.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Phase-3 `signal` placeholder.
    pub fn signal(&mut self, _signal: BackpressureSignal) {
        // TODO(phase-3): propagate upstream.
    }

    /// Phase-3 `clear` placeholder.
    pub fn clear(&mut self, _session_id: SessionId) {
        // TODO(phase-3): remove all signals for this session.
    }

    /// Phase-3 `is_blocked` placeholder.
    #[must_use]
    pub const fn is_blocked(&self, _session_id: SessionId) -> bool {
        // TODO(phase-3): check signal queue.
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_variants_distinct() {
        assert_ne!(BackpressureSource::Application, BackpressureSource::Session);
        assert_ne!(BackpressureSource::Stream, BackpressureSource::Transport);
        assert_ne!(BackpressureSource::Runtime, BackpressureSource::Io);
    }

    #[test]
    fn signal_construction() {
        let s = BackpressureSignal {
            session_id: SessionId::nil(),
            source: BackpressureSource::Stream,
            pause_ns: 1_000_000,
        };
        assert_eq!(s.pause_ns, 1_000_000);
    }

    #[test]
    fn is_blocked_phase0_returns_false() {
        let t = BackpressureTracker::new();
        assert!(!t.is_blocked(SessionId::nil()));
    }

    #[test]
    fn signal_and_clear_do_not_panic() {
        let mut t = BackpressureTracker::new();
        t.signal(BackpressureSignal {
            session_id: SessionId::nil(),
            source: BackpressureSource::Stream,
            pause_ns: 0,
        });
        t.clear(SessionId::nil());
    }
}

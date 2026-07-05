//! [`TimerBinding`] — translate quiche's `timeout()` into
//! [`timer-wheel`] ticks.
//!
//! Phase 11 — Timer System. See `DESIGN.md` §4.11.
//!
//! quiche is a polling library — it tells you the next deadline and
//! expects to be called back at that time. Vireon has a hierarchical
//! timer wheel that drives all per-connection timers, so we
//! translate quiche's per-connection deadline into a single
//! `timer-wheel::start` call per connection.
//!
//! Timer categories per connection:
//!
//! - **Loss detection / PTO** (RFC 9002 §6.7–6.8) — set by quiche's
//!   `timeout()`, expires when loss detection must run
//! - **Idle timeout** (RFC 9000 §10.1) — configured per-connection
//! - **Handshake timeout** — abandoned if not done within
//!   `handshake_timeout`
//! - **Keepalive** — application-controlled periodic tick
//! - **ACK delay** — when to send the next ACK frame (Phase 6)
//!
//! When a timer fires, the binding determines which connection owns
//! it and triggers the appropriate quiche callback (timeout, stream
//! close, idle close, etc.).

use std::time::Duration;

/// Phase-0 skeleton. Phase 11 will hold the timer-wheel handle and
/// the per-connection timer registrations.
#[derive(Default)]
pub struct TimerBinding {
    /// Next deadline in monotonic nanos (placeholder).
    next_deadline_ns: u64,
}

impl TimerBinding {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next_deadline_ns: 0,
        }
    }

    /// TODO(phase-11):
    /// `fn on_quiche_timeout(&mut self, deadline_ns: u64)`
    /// `fn fire(&mut self, conn_slot: u32, now_ns: u64) -> TimerFired`
    pub fn _placeholder(&self) {}
}

/// Phase-11 categorization of a fired timer.
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq)]
pub enum TimerFired {
    /// Loss detection / PTO expired (RFC 9002).
    LossDetection,
    /// Idle timeout — close the connection.
    IdleTimeout,
    /// Handshake deadline exceeded — abandon connection.
    HandshakeTimeout,
    /// Keepalive tick — send a PING frame.
    Keepalive,
    /// ACK delay expired — emit an ACK frame.
    AckDelay,
}

/// Default idle timeout (RFC 9000 §10.1).
pub const DEFAULT_IDLE: Duration = Duration::from_secs(30);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let _t = TimerBinding::new();
        // TODO(phase-11): timer-wheel integration, multi-timer per
        // connection (loss + idle + keepalive), min-deadline selection.
    }
}

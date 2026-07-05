//! [`ReactorDriver`] — the protocol layer's callback interface.
//!
//! See `DESIGN.md` §1 (responsibilities) and §3 (event loop —
//! where each callback originates).
//!
//! # Pattern
//!
//! The reactor owns the ring and every runtime primitive. The
//! *protocol layer* (e.g. the future `quic-engine::QuicEngine`)
//! implements [`ReactorDriver`] and passes `&mut self` to the
//! reactor at start. On every event the reactor calls the driver
//! directly — no queue hop, no allocation, no synchronisation.
//!
//! This mirrors Cloudflare Pingora's `Service` and Seastar's
//! `pollable` pattern.

use crate::tick::TickResult;

/// What the driver wants the reactor to do after a callback.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DriverAction {
    /// Continue the event loop normally.
    #[default]
    Continue,
    /// Request graceful shutdown at the next tick boundary.
    RequestShutdown,
}

/// Callback interface implemented by the protocol layer.
///
/// Every method has a default no-op implementation, so Phase 0
/// consumers can implement only the callbacks they care about.
/// Phase 1 will firm up the per-event signatures (e.g. add typed
/// `ConnectionId`, `TimerKind`, `TaskOutcome` parameters).
// TODO(phase-1): flesh out per-event signatures + object safety review.
pub trait ReactorDriver {
    /// Called when a datagram is received into a provided buffer.
    /// `buf_id` references the buffer-pool slot; `len` is the
    /// number of bytes received.
    fn on_packet_recv(&mut self, _buf_id: u32, _len: u32) -> DriverAction {
        DriverAction::Continue
    }

    /// Called when a datagram send completes (SEND_ZC second-CQE).
    fn on_packet_sent(&mut self, _packet_id: u64) -> DriverAction {
        DriverAction::Continue
    }

    /// Called when a registered timer fires.
    fn on_timer(&mut self, _task_id: u64) -> DriverAction {
        DriverAction::Continue
    }

    /// Called when a task finishes executing on the task queue.
    fn on_task_result(&mut self, _task_id: u64) -> DriverAction {
        DriverAction::Continue
    }

    /// Called at the end of every tick with the tick's outcome.
    fn on_tick_complete(&mut self, _tick: &TickResult) {}

    /// Called when the kernel reports CQ overflow. Default
    /// behaviour requests shutdown; override to recover.
    fn on_overflow(&mut self) -> DriverAction {
        DriverAction::RequestShutdown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopDriver;

    impl ReactorDriver for NoopDriver {}

    #[test]
    fn placeholder() {
        let mut d = NoopDriver;
        assert_eq!(d.on_packet_recv(0, 0), DriverAction::Continue);
        assert_eq!(d.on_overflow(), DriverAction::RequestShutdown);
        d.on_tick_complete(&TickResult::default());
        // TODO(phase-1): real driver under real event loop.
    }
}

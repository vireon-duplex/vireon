//! [`PacketScheduler`] — egress ordering: ACK > probe > retransmit >
//! application, with batching and pacing integration.
//!
//! Phase 10 — Packet Scheduler. See `DESIGN.md` §4.10.
//!
//! Per-tick, the scheduler decides what to put into the next
//! outgoing packet. RFC 9000 §13.3 and §17 list the rules:
//!
//! - ACK frames should be sent promptly
//! - Probe packets (PTO) are sent when no other data is available
//! - Retransmitted frames take priority over new data
//! - New application data is paced (token-bucket; see [`crate::pacing`])
//!
//! Multiple packets may be batched into a single UDP datagram
//! (coalescing) or multiple datagrams into a single `sendmmsg` /
//! GSO burst (see [`crate::gso`]).

use crate::stream::StreamId;

/// Phase-0 skeleton. Phase 10 will hold the priority queues and the
/// pacing-coupled emission loop.
#[derive(Default)]
pub struct PacketScheduler {
    /// Whether a probe packet is pending.
    probe_pending: bool,
    /// Number of ACK-eliciting packets sent since last ACK.
    ack_eliciting_outstanding: u32,
}

impl PacketScheduler {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            probe_pending: false,
            ack_eliciting_outstanding: 0,
        }
    }

    /// TODO(phase-10): `fn schedule(&mut self, conn: &mut QuicConnection,
    /// now_ns: u64) -> SchedulingPlan` returning the ordered list of
    /// frames to emit and which streams they come from.
    pub fn _placeholder(&self) -> Option<StreamId> {
        None
    }
}

/// Phase-10 plan returned by `schedule()`.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct SchedulingPlan {
    /// Number of ACK frames to send.
    pub acks: u32,
    /// Number of retransmissions to include.
    pub retransmits: u32,
    /// Number of probe packets to send.
    pub probes: u32,
    /// Streams whose data should be packed into this packet burst.
    pub streams: Vec<StreamId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let s = PacketScheduler::new();
        assert!(s._placeholder().is_none());
        // TODO(phase-10): full scheduler, pacing integration, GSO
        // batching, anti-amplification accounting.
    }
}

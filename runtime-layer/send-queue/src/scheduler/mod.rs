//! Scheduling policies (Layer 4).
//!
//! The scheduler decides the order in which queued packets are drained
//! and batched for submission to the IO Layer.
//!
//! # Phase 1: FIFO only
//!
//! The initial implementation provides [`FifoPolicy`], which drains the
//! ingress ring in submission order. Future phases will add
//! [`PriorityPolicy`], [`DeadlinePolicy`], and [`WeightedFairPolicy`].

use crate::config::SchedulePolicyKind;
use crate::queue::{IngressRing, PacketSlot};

/// Policy object that produces an ordered batch of packets to transmit.
///
/// The policy owns no packet data — it only reorders the drain sequence.
/// For [`FifoPolicy`], this is the ring's natural order. Future policies
/// (Priority, Deadline, WFQ) will maintain internal heaps or per-stream
/// queues.
pub trait SchedulePolicy: Send {
    /// Drains up to `max_count` packets from `ring` in the policy's
    /// desired transmission order.
    ///
    /// Returns the drained slots as a `Vec`. Callers submit them to the
    /// IO Layer via the [`SubmissionSink`](crate::submit::SubmissionSink).
    fn drain(&mut self, ring: &IngressRing, max_count: usize) -> Vec<PacketSlot>;
}

/// First-in-first-out scheduling — the ring's natural order.
#[derive(Debug, Default)]
pub struct FifoPolicy;

impl FifoPolicy {
    /// Creates a new FIFO policy.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl SchedulePolicy for FifoPolicy {
    fn drain(&mut self, ring: &IngressRing, max_count: usize) -> Vec<PacketSlot> {
        let mut out = Vec::with_capacity(max_count.min(ring.capacity()));
        while out.len() < max_count {
            match ring.pop() {
                Some(slot) => out.push(slot),
                None => break,
            }
        }
        out
    }
}

/// Constructs the policy implementation for a given [`SchedulePolicyKind`].
///
/// Non-FIFO policies fall back to FIFO in Phase 1; the `kind` is stored
/// for future use.
#[must_use]
pub fn build_policy(kind: SchedulePolicyKind) -> Box<dyn SchedulePolicy> {
    match kind {
        SchedulePolicyKind::Fifo => Box::new(FifoPolicy::new()),
        // Phase 3 will add real implementations; until then, FIFO is the
        // safe fallback for all kinds.
        SchedulePolicyKind::Priority
        | SchedulePolicyKind::Deadline
        | SchedulePolicyKind::WeightedFair => Box::new(FifoPolicy::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn make_slot(stream_id: u64) -> PacketSlot {
        let mut s = PacketSlot::empty();
        s.fill(
            stream_id,                // tracking_id
            (stream_id % 256) as u16, // buf_index
            64,                       // len
            "127.0.0.1:1".parse::<SocketAddr>().unwrap(),
            0,         // priority
            0,         // deadline_ns
            stream_id, // stream_id
        );
        s
    }

    #[test]
    fn fifo_drains_in_order() {
        let ring = IngressRing::new(16).expect("ring");
        for i in 0..8 {
            ring.push(make_slot(i)).expect("push");
        }
        let mut policy = FifoPolicy::new();
        let drained = policy.drain(&ring, 4);
        assert_eq!(drained.len(), 4);
        for (i, slot) in drained.iter().enumerate() {
            assert_eq!(slot.stream_id, i as u64);
        }
    }

    #[test]
    fn fifo_drains_all_when_fewer_than_max() {
        let ring = IngressRing::new(16).expect("ring");
        ring.push(make_slot(1)).expect("push");
        ring.push(make_slot(2)).expect("push");
        let mut policy = FifoPolicy::new();
        let drained = policy.drain(&ring, 10);
        assert_eq!(drained.len(), 2);
    }

    #[test]
    fn fifo_empty_ring_returns_empty() {
        let ring = IngressRing::new(16).expect("ring");
        let mut policy = FifoPolicy::new();
        let drained = policy.drain(&ring, 10);
        assert!(drained.is_empty());
    }

    #[test]
    fn build_policy_returns_boxed_fifo() {
        let _p = build_policy(SchedulePolicyKind::Fifo);
        let _p = build_policy(SchedulePolicyKind::Priority);
    }
}

//! [`StreamScheduler`] — round-robin / priority / weighted-fair
//! queuing across streams within a connection.
//!
//! Phase 8 — Stream Manager (concurrency half). See `DESIGN.md`
//! §4.8.
//!
//! RFC 9000 does not specify stream scheduling — it's a server-side
//! decision. Vireon supports several policies:
//!
//! - **RoundRobin** (default) — cycles through ready streams, fair
//! - **Priority** — strict priority, lower priority starves
//! - **WeightedFair** — fluid-fair queuing (FFQ), each stream gets
//!   a configurable weight
//!
//! The scheduler is invoked from the [`crate::packet_scheduler`]
//! (Phase 10) when building outgoing packets: it picks which
//! streams' bytes go into the next STREAM frame.

/// Scheduling policy. See module docs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchedulerPolicy {
    /// Simple round-robin over ready streams.
    #[default]
    RoundRobin,
    /// Strict priority (lower numeric value = higher priority).
    Priority,
    /// Weighted-fair queuing.
    WeightedFair,
}

/// Phase-0 skeleton. Phase 8 will hold the ready-queue (heap or
/// list) and the per-stream weights.
#[derive(Default)]
pub struct StreamScheduler {
    policy: SchedulerPolicy,
    /// Number of streams currently in the ready queue.
    ready_count: usize,
}

impl StreamScheduler {
    #[must_use]
    pub const fn new(policy: SchedulerPolicy) -> Self {
        Self {
            policy,
            ready_count: 0,
        }
    }

    /// TODO(phase-8): `fn next_ready(&mut self) -> Option<StreamId>`.
    pub fn _placeholder(&self) {}

    /// Returns the configured policy.
    #[must_use]
    pub const fn policy(&self) -> SchedulerPolicy {
        self.policy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let s = StreamScheduler::new(SchedulerPolicy::RoundRobin);
        assert_eq!(s.policy(), SchedulerPolicy::RoundRobin);
        // TODO(phase-8): implement RR + Priority + WFQ, micro-bench
        // against stream count scaling.
    }
}

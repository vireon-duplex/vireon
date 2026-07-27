//! Scheduler bridge (Layer 12).
//!
//! The CQE Processor does not call into the Task Queue directly from
//! dispatch handlers (that would require a `&mut TaskQueue` in scope,
//! which the dispatch table signature does not allow). Instead, handlers
//! set `Outcome::schedule_task = true` and the processor accumulates
//! [`ScheduledTask`] descriptors for the reactor to drain after `tick()`.

use io::completion::Completion;

/// A descriptor for a task that should be spawned on the Task Queue in
/// response to a completion. The reactor (or test) interprets the
/// descriptor and constructs the actual `impl Task`.
#[derive(Clone, Debug)]
pub struct ScheduledTask {
    /// The completion that triggered this schedule.
    pub completion: Completion,
    /// When the schedule was emitted (monotonic nanos).
    pub emitted_at_ns: u64,
}

/// Accumulator for scheduled tasks across a single drain batch.
pub struct CqeScheduler {
    pending: Vec<ScheduledTask>,
}

impl CqeScheduler {
    /// Construct with a pre-allocated capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            pending: Vec::with_capacity(capacity.max(8)),
        }
    }

    /// Enqueue a scheduled task. Called by handlers via the processor.
    #[inline]
    pub fn push(&mut self, completion: Completion, now_ns: u64) {
        self.pending.push(ScheduledTask {
            completion,
            emitted_at_ns: now_ns,
        });
    }

    /// Drain all pending scheduled tasks.
    #[must_use]
    pub fn drain(&mut self) -> Vec<ScheduledTask> {
        std::mem::take(&mut self.pending)
    }

    /// Pending count.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

impl std::fmt::Debug for CqeScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CqeScheduler")
            .field("pending", &self.pending.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use io::ops::OpKind;
    use io::ops::encode;

    #[test]
    fn push_and_drain_round_trip() {
        let mut s = CqeScheduler::new(16);
        let c = Completion::Timeout {
            id: encode(OpKind::Timeout, 5),
            more: false,
        };
        s.push(c.clone(), 0);
        assert_eq!(s.pending_count(), 1);
        let drained = s.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].emitted_at_ns, 0);
        assert_eq!(s.pending_count(), 0);
    }
}

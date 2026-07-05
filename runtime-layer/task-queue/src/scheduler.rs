//! TaskScheduler (Layer 15).
//!
//! Orchestrates per-tick work: select a batch via the active
//! [`TaskPolicy`], apply aging promotions, and expire stale tasks.

use crate::aging::AgingTracker;
use crate::batch::TaskBatch;
use crate::budget::Budget;
use crate::config::PolicyKind;
use crate::metrics::MetricsCollector;
use crate::policy::{FifoPolicy, TaskPolicy};
use crate::pool::TaskPool;
use crate::ready::ReadyQueue;

/// Builds a boxed [`TaskPolicy`] from a [`PolicyKind`].
pub fn build_policy(kind: PolicyKind) -> Box<dyn TaskPolicy> {
    match kind {
        PolicyKind::Fifo | PolicyKind::PriorityBandedFifo | PolicyKind::Deadline => {
            // Phase 1 ships FIFO only; the other kinds fall back to it.
            Box::new(FifoPolicy::new())
        }
    }
}

/// Per-tick scheduler. Owns the active policy and the aging tracker.
pub struct TaskScheduler {
    policy: Box<dyn TaskPolicy>,
    aging: AgingTracker,
}

impl TaskScheduler {
    /// Creates a scheduler from the runtime config.
    #[must_use]
    pub fn new(kind: PolicyKind, aging_threshold_ns: u64) -> Self {
        Self {
            policy: build_policy(kind),
            aging: AgingTracker::new(aging_threshold_ns),
        }
    }

    /// Returns the policy's name.
    #[must_use]
    pub fn policy_name(&self) -> &'static str {
        self.policy.name()
    }

    /// Returns the aging threshold in nanoseconds.
    #[must_use]
    pub fn aging_threshold_ns(&self) -> u64 {
        self.aging.threshold_ns()
    }

    /// Applies aging and selects a batch. Writes slot indices into
    /// `batch` and returns the count.
    pub fn select(
        &mut self,
        ready: &mut ReadyQueue,
        budget: Budget,
        batch: &mut TaskBatch,
        now_ns: u64,
    ) -> usize {
        let promoted = self.aging.apply(ready, now_ns);
        if promoted > 0 {
            // Could be exposed via a return value for metrics.
        }
        self.policy.select(ready, budget, batch)
    }

    /// Expires tasks past their deadline. Returns the count dropped.
    pub fn expire(
        &self,
        ready: &mut ReadyQueue,
        pool: &mut TaskPool,
        metrics: &mut MetricsCollector,
        now_ns: u64,
    ) -> usize {
        // Walk the queue in band-priority order, collecting expired slots.
        // The ReadyQueue doesn't expose a scan API directly, so we drain
        // and re-push — O(n) per tick but n is small (batch-sized).
        let mut expired = 0usize;
        for band in crate::priority::TaskPriority::ALL {
            let depth = ready.depth_of(band) as usize;
            if depth == 0 {
                continue;
            }
            let mut keep = Vec::with_capacity(depth);
            while let Some(slot_idx) = ready.pop_from(band) {
                if let Some(slot) = pool.get(slot_idx) {
                    if slot.metadata.is_expired(now_ns) {
                        pool.free(slot_idx);
                        metrics.record_expired();
                        expired += 1;
                        continue;
                    }
                }
                keep.push(slot_idx);
            }
            for slot_idx in keep {
                let _ = ready.push(band, slot_idx, now_ns);
            }
        }
        expired
    }
}

impl std::fmt::Debug for TaskScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskScheduler")
            .field("policy", &self.policy_name())
            .field("aging_threshold_ns", &self.aging.threshold_ns())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskContext;
    use crate::metadata::TaskMetadata;
    use crate::priority::TaskPriority;
    use crate::task::{Task, TaskResult};

    struct Noop;
    impl Task for Noop {
        fn run(&mut self, _ctx: &mut TaskContext) -> TaskResult {
            TaskResult::Done
        }
    }

    #[test]
    fn select_returns_in_priority_order() {
        let mut sched = TaskScheduler::new(PolicyKind::Fifo, 1_000_000);
        let mut ready = ReadyQueue::new(16);
        let mut batch = TaskBatch::new(16);

        ready.push(TaskPriority::Low, 1, 0).expect("push");
        ready.push(TaskPriority::High, 2, 0).expect("push");

        let n = sched.select(&mut ready, Budget::new(16, 1_000_000), &mut batch, 0);
        assert_eq!(n, 2);
        assert_eq!(batch.as_slice(), &[2, 1]); // High first
    }

    #[test]
    fn expire_drops_past_deadline() {
        let sched = TaskScheduler::new(PolicyKind::Fifo, 1_000_000);
        let mut ready = ReadyQueue::new(16);
        let mut pool = TaskPool::new(16);
        let mut metrics = MetricsCollector::new();

        // Slot 1 has deadline 100; we'll expire at now=200.
        let meta = TaskMetadata::new(0).with_deadline(100);
        let (_h, slot) = pool
            .alloc(Box::new(Noop), TaskPriority::Normal, meta, 0)
            .expect("alloc");
        ready.push(TaskPriority::Normal, slot, 0).expect("push");
        // Slot 2 has no deadline; survives.
        let (_h2, slot2) = pool
            .alloc(
                Box::new(Noop),
                TaskPriority::Normal,
                TaskMetadata::new(0),
                0,
            )
            .expect("alloc");
        ready.push(TaskPriority::Normal, slot2, 0).expect("push");

        let expired = sched.expire(&mut ready, &mut pool, &mut metrics, 200);
        assert_eq!(expired, 1);
        assert_eq!(metrics.expired_total(), 1);
        assert_eq!(ready.depth(), 1);
    }

    #[test]
    fn build_policy_returns_boxed_fifo() {
        let p = build_policy(PolicyKind::Fifo);
        assert_eq!(p.name(), "fifo");
    }
}

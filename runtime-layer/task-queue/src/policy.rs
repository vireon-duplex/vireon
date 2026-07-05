//! TaskPolicy trait + FifoPolicy (Layer 14).
//!
//! The scheduler delegates the choice of which ready tasks to run to
//! a [`TaskPolicy`] implementation. Phase 1 ships [`FifoPolicy`];
//! Phase 2 will add `PriorityBandedFifoPolicy` and `DeadlinePolicy`.

use crate::batch::TaskBatch;
use crate::budget::Budget;
use crate::ready::ReadyQueue;

/// Pluggable scheduling strategy.
///
/// Implementations receive a mutable reference to the ready queue and
/// a scratch batch buffer; they fill the batch with up to
/// `budget.max_tasks` slot indices in the order they should execute.
pub trait TaskPolicy: Send {
    /// Human-readable name for metrics/logging.
    fn name(&self) -> &'static str;

    /// Fills `batch` with up to `budget.max_tasks` slot indices from
    /// `ready`. Returns the number of indices written.
    fn select(&mut self, ready: &mut ReadyQueue, budget: Budget, batch: &mut TaskBatch) -> usize;
}

/// Single-band strict FIFO policy. Drains the ready queue in priority
/// order (High → Background), FIFO within each band.
pub struct FifoPolicy;

impl FifoPolicy {
    /// Creates a new FIFO policy.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for FifoPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskPolicy for FifoPolicy {
    fn name(&self) -> &'static str {
        "fifo"
    }

    fn select(&mut self, ready: &mut ReadyQueue, budget: Budget, batch: &mut TaskBatch) -> usize {
        batch.clear();
        let max = budget.max_tasks as usize;
        while batch.len() < max {
            match ready.pop_next() {
                Some((slot_idx, _band)) => {
                    if !batch.push(slot_idx) {
                        break;
                    }
                }
                None => break,
            }
        }
        batch.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::priority::TaskPriority;

    #[test]
    fn fifo_drains_in_priority_order() {
        let mut ready = ReadyQueue::new(16);
        ready.push(TaskPriority::Low, 30, 0).expect("push");
        ready.push(TaskPriority::Normal, 20, 0).expect("push");
        ready.push(TaskPriority::High, 10, 0).expect("push");

        let mut policy = FifoPolicy::new();
        let mut batch = TaskBatch::new(16);
        let n = policy.select(&mut ready, Budget::new(16, 1_000_000), &mut batch);
        assert_eq!(n, 3);
        assert_eq!(batch.as_slice(), &[10, 20, 30]);
    }

    #[test]
    fn fifo_respects_budget() {
        let mut ready = ReadyQueue::new(16);
        for i in 0..10 {
            ready.push(TaskPriority::Normal, i, 0).expect("push");
        }
        let mut policy = FifoPolicy::new();
        let mut batch = TaskBatch::new(16);
        let n = policy.select(&mut ready, Budget::new(3, 1_000_000), &mut batch);
        assert_eq!(n, 3);
        assert_eq!(batch.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn fifo_empty_ready() {
        let mut ready = ReadyQueue::new(16);
        let mut policy = FifoPolicy::new();
        let mut batch = TaskBatch::new(16);
        let n = policy.select(&mut ready, Budget::new(16, 1_000_000), &mut batch);
        assert_eq!(n, 0);
    }

    #[test]
    fn name() {
        assert_eq!(FifoPolicy::new().name(), "fifo");
    }
}

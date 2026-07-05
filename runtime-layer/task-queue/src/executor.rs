//! TaskExecutor — batch run + recycle (Layer 16).

use std::time::Instant;

use crate::batch::TaskBatch;
use crate::budget::Budget;
use crate::context::TaskContext;
use crate::metrics::MetricsCollector;
use crate::pool::TaskPool;
use crate::priority::TaskPriority;
use crate::task::TaskResult;

/// Outcome of one batch execution.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExecuteResult {
    /// Tasks that ran to `Done`.
    pub executed: u16,
    /// Tasks that returned `Error`.
    pub failed: u16,
    /// Tasks that were cancelled (skipped).
    pub cancelled: u16,
    /// Tasks that requested re-scheduling.
    pub rescheduled: u16,
    /// Total wall-clock time spent in this batch (ns).
    pub time_elapsed_ns: u64,
}

impl ExecuteResult {
    /// Total tasks processed (any outcome).
    #[must_use]
    pub const fn total(self) -> u16 {
        self.executed + self.failed + self.cancelled + self.rescheduled
    }
}

/// Drains a batch of tasks, calling `run()` on each within the budget.
pub struct TaskExecutor {
    /// Reusable buffer to collect slot indices that requested
    /// immediate re-scheduling (delay == ZERO).
    pub(crate) reschedule_buf: Vec<(u32, TaskPriority)>,
}

impl TaskExecutor {
    /// Creates an executor with a scratch sized to `batch_capacity`.
    #[must_use]
    pub fn new(batch_capacity: usize) -> Self {
        Self {
            reschedule_buf: Vec::with_capacity(batch_capacity.max(1)),
        }
    }

    /// Runs the batch. Returns execution statistics.
    ///
    /// `batch` contains slot indices to execute. The executor checks
    /// cancellation, runs the task, and recycles the slot (or
    /// re-enqueues it for rescheduled tasks).
    ///
    /// `now_ns` is the reactor's monotonic timestamp at tick start;
    /// the executor measures elapsed time with `Instant::now()` to
    /// honor the time budget.
    pub fn run_batch(
        &mut self,
        batch: &TaskBatch,
        pool: &mut TaskPool,
        ctx: &mut TaskContext,
        budget: Budget,
        now_ns: u64,
        metrics: &mut MetricsCollector,
        // Re-enqueue hook: called for `Reschedule { delay: ZERO }`.
        // The closure receives `(slot_idx, priority)`.
        mut reenqueue: impl FnMut(u32, TaskPriority),
    ) -> ExecuteResult {
        self.reschedule_buf.clear();
        let start = Instant::now();
        let mut executed = 0u16;
        let mut failed = 0u16;
        let mut cancelled = 0u16;
        let mut rescheduled = 0u16;

        for &slot_idx in batch.as_slice() {
            let elapsed = start.elapsed().as_nanos() as u64;
            if budget.is_exhausted(executed + failed + cancelled + rescheduled, elapsed) {
                break;
            }

            // Check cancellation via the slot's shared flag.
            let is_cancelled = {
                let Some(slot) = pool.get(slot_idx) else {
                    continue;
                };
                slot.cancel_flag.load(std::sync::atomic::Ordering::Acquire)
            };
            if is_cancelled {
                pool.free(slot_idx);
                metrics.record_cancelled();
                cancelled += 1;
                continue;
            }

            // Mark executing + run.
            let result = {
                let Some(slot) = pool.get_mut(slot_idx) else {
                    continue;
                };
                slot.state = crate::state::TaskState::Executing;
                let task = match slot.task.as_mut() {
                    Some(t) => t,
                    None => continue,
                };
                task.run(ctx)
            };

            match result {
                TaskResult::Done => {
                    pool.free(slot_idx);
                    metrics.record_executed();
                    executed += 1;
                }
                TaskResult::Error(_e) => {
                    pool.free(slot_idx);
                    metrics.record_failed();
                    failed += 1;
                }
                TaskResult::Reschedule { delay, priority } => {
                    if delay.is_zero() {
                        // Re-enqueue immediately; defer the actual push
                        // because we hold a `&mut pool` borrow.
                        pool.reschedule_in_place(slot_idx, priority, now_ns);
                        self.reschedule_buf.push((slot_idx, priority));
                        metrics.record_rescheduled();
                        rescheduled += 1;
                    } else {
                        // Non-zero delay would normally go through the
                        // Timer Wheel. For Phase 1 (no Timer Wheel
                        // integration in this crate) we treat it the
                        // same as immediate reschedule and let the
                        // caller drive timing externally.
                        pool.reschedule_in_place(slot_idx, priority, now_ns);
                        self.reschedule_buf.push((slot_idx, priority));
                        metrics.record_rescheduled();
                        rescheduled += 1;
                    }
                }
            }
        }

        // Now that the pool borrow is released, perform re-enqueues.
        for (slot_idx, priority) in self.reschedule_buf.drain(..) {
            reenqueue(slot_idx, priority);
        }

        let time_elapsed_ns = start.elapsed().as_nanos() as u64;
        ExecuteResult {
            executed,
            failed,
            cancelled,
            rescheduled,
            time_elapsed_ns,
        }
    }
}

impl std::fmt::Debug for TaskExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskExecutor")
            .field("reschedule_buf_cap", &self.reschedule_buf.capacity())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::TaskMetadata;
    use crate::priority::TaskPriority;
    use crate::task::{Task, TaskResult};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct Counting(Arc<AtomicU32>);
    impl Task for Counting {
        fn run(&mut self, _ctx: &mut TaskContext) -> TaskResult {
            self.0.fetch_add(1, Ordering::Relaxed);
            TaskResult::Done
        }
    }

    struct Failing;
    impl Task for Failing {
        fn run(&mut self, _ctx: &mut TaskContext) -> TaskResult {
            TaskResult::Error(crate::error::TaskError::TaskFailed("oops".into()))
        }
    }

    struct YieldTwice {
        remaining: u8,
    }
    impl Task for YieldTwice {
        fn run(&mut self, _ctx: &mut TaskContext) -> TaskResult {
            if self.remaining > 0 {
                self.remaining -= 1;
                TaskResult::yield_next(crate::priority::TaskPriority::Low)
            } else {
                TaskResult::Done
            }
        }
    }

    #[test]
    fn run_batch_executes_and_frees() {
        let mut pool = TaskPool::new(8);
        let counter = Arc::new(AtomicU32::new(0));
        let mut batch = TaskBatch::new(8);
        let mut metrics = MetricsCollector::new();

        for _ in 0..3 {
            let (_h, slot) = pool
                .alloc(
                    Box::new(Counting(Arc::clone(&counter))),
                    TaskPriority::Normal,
                    TaskMetadata::default(),
                    0,
                )
                .expect("alloc");
            batch.push(slot);
        }

        let mut exec = TaskExecutor::new(8);
        let mut ctx = TaskContext::new(0, 0);
        let result = exec.run_batch(
            &batch,
            &mut pool,
            &mut ctx,
            Budget::new(16, 1_000_000_000),
            0,
            &mut metrics,
            |_, _| {},
        );
        assert_eq!(result.executed, 3);
        assert_eq!(counter.load(Ordering::Relaxed), 3);
        assert_eq!(pool.free_count(), 8);
        assert_eq!(metrics.executed_total(), 3);
    }

    #[test]
    fn run_batch_handles_failure() {
        let mut pool = TaskPool::new(8);
        let mut batch = TaskBatch::new(8);
        let mut metrics = MetricsCollector::new();

        let (_h, slot) = pool
            .alloc(
                Box::new(Failing),
                TaskPriority::Normal,
                TaskMetadata::default(),
                0,
            )
            .expect("alloc");
        batch.push(slot);

        let mut exec = TaskExecutor::new(8);
        let mut ctx = TaskContext::new(0, 0);
        let result = exec.run_batch(
            &batch,
            &mut pool,
            &mut ctx,
            Budget::new(16, 1_000_000_000),
            0,
            &mut metrics,
            |_, _| {},
        );
        assert_eq!(result.failed, 1);
        assert_eq!(result.executed, 0);
        assert_eq!(metrics.failed_total(), 1);
    }

    #[test]
    fn run_batch_handles_reschedule() {
        let mut pool = TaskPool::new(8);
        let mut batch = TaskBatch::new(8);
        let mut metrics = MetricsCollector::new();
        let mut ready = crate::ready::ReadyQueue::new(8);

        let (_h, slot) = pool
            .alloc(
                Box::new(YieldTwice { remaining: 1 }),
                TaskPriority::Normal,
                TaskMetadata::default(),
                0,
            )
            .expect("alloc");
        batch.push(slot);

        let mut exec = TaskExecutor::new(8);
        let mut ctx = TaskContext::new(0, 0);
        let result = exec.run_batch(
            &batch,
            &mut pool,
            &mut ctx,
            Budget::new(16, 1_000_000_000),
            0,
            &mut metrics,
            |slot_idx, priority| {
                ready.push(priority, slot_idx, 0).expect("push");
            },
        );
        assert_eq!(result.rescheduled, 1);
        // Slot still live (re-enqueued).
        assert_eq!(ready.depth(), 1);
    }

    #[test]
    fn budget_stops_mid_batch() {
        let mut pool = TaskPool::new(8);
        let counter = Arc::new(AtomicU32::new(0));
        let mut batch = TaskBatch::new(8);
        let mut metrics = MetricsCollector::new();

        for _ in 0..5 {
            let (_h, slot) = pool
                .alloc(
                    Box::new(Counting(Arc::clone(&counter))),
                    TaskPriority::Normal,
                    TaskMetadata::default(),
                    0,
                )
                .expect("alloc");
            batch.push(slot);
        }

        let mut exec = TaskExecutor::new(8);
        let mut ctx = TaskContext::new(0, 0);
        // Budget = 3 tasks.
        let result = exec.run_batch(
            &batch,
            &mut pool,
            &mut ctx,
            Budget::new(3, 1_000_000_000),
            0,
            &mut metrics,
            |_, _| {},
        );
        assert_eq!(result.executed, 3);
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }
}

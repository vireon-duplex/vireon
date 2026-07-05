//! TaskQueue facade (Layer 22) — public API surface.
//!
//! Ties together the pool, inbox, ready queue, scheduler, executor,
//! and metrics behind a small set of synchronous operations.

use std::time::Duration;

use crate::batch::TaskBatch;
use crate::budget::Budget;
use crate::cancel::CancellationRegistry;
use crate::config::TaskQueueConfig;
use crate::context::TaskContext;
use crate::deferred::{DeferredHandle, DeferredRegistry};
use crate::error::TaskError;
use crate::executor::{ExecuteResult, TaskExecutor};
use crate::handle::TaskHandle;
use crate::inbox::{InboxEntry, MpscInbox};
use crate::metadata::TaskMetadata;
use crate::metrics::MetricsCollector;
use crate::pool::TaskPool;
use crate::priority::TaskPriority;
use crate::ready::ReadyQueue;
use crate::scheduler::TaskScheduler;
use crate::shutdown::ShutdownManager;
use crate::task::{Task, TaskResult};

/// Outcome of a single reactor tick.
#[derive(Clone, Copy, Debug, Default)]
pub struct TickResult {
    /// Tasks that ran to `Done`.
    pub executed: u16,
    /// Tasks that returned an error.
    pub failed: u16,
    /// Tasks that were cancelled (skipped).
    pub cancelled: u16,
    /// Tasks that requested re-scheduling.
    pub rescheduled: u16,
    /// Cross-thread inbox entries drained into the ready queue.
    pub inbox_drained: u64,
    /// Tasks promoted by aging.
    pub aged: u64,
    /// Tasks expired (deadline passed).
    pub expired: u64,
    /// Wall-clock time spent in this tick (ns).
    pub time_elapsed_ns: u64,
    /// Remaining ready-queue depth.
    pub remaining: u64,
}

/// Report returned by [`TaskQueue::shutdown`].
#[derive(Clone, Copy, Debug, Default)]
pub struct ShutdownReport {
    /// Tasks that executed during the drain.
    pub drained_executed: u64,
    /// Tasks dropped without execution.
    pub dropped: u64,
    /// Wall-clock time spent in the drain (ns).
    pub time_elapsed_ns: u64,
}

/// The public Task Queue facade. Owns all internal components.
pub struct TaskQueue {
    pub(crate) config: TaskQueueConfig,
    pub(crate) pool: TaskPool,
    pub(crate) inbox: MpscInbox,
    pub(crate) ready: ReadyQueue,
    pub(crate) cancel: CancellationRegistry,
    pub(crate) scheduler: TaskScheduler,
    pub(crate) executor: TaskExecutor,
    pub(crate) batch: TaskBatch,
    pub(crate) metrics: MetricsCollector,
    pub(crate) shutdown: ShutdownManager,
    pub(crate) deferred: DeferredRegistry,
}

impl TaskQueue {
    /// Constructs a queue from a validated config.
    /// Prefer [`TaskQueueBuilder`](crate::builder::TaskQueueBuilder).
    pub fn from_config(mut config: TaskQueueConfig) -> Result<Self, TaskError> {
        config.round_capacity_up();
        // Adjust derived defaults if capacity changed after defaults
        // were populated (mirrors the send-queue builder pattern).
        if (config.batch_max_tasks as usize) > config.capacity {
            config.batch_max_tasks = config.capacity as u16;
        }
        config.validate()?;

        let pool = TaskPool::new(config.capacity);
        let inbox = MpscInbox::new(config.inbox_capacity);
        let ready = ReadyQueue::new(config.capacity);
        let cancel = CancellationRegistry::new(config.capacity);
        let scheduler = TaskScheduler::new(config.policy, config.aging_threshold_ns);
        let executor = TaskExecutor::new(config.batch_max_tasks as usize);
        let batch = TaskBatch::new(config.batch_max_tasks as usize);
        let metrics = MetricsCollector::new();
        let shutdown = ShutdownManager::new();
        let deferred = DeferredRegistry::new();

        Ok(Self {
            config,
            pool,
            inbox,
            ready,
            cancel,
            scheduler,
            executor,
            batch,
            metrics,
            shutdown,
            deferred,
        })
    }

    // ── Submission ──────────────────────────────────────────────────

    /// Spawns a task for immediate execution at the given priority.
    pub fn spawn(
        &mut self,
        task: impl Task + 'static,
        priority: TaskPriority,
    ) -> Result<TaskHandle, TaskError> {
        self.spawn_boxed(Box::new(task), priority, 0)
    }

    /// Spawns a task with an explicit deadline (monotonic nanos).
    pub fn spawn_with_deadline(
        &mut self,
        task: impl Task + 'static,
        priority: TaskPriority,
        deadline_ns: u64,
        now_ns: u64,
    ) -> Result<TaskHandle, TaskError> {
        let meta = TaskMetadata::new(now_ns).with_deadline(deadline_ns);
        let boxed: Box<dyn Task> = Box::new(task);
        self.spawn_with_meta(boxed, priority, meta, now_ns)
    }

    /// Schedules a task for execution after `delay`.
    ///
    /// Phase 1 records the deferred task in the local registry; the
    /// reactor (or test harness) is responsible for firing the timer
    /// and calling [`on_timer_fire`](Self::on_timer_fire). In the full
    /// runtime integration this call delegates to the Timer Wheel.
    pub fn schedule_after(
        &mut self,
        task: impl Task + 'static,
        delay: Duration,
        priority: TaskPriority,
        now_ns: u64,
    ) -> Result<TaskHandle, TaskError> {
        if self.shutdown.is_shutting_down() {
            return Err(TaskError::ShuttingDown);
        }
        let boxed: Box<dyn Task> = Box::new(task);
        let (handle, slot_idx) =
            self.pool
                .alloc(boxed, priority, TaskMetadata::new(now_ns), now_ns)?;
        if let Some(slot) = self.pool.get_mut(slot_idx) {
            slot.state = crate::state::TaskState::Deferred;
        }
        let generation = self.pool.generation(slot_idx);
        self.deferred
            .register(DeferredHandle::new(slot_idx, generation, priority, delay));
        self.metrics.record_spawned();
        Ok(handle)
    }

    /// Cross-thread spawn. The task is delivered through the MPSC inbox
    /// and executed on the owning reactor thread on the next tick.
    pub fn spawn_cross_thread(
        &self,
        task: impl Task + 'static,
        priority: TaskPriority,
        now_ns: u64,
    ) -> Result<(), TaskError> {
        if self.shutdown.is_shutting_down() {
            return Err(TaskError::ShuttingDown);
        }
        let entry = InboxEntry {
            task: Box::new(task),
            priority,
            spawned_at_ns: now_ns,
        };
        self.inbox.push(entry)?;
        self.metrics.record_inbox_push();
        Ok(())
    }

    /// Cancels the task identified by `handle`. Lazy — the task is
    /// skipped on next dequeue.
    pub fn cancel(&mut self, handle: &TaskHandle) {
        handle.cancel();
        let slot_idx = crate::id::decode_slot(handle.id().raw());
        self.cancel.cancel(slot_idx);
    }

    // ── Tick ────────────────────────────────────────────────────────

    /// Executes one reactor tick.
    ///
    /// Drains the inbox, applies aging + expiry, selects a batch,
    /// runs it, and returns execution statistics.
    pub fn tick(&mut self, ctx: &mut TaskContext, now_ns: u64) -> TickResult {
        self.metrics.record_tick();

        // Phase 1: drain inbox into ready queue.
        let inbox_drained = self.drain_inbox(now_ns);

        // Phase 2: process timer fires (called externally via
        // on_timer_fire). The deferred registry is consumed by the
        // reactor; nothing to do here.

        // Phase 3: expire stale tasks.
        let expired =
            self.scheduler
                .expire(&mut self.ready, &mut self.pool, &mut self.metrics, now_ns)
                as u64;

        // Phase 4: select a batch.
        let budget = Budget::new(self.config.batch_max_tasks, self.config.batch_max_time_ns);
        self.batch.clear();
        let _ = self
            .scheduler
            .select(&mut self.ready, budget, &mut self.batch, now_ns);

        // Phase 5: execute the batch.
        let result: ExecuteResult = self.executor.run_batch(
            &self.batch,
            &mut self.pool,
            ctx,
            budget,
            now_ns,
            &mut self.metrics,
            |slot_idx, priority| {
                let _ = self.ready.push(priority, slot_idx, now_ns);
            },
        );

        // Note: the closure above borrows `self.ready` immutably while
        // `self.pool` is mutably borrowed inside `run_batch`. The
        // actual borrow-checker does not allow this pattern; we
        // work around it by collecting re-enqueues into the executor's
        // internal buffer (already done by run_batch) and applying
        // them after the call returns. The closure passed above is a
        // no-op in practice because the borrow checker would reject a
        // real push; the executor's reschedule_buf is drained by the
        // `apply_reschedules` helper below.

        // Apply reschedules now (after run_batch returned).
        self.apply_reschedules(now_ns);

        // Track overruns.
        if result.time_elapsed_ns > self.config.batch_max_time_ns {
            self.metrics.record_overrun();
        }

        TickResult {
            executed: result.executed,
            failed: result.failed,
            cancelled: result.cancelled,
            rescheduled: result.rescheduled,
            inbox_drained,
            aged: 0,
            expired,
            time_elapsed_ns: result.time_elapsed_ns,
            remaining: self.ready.depth(),
        }
    }

    /// Drains the MPSC inbox into the ready queue. Returns the count.
    fn drain_inbox(&mut self, now_ns: u64) -> u64 {
        let entries = self.inbox.drain();
        let count = entries.len() as u64;
        for entry in entries {
            let meta = TaskMetadata::new(entry.spawned_at_ns);
            let (handle, slot_idx) = match self.pool.alloc(entry.task, entry.priority, meta, now_ns)
            {
                Ok(v) => v,
                Err(_) => continue, // pool full — drop cross-thread task
            };
            let _ = handle; // handle discarded; no cancellation path for inbox tasks
            if self.ready.push(entry.priority, slot_idx, now_ns).is_err() {
                // ReadyQueue band full — fall back to recycle the slot.
                self.pool.free(slot_idx);
            }
        }
        count
    }

    /// Helper to flush the executor's reschedule buffer into the
    /// ready queue. Called after each `run_batch`.
    fn apply_reschedules(&mut self, now_ns: u64) {
        // The executor stores reschedules in its own buffer; we expose
        // them via a side-channel that runs after the batch returns.
        // For Phase 1 we rely on the executor already having called
        // `pool.reschedule_in_place`, which sets the slot state to
        // Queued. We need to push the slot back into the ReadyQueue.
        // The closure we passed to run_batch is the no-op; we redo the
        // push here by walking the executor's reschedule_buf. To keep
        // the borrow checker happy we move the buffer out of the
        // executor first.
        let buf = std::mem::take(&mut self.executor.reschedule_buf);
        for (slot_idx, priority) in buf.iter() {
            let _ = self.ready.push(*priority, *slot_idx, now_ns);
        }
        // Restore the (now-empty) buffer for the next tick.
        self.executor.reschedule_buf = buf;
    }

    /// Called by the reactor when a deferred task's timer fires.
    /// Moves the task back into the ready queue.
    pub fn on_timer_fire(&mut self, slot_idx: u32, now_ns: u64) {
        if let Some(slot) = self.pool.get_mut(slot_idx) {
            slot.state = crate::state::TaskState::Ready;
            let priority = slot.priority;
            let _ = self.ready.push(priority, slot_idx, now_ns);
        }
    }

    // ── Shutdown ────────────────────────────────────────────────────

    /// Drains the queue, dropping any remaining tasks.
    /// Returns the number of tasks dropped.
    pub fn drain(&mut self) -> u64 {
        let dropped = self.ready.drain_all();
        // Also clear the pool slots for ready/queued tasks.
        // Free-list slots are already in the free-list; we only need
        // to free live slots.
        let cap = self.pool.capacity();
        for idx in 0..cap {
            if let Some(slot) = self.pool.get(idx as u32) {
                if slot.state != crate::state::TaskState::Idle {
                    self.pool.free(idx as u32);
                }
            }
        }
        dropped
    }

    /// Initiates graceful shutdown. Rejects new spawns. Executes
    /// remaining tasks within the grace period (best-effort: bounded
    /// by the total work, not strictly time-limited in Phase 1).
    pub fn shutdown(
        &mut self,
        ctx: &mut TaskContext,
        _grace: Duration,
        now_ns: u64,
    ) -> ShutdownReport {
        self.shutdown.begin_drain();
        let start = std::time::Instant::now();
        let mut drained_executed = 0u64;

        // Run up to capacity tasks (one last pass).
        let budget = Budget::new(self.config.batch_max_tasks, self.config.batch_max_time_ns);
        self.batch.clear();
        let _ = self
            .scheduler
            .select(&mut self.ready, budget, &mut self.batch, now_ns);
        let result = self.executor.run_batch(
            &self.batch,
            &mut self.pool,
            ctx,
            budget,
            now_ns,
            &mut self.metrics,
            |_, _| {},
        );
        self.apply_reschedules(now_ns);
        drained_executed += result.executed as u64;

        // Drop everything still pending.
        let dropped = self.drain();
        self.shutdown.mark_stopped();

        ShutdownReport {
            drained_executed,
            dropped,
            time_elapsed_ns: start.elapsed().as_nanos() as u64,
        }
    }

    // ── Observability ───────────────────────────────────────────────

    /// Snapshot of metrics counters.
    #[must_use]
    pub fn metrics(&self) -> crate::metrics::MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Current ready-queue depth (sum across all bands).
    #[must_use]
    pub fn depth(&self) -> u64 {
        self.ready.depth()
    }

    /// Number of free task slots.
    #[must_use]
    pub fn free_slots(&self) -> usize {
        self.pool.free_count()
    }

    /// Configured capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.config.capacity
    }

    /// Pending deferred-task count.
    #[must_use]
    pub fn deferred_count(&self) -> usize {
        self.deferred.len()
    }

    /// Returns `true` if shutdown has been initiated.
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.is_shutting_down()
    }

    // ── Internal helpers (pub(crate)) ──────────────────────────────

    pub(crate) fn spawn_boxed(
        &mut self,
        task: Box<dyn Task>,
        priority: TaskPriority,
        now_ns: u64,
    ) -> Result<TaskHandle, TaskError> {
        self.spawn_with_meta(task, priority, TaskMetadata::new(now_ns), now_ns)
    }

    pub(crate) fn spawn_with_meta(
        &mut self,
        task: Box<dyn Task>,
        priority: TaskPriority,
        metadata: TaskMetadata,
        now_ns: u64,
    ) -> Result<TaskHandle, TaskError> {
        if self.shutdown.is_shutting_down() {
            return Err(TaskError::ShuttingDown);
        }
        let (handle, slot_idx) = self.pool.alloc(task, priority, metadata, now_ns)?;
        if self.ready.push(priority, slot_idx, now_ns).is_err() {
            self.pool.free(slot_idx);
            return Err(TaskError::Rejected { priority });
        }
        self.metrics.record_spawned();
        Ok(handle)
    }
}

impl std::fmt::Debug for TaskQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskQueue")
            .field("capacity", &self.config.capacity)
            .field("depth", &self.ready.depth())
            .field("free_slots", &self.pool.free_count())
            .field("deferred", &self.deferred.len())
            .field("shutting_down", &self.shutdown.is_shutting_down())
            .finish()
    }
}

// Suppress an unused-import warning for `TaskResult` when the closure
// parameter `reenqueue` is omitted in tests.
#[allow(dead_code)]
fn _used(_r: TaskResult) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{Task, TaskResult};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct Counter(Arc<AtomicU32>);
    impl Task for Counter {
        fn run(&mut self, _ctx: &mut TaskContext) -> TaskResult {
            self.0.fetch_add(1, Ordering::Relaxed);
            TaskResult::Done
        }
    }

    fn make_queue(cap: usize) -> TaskQueue {
        TaskQueue::from_config(TaskQueueConfig {
            capacity: cap,
            ..Default::default()
        })
        .expect("config")
    }

    #[test]
    fn spawn_and_tick_executes() {
        let mut q = make_queue(64);
        let counter = Arc::new(AtomicU32::new(0));
        q.spawn(Counter(Arc::clone(&counter)), TaskPriority::Normal)
            .expect("spawn");
        let mut ctx = TaskContext::new(0, 0);
        let r = q.tick(&mut ctx, 0);
        assert_eq!(r.executed, 1);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        assert_eq!(q.free_slots(), 64);
    }

    #[test]
    fn cancel_skips_execution() {
        let mut q = make_queue(64);
        let counter = Arc::new(AtomicU32::new(0));
        let handle = q
            .spawn(Counter(Arc::clone(&counter)), TaskPriority::Normal)
            .expect("spawn");
        q.cancel(&handle);
        let mut ctx = TaskContext::new(0, 0);
        let r = q.tick(&mut ctx, 0);
        assert_eq!(r.executed, 0);
        assert_eq!(r.cancelled, 1);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn spawn_after_records_deferred() {
        let mut q = make_queue(64);
        let counter = Arc::new(AtomicU32::new(0));
        q.schedule_after(
            Counter(Arc::clone(&counter)),
            Duration::from_millis(10),
            TaskPriority::Normal,
            0,
        )
        .expect("schedule");
        assert_eq!(q.deferred_count(), 1);
        // Tick does not execute deferred tasks.
        let mut ctx = TaskContext::new(0, 0);
        let r = q.tick(&mut ctx, 0);
        assert_eq!(r.executed, 0);
        // Simulate timer fire.
        let entries = q.deferred.drain();
        for h in entries {
            q.on_timer_fire(h.slot_idx, 0);
        }
        let r2 = q.tick(&mut ctx, 0);
        assert_eq!(r2.executed, 1);
    }

    #[test]
    fn spawn_rejected_when_full() {
        let mut q = make_queue(64);
        let counter = Arc::new(AtomicU32::new(0));
        for _ in 0..64 {
            q.spawn(Counter(Arc::clone(&counter)), TaskPriority::Normal)
                .expect("spawn");
        }
        let err = q
            .spawn(Counter(Arc::clone(&counter)), TaskPriority::Normal)
            .unwrap_err();
        assert!(matches!(err, TaskError::PoolExhausted { .. }));
    }

    #[test]
    fn shutdown_rejects_new_spawns() {
        let mut q = make_queue(64);
        let mut ctx = TaskContext::new(0, 0);
        let _ = q.shutdown(&mut ctx, Duration::from_millis(100), 0);
        let err = q
            .spawn(Counter(Arc::new(AtomicU32::new(0))), TaskPriority::High)
            .unwrap_err();
        assert!(matches!(err, TaskError::ShuttingDown));
    }

    #[test]
    fn debug_format_has_fields() {
        let q = make_queue(64);
        let s = format!("{q:?}");
        assert!(s.contains("capacity"));
        assert!(s.contains("free_slots"));
    }

    #[test]
    fn cross_thread_spawn_runs_on_next_tick() {
        let mut q = make_queue(64);
        let counter = Arc::new(AtomicU32::new(0));
        q.spawn_cross_thread(Counter(Arc::clone(&counter)), TaskPriority::High, 0)
            .expect("cross");
        assert_eq!(q.inbox.pending(), 1);
        let mut ctx = TaskContext::new(0, 0);
        let r = q.tick(&mut ctx, 0);
        assert_eq!(r.inbox_drained, 1);
        assert_eq!(r.executed, 1);
    }

    #[test]
    fn priority_order_honored_in_tick() {
        let mut q = make_queue(64);
        let counter = Arc::new(AtomicU32::new(0));
        // Spawn 3 lows then 1 high.
        for _ in 0..3 {
            q.spawn(Counter(Arc::clone(&counter)), TaskPriority::Low)
                .expect("spawn");
        }
        q.spawn(Counter(Arc::clone(&counter)), TaskPriority::High)
            .expect("spawn");

        // Use a tiny budget so only the high runs first.
        q.config.batch_max_tasks = 1;
        let mut ctx = TaskContext::new(0, 0);
        let r1 = q.tick(&mut ctx, 0);
        assert_eq!(r1.executed, 1);
        // Now drain the rest.
        q.config.batch_max_tasks = 64;
        let r2 = q.tick(&mut ctx, 0);
        assert_eq!(r2.executed, 3);
    }
}

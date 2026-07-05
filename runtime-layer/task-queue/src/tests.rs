//! Integration tests for the Task Queue.
//!
//! These live inside the crate so they can access `pub(crate)` items
//! when needed. The tests cover end-to-end lifecycle scenarios that
//! span multiple internal modules.

#![cfg(test)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::TaskQueueBuilder;
use crate::context::TaskContext;
use crate::priority::TaskPriority;
use crate::task::{Task, TaskResult};

struct Counter(Arc<AtomicU32>);
impl Task for Counter {
    fn run(&mut self, _ctx: &mut TaskContext) -> TaskResult {
        self.0.fetch_add(1, Ordering::Relaxed);
        TaskResult::Done
    }
}

#[test]
fn end_to_end_spawn_tick_complete() {
    let mut q = TaskQueueBuilder::new()
        .capacity(256)
        .build()
        .expect("build");
    let counter = Arc::new(AtomicU32::new(0));
    for _ in 0..10 {
        q.spawn(Counter(Arc::clone(&counter)), TaskPriority::Normal)
            .expect("spawn");
    }
    let mut ctx = TaskContext::new(0, 0);
    let r = q.tick(&mut ctx, 0);
    assert_eq!(r.executed, 10);
    assert_eq!(counter.load(Ordering::Relaxed), 10);
    assert_eq!(q.depth(), 0);
    assert_eq!(q.free_slots(), 256);
}

#[test]
fn priority_strict_order_under_budget() {
    let mut q = TaskQueueBuilder::new()
        .capacity(256)
        .batch_max_tasks(1)
        .build()
        .expect("build");
    let counter = Arc::new(AtomicU32::new(0));
    q.spawn(Counter(Arc::clone(&counter)), TaskPriority::Low)
        .expect("low");
    q.spawn(Counter(Arc::clone(&counter)), TaskPriority::High)
        .expect("high");

    let mut ctx = TaskContext::new(0, 0);
    let r1 = q.tick(&mut ctx, 0);
    assert_eq!(r1.executed, 1);
    assert_eq!(counter.load(Ordering::Relaxed), 1);
    // Bump budget for the second tick.
    q.config.batch_max_tasks = 16;
    let r2 = q.tick(&mut ctx, 0);
    assert_eq!(r2.executed, 1);
    assert_eq!(counter.load(Ordering::Relaxed), 2);
}

#[test]
fn schedule_after_fires_via_on_timer_fire() {
    let mut q = TaskQueueBuilder::new().capacity(64).build().expect("build");
    let counter = Arc::new(AtomicU32::new(0));
    q.schedule_after(
        Counter(Arc::clone(&counter)),
        Duration::from_millis(5),
        TaskPriority::Normal,
        0,
    )
    .expect("schedule");
    let mut ctx = TaskContext::new(0, 0);
    let before = q.tick(&mut ctx, 0);
    assert_eq!(before.executed, 0);

    // Simulate timer wheel firing.
    let pending = q.deferred.drain();
    for h in pending {
        q.on_timer_fire(h.slot_idx, 0);
    }
    let after = q.tick(&mut ctx, 0);
    assert_eq!(after.executed, 1);
    assert_eq!(counter.load(Ordering::Relaxed), 1);
}

#[test]
fn cancel_then_tick_skips_task() {
    let mut q = TaskQueueBuilder::new().capacity(64).build().expect("build");
    let counter = Arc::new(AtomicU32::new(0));
    let h = q
        .spawn(Counter(Arc::clone(&counter)), TaskPriority::High)
        .expect("spawn");
    q.cancel(&h);
    let mut ctx = TaskContext::new(0, 0);
    let r = q.tick(&mut ctx, 0);
    assert_eq!(r.executed, 0);
    assert_eq!(r.cancelled, 1);
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

#[test]
fn cross_thread_then_tick_drains() {
    let mut q = TaskQueueBuilder::new().capacity(64).build().expect("build");
    let counter = Arc::new(AtomicU32::new(0));
    q.spawn_cross_thread(Counter(Arc::clone(&counter)), TaskPriority::Normal, 0)
        .expect("cross");
    let mut ctx = TaskContext::new(0, 0);
    let r = q.tick(&mut ctx, 0);
    assert_eq!(r.inbox_drained, 1);
    assert_eq!(r.executed, 1);
}

#[test]
fn shutdown_drops_remaining_tasks() {
    let mut q = TaskQueueBuilder::new().capacity(64).build().expect("build");
    let counter = Arc::new(AtomicU32::new(0));
    for _ in 0..5 {
        q.spawn(Counter(Arc::clone(&counter)), TaskPriority::Normal)
            .expect("spawn");
    }
    let mut ctx = TaskContext::new(0, 0);
    let report = q.shutdown(&mut ctx, Duration::from_millis(50), 0);
    // Some tasks may have been drained in the shutdown pass; the rest dropped.
    let processed = report.drained_executed + report.dropped;
    assert_eq!(processed, 5);
    assert!(q.is_shutting_down());
}

#[test]
fn metrics_reflect_activity() {
    let mut q = TaskQueueBuilder::new().capacity(64).build().expect("build");
    let counter = Arc::new(AtomicU32::new(0));
    let _h = q
        .spawn(Counter(Arc::clone(&counter)), TaskPriority::Normal)
        .expect("spawn");
    let mut ctx = TaskContext::new(0, 0);
    let _ = q.tick(&mut ctx, 0);
    let snap = q.metrics();
    assert_eq!(snap.spawned, 1);
    assert_eq!(snap.executed, 1);
    assert_eq!(snap.ticks, 1);
}

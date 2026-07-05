//! Shared helpers for the `task-queue` crate benchmark suite.

#![allow(clippy::expect_used, clippy::unwrap_used, dead_code)]

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use criterion::BatchSize;

use task_queue::{Task, TaskContext, TaskQueue, TaskQueueBuilder, TaskResult};

// ── Workload constants ────────────────────────────────────────────

/// Capacities swept across benches (power-of-two).
pub const CAPACITIES: &[usize] = &[256, 1024, 4096];

/// Batch sizes swept in tick benches.
pub const BATCH_SIZES: &[u16] = &[16, 64, 128, 256];

/// Producer-thread counts for MPSC scaling benches.
pub const PRODUCER_COUNTS: &[usize] = &[1, 2, 4, 8, 16];

/// Fill-level percentages for saturation benches.
pub const FILL_PERCENTS: &[u8] = &[50, 75, 90, 95, 98, 99];

// ── Fixture builders ──────────────────────────────────────────────

/// Builds a fresh queue with `capacity` using the default builder path.
pub fn fresh_queue(capacity: usize) -> TaskQueue {
    TaskQueueBuilder::new()
        .capacity(capacity)
        .build()
        .expect("queue build")
}

// ── Trivial tasks ─────────────────────────────────────────────────

/// Counter task — increments a shared counter on each run.
pub struct Counter(pub Arc<AtomicU32>);
impl Task for Counter {
    fn run(&mut self, _ctx: &mut TaskContext) -> TaskResult {
        self.0.fetch_add(1, Ordering::Relaxed);
        TaskResult::Done
    }
}

/// No-op task — cheapest possible run() body.
pub struct Noop;
impl Task for Noop {
    fn run(&mut self, _ctx: &mut TaskContext) -> TaskResult {
        TaskResult::Done
    }
}

/// Counter factory — returns a counter + its shared Arc for verification.
pub fn counter_pair() -> (Counter, Arc<AtomicU32>) {
    let arc = Arc::new(AtomicU32::new(0));
    (Counter(Arc::clone(&arc)), arc)
}

// ── Batch size helpers ────────────────────────────────────────────

pub const SMALL_BATCH: BatchSize = BatchSize::SmallInput;
pub const LARGE_BATCH: BatchSize = BatchSize::LargeInput;

// ── Black-box sink ────────────────────────────────────────────────

#[inline]
pub fn sink<T>(v: T) {
    let _ = black_box(v);
}

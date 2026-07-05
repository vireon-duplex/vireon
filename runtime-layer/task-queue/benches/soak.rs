//! Benchmark: sustained spawn→tick→complete cycle (leak detection).
//!
//! Repeats the full lifecycle many times in a single Criterion
//! iteration to surface per-cycle allocation growth, pool-fragmentation
//! regressions, or recycler stalls.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use task_queue::{TaskContext, TaskPriority, TaskQueueBuilder};

use common::Noop;

/// Sustained spawn→tick cycles: each iteration does K rounds of
/// "spawn N → tick once". The pool should never grow, and free_slots
/// should return to capacity each tick.
fn bench_sustained_cycle(c: &mut Criterion) {
    let cap = 1024usize;
    let n_per_round: u16 = 256;
    let rounds: u32 = 16;
    let total = (n_per_round as u64) * (rounds as u64);
    let mut g = c.benchmark_group("sustained_cycle");
    g.throughput(Throughput::Elements(total));
    g.bench_function("16x256", |b| {
        b.iter_batched_ref(
            || {
                TaskQueueBuilder::new()
                    .capacity(cap)
                    .batch_max_tasks(cap as u16)
                    .build()
                    .expect("build")
            },
            |q| {
                let mut ctx = TaskContext::new(0, 0);
                for _ in 0..rounds {
                    for _ in 0..n_per_round {
                        q.spawn(Noop, TaskPriority::Normal).expect("spawn");
                    }
                    let r = q.tick(&mut ctx, 0);
                    black_box(r);
                }
                // Sanity: pool must be fully recycled.
                debug_assert_eq!(q.free_slots(), cap, "pool leaked slots");
            },
            common::LARGE_BATCH,
        )
    });
    g.finish();
}

/// Long steady-state: many small batches ticked back-to-back to
/// approximate the reactor's hot loop.
fn bench_steady_state(c: &mut Criterion) {
    let cap = 512usize;
    let batch: u16 = 32;
    let iters: u32 = 512;
    let total = (batch as u64) * (iters as u64);
    let mut g = c.benchmark_group("steady_state");
    g.throughput(Throughput::Elements(total));
    g.bench_function("32x512", |b| {
        b.iter_batched_ref(
            || {
                TaskQueueBuilder::new()
                    .capacity(cap)
                    .batch_max_tasks(batch)
                    .build()
                    .expect("build")
            },
            |q| {
                let mut ctx = TaskContext::new(0, 0);
                for _ in 0..iters {
                    for _ in 0..batch {
                        q.spawn(Noop, TaskPriority::Normal).expect("spawn");
                    }
                    let r = q.tick(&mut ctx, 0);
                    black_box(r);
                }
            },
            common::LARGE_BATCH,
        )
    });
    g.finish();
}

criterion_group!(benches, bench_sustained_cycle, bench_steady_state);
criterion_main!(benches);

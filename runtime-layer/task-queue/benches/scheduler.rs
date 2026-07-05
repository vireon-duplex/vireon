//! Benchmark: TaskScheduler select() + policy construction overhead.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use task_queue::{
    FifoPolicy, TaskContext, TaskPolicy, TaskPriority, TaskQueueBuilder, batch::TaskBatch,
    budget::Budget, ready::ReadyQueue,
};

use common::Noop;

/// Isolate the `FifoPolicy::select` call from the full tick path.
/// Builds a fresh ready queue + batch each iteration to measure pure
/// selection cost across depths.
fn bench_policy_select_only(c: &mut Criterion) {
    let mut g = c.benchmark_group("policy_select_only");
    for &n in &[64usize, 256, 1024] {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched_ref(
                || {
                    let mut ready = ReadyQueue::new(n.max(64).next_power_of_two());
                    for i in 0..n as u32 {
                        let prio = match i % 4 {
                            0 => TaskPriority::High,
                            1 => TaskPriority::Normal,
                            2 => TaskPriority::Low,
                            _ => TaskPriority::Background,
                        };
                        ready.push(prio, i, 0).expect("push");
                    }
                    (ready, FifoPolicy::new(), TaskBatch::new(n))
                },
                |(ready, policy, batch)| {
                    let _ = policy.select(ready, Budget::new(n as u16, u64::MAX), batch);
                    black_box(batch.len());
                },
                common::SMALL_BATCH,
            )
        });
    }
    g.finish();
}

/// Scheduler-driven tick with mixed priorities — measures the full
/// select+execute path under the FIFO policy.
fn bench_scheduler_tick_mixed(c: &mut Criterion) {
    let mut g = c.benchmark_group("scheduler_tick_mixed");
    g.throughput(Throughput::Elements(256));
    g.bench_function("mixed", |b| {
        b.iter_batched_ref(
            || {
                let mut q = TaskQueueBuilder::new()
                    .capacity(1024)
                    .batch_max_tasks(256)
                    .build()
                    .expect("build");
                for i in 0..256u32 {
                    let prio = match i % 4 {
                        0 => TaskPriority::High,
                        1 => TaskPriority::Normal,
                        2 => TaskPriority::Low,
                        _ => TaskPriority::Background,
                    };
                    q.spawn(Noop, prio).expect("spawn");
                }
                q
            },
            |q| {
                let mut ctx = TaskContext::new(0, 0);
                let r = q.tick(&mut ctx, 0);
                black_box(r);
            },
            common::SMALL_BATCH,
        )
    });
    g.finish();
}

/// Pure ready-queue push/pop cost — underlying data structure benchmark.
fn bench_ready_queue_push_pop(c: &mut Criterion) {
    let mut g = c.benchmark_group("ready_queue_push_pop");
    for &n in &[64usize, 256, 1024] {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched_ref(
                || ReadyQueue::new(n.max(64).next_power_of_two()),
                |q| {
                    for i in 0..n as u32 {
                        let _ = q.push(TaskPriority::Normal, i, 0);
                    }
                    for _ in 0..n {
                        let _ = q.pop_next();
                    }
                },
                common::SMALL_BATCH,
            )
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_policy_select_only,
    bench_scheduler_tick_mixed,
    bench_ready_queue_push_pop,
);
criterion_main!(benches);

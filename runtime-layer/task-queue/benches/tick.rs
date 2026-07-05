//! Benchmark: tick() batch execution at various batch sizes.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use task_queue::{TaskContext, TaskPriority, TaskQueueBuilder};

use common::Noop;

/// Spawn N tasks then tick once. Measures the full drain + execute path.
fn bench_tick_at_depth(c: &mut Criterion) {
    let mut g = c.benchmark_group("tick_at_depth");
    for &n in &[16usize, 64, 256, 1024] {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched_ref(
                || {
                    let mut q = TaskQueueBuilder::new()
                        .capacity(n.max(64).next_power_of_two())
                        .batch_max_tasks(1024)
                        .build()
                        .expect("build");
                    for _ in 0..n {
                        q.spawn(Noop, TaskPriority::Normal).expect("spawn");
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
    }
    g.finish();
}

/// Empty tick — queue has no ready work.
fn bench_tick_empty(c: &mut Criterion) {
    let mut g = c.benchmark_group("tick_empty");
    g.bench_function("noop", |b| {
        b.iter_batched_ref(
            || {
                TaskQueueBuilder::new()
                    .capacity(64)
                    .batch_max_tasks(128)
                    .build()
                    .expect("build")
            },
            |q| {
                let mut ctx = TaskContext::new(0, 0);
                let _ = q.tick(&mut ctx, 0);
            },
            common::SMALL_BATCH,
        )
    });
    g.finish();
}

/// Tick with mixed priorities — verify priority-banded drain overhead.
fn bench_tick_mixed_priority(c: &mut Criterion) {
    let mut g = c.benchmark_group("tick_mixed_priority");
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
                let _ = q.tick(&mut ctx, 0);
            },
            common::SMALL_BATCH,
        )
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_tick_at_depth,
    bench_tick_empty,
    bench_tick_mixed_priority
);
criterion_main!(benches);

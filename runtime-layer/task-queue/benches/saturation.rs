//! Benchmark: queue behavior at high fill levels (50–99%).

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use task_queue::{TaskContext, TaskPriority, TaskQueueBuilder};

use common::Noop;

/// Spawns up to `pct%` of capacity, then ticks once to drain all.
/// Measures how spawn cost scales with fill level (free-list
/// fragmentation, ready-queue band pressure).
fn bench_spawn_to_fill(c: &mut Criterion) {
    let cap = 1024usize;
    let mut g = c.benchmark_group("spawn_to_fill");
    for &pct in common::FILL_PERCENTS {
        let n = (cap * pct as usize) / 100;
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(pct), &n, |b, &n| {
            b.iter_batched_ref(
                || {
                    let q = TaskQueueBuilder::new()
                        .capacity(cap)
                        .build()
                        .expect("build");
                    (q, n)
                },
                |(q, n)| {
                    for _ in 0..*n {
                        black_box(q.spawn(Noop, TaskPriority::Normal).expect("spawn"));
                    }
                },
                common::LARGE_BATCH,
            )
        });
    }
    g.finish();
}

/// Tick (full drain) at varying fill levels. Measures how the
/// executor + recycler handle a near-saturated queue.
fn bench_tick_at_fill(c: &mut Criterion) {
    let cap = 1024usize;
    let mut g = c.benchmark_group("tick_at_fill");
    for &pct in common::FILL_PERCENTS {
        let n = (cap * pct as usize) / 100;
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(pct), &n, |b, &n| {
            b.iter_batched_ref(
                || {
                    let mut q = TaskQueueBuilder::new()
                        .capacity(cap)
                        .batch_max_tasks(cap as u16)
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
                common::LARGE_BATCH,
            )
        });
    }
    g.finish();
}

/// Repeated spawn→tick cycles at high fill — exposes any per-tick
/// allocation or capacity-regression behavior.
fn bench_sustained_high_fill(c: &mut Criterion) {
    let cap = 1024usize;
    let n = (cap * 95) / 100; // 95% fill
    let mut g = c.benchmark_group("sustained_high_fill");
    g.throughput(Throughput::Elements(n as u64));
    g.bench_function("95pct", |b| {
        b.iter_batched_ref(
            || {
                TaskQueueBuilder::new()
                    .capacity(cap)
                    .batch_max_tasks(cap as u16)
                    .build()
                    .expect("build")
            },
            |q| {
                for _ in 0..n {
                    let _ = q.spawn(Noop, TaskPriority::Normal);
                }
                let mut ctx = TaskContext::new(0, 0);
                let r = q.tick(&mut ctx, 0);
                black_box(r);
            },
            common::LARGE_BATCH,
        )
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_spawn_to_fill,
    bench_tick_at_fill,
    bench_sustained_high_fill,
);
criterion_main!(benches);

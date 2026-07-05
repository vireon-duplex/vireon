//! Benchmark: MPSC inbox push + drain paths.
//!
//! Measures the lock-free `ArrayQueue` push throughput and the
//! consumer-side drain cost. Sequential producers simulate the
//! hot-path push rate; the underlying `crossbeam_queue::ArrayQueue`
//! is itself MPMC-safe so the numbers are a strict lower bound on
//! cross-thread throughput.
//!
//! Note: `TaskQueue` is `!Sync` (it owns `Box<dyn Task>` slots), so
//! it cannot be wrapped in `Arc` and shared across threads directly.
//! The runtime integration owns the queue on the reactor thread and
//! exposes a separate `InboxHandle` for cross-thread producers.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use task_queue::{TaskContext, TaskPriority, TaskQueueBuilder};

use common::Noop;

/// Single `spawn_cross_thread()` call latency — one task into an
/// empty inbox. Measures the lock-free ArrayQueue push path.
fn bench_cross_thread_single(c: &mut Criterion) {
    let mut g = c.benchmark_group("cross_thread_single");
    g.throughput(Throughput::Elements(1));
    g.bench_function("single", |b| {
        b.iter_batched_ref(
            || {
                TaskQueueBuilder::new()
                    .capacity(1024)
                    .inbox_capacity(1024)
                    .build()
                    .expect("build")
            },
            |q| {
                q.spawn_cross_thread(Noop, TaskPriority::Normal, 0)
                    .expect("cross");
            },
            common::SMALL_BATCH,
        )
    });
    g.finish();
}

/// Burst N entries into the inbox (no drain between pushes). Measures
/// raw lock-free push throughput at varying burst sizes.
fn bench_cross_thread_burst(c: &mut Criterion) {
    let mut g = c.benchmark_group("cross_thread_burst");
    for &n in &[16usize, 64, 256, 1024] {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched_ref(
                || {
                    TaskQueueBuilder::new()
                        .capacity(n.max(1024))
                        .inbox_capacity(n.max(1024))
                        .build()
                        .expect("build")
                },
                |q| {
                    for _ in 0..n {
                        q.spawn_cross_thread(Noop, TaskPriority::Normal, 0)
                            .expect("cross");
                    }
                },
                common::SMALL_BATCH,
            )
        });
    }
    g.finish();
}

/// Producer-side burst + consumer-side drain. Pre-fills the inbox
/// with `n` entries, then measures the `tick()` that drains them.
fn bench_consumer_drain(c: &mut Criterion) {
    let mut g = c.benchmark_group("consumer_drain");
    for &n in &[64usize, 256, 1024] {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched_ref(
                || {
                    let q = TaskQueueBuilder::new()
                        .capacity(n)
                        .inbox_capacity(n)
                        .batch_max_tasks(n as u16)
                        .build()
                        .expect("build");
                    for _ in 0..n {
                        q.spawn_cross_thread(Noop, TaskPriority::Normal, 0)
                            .expect("cross");
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

/// Simulated producer pressure: many bursts into the same inbox
/// (drained once at the end). Models the reactor's amortized inbox
/// drain pattern under sustained cross-thread load.
fn bench_sustained_inbox_pressure(c: &mut Criterion) {
    let rounds: u32 = 64;
    let per_round: u32 = 16;
    let total = (rounds as u64) * (per_round as u64);
    let mut g = c.benchmark_group("sustained_inbox_pressure");
    g.throughput(Throughput::Elements(total));
    g.bench_function("64x16", |b| {
        b.iter_batched_ref(
            || {
                TaskQueueBuilder::new()
                    .capacity(2048)
                    .inbox_capacity(2048)
                    .batch_max_tasks(2048)
                    .build()
                    .expect("build")
            },
            |q| {
                let mut ctx = TaskContext::new(0, 0);
                for _ in 0..rounds {
                    for _ in 0..per_round {
                        let _ = q.spawn_cross_thread(Noop, TaskPriority::Normal, 0);
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

criterion_group!(
    benches,
    bench_cross_thread_single,
    bench_cross_thread_burst,
    bench_consumer_drain,
    bench_sustained_inbox_pressure,
);
criterion_main!(benches);

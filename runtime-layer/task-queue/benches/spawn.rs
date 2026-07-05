//! Benchmark: spawn() latency + burst throughput.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use task_queue::TaskPriority;

use common::{Counter, Noop, fresh_queue};
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

/// Single `spawn()` latency — one task into an empty queue.
fn bench_spawn_single(c: &mut Criterion) {
    let mut g = c.benchmark_group("spawn");
    g.throughput(Throughput::Elements(1));
    g.bench_function("single", |b| {
        b.iter_batched_ref(
            || fresh_queue(1024),
            |q| {
                black_box(q.spawn(Noop, TaskPriority::Normal).expect("spawn"));
            },
            common::SMALL_BATCH,
        )
    });
    g.finish();
}

/// Burst spawn — N tasks back-to-back before any tick drains them.
fn bench_spawn_burst(c: &mut Criterion) {
    let mut g = c.benchmark_group("spawn_burst");
    for &n in &[16usize, 64, 256] {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_function(format!("n={n}"), |b| {
            b.iter_batched_ref(
                || fresh_queue(1024),
                |q| {
                    for _ in 0..n {
                        black_box(q.spawn(Noop, TaskPriority::Normal).expect("spawn"));
                    }
                },
                common::SMALL_BATCH,
            )
        });
    }
    g.finish();
}

/// Counter task — measures the overhead of a task with a real run() body.
fn bench_spawn_counter(c: &mut Criterion) {
    let mut g = c.benchmark_group("spawn_counter");
    g.throughput(Throughput::Elements(1));
    g.bench_function("single", |b| {
        b.iter_batched_ref(
            || (fresh_queue(1024), Arc::new(AtomicU32::new(0))),
            |(q, counter)| {
                black_box(
                    q.spawn(Counter(Arc::clone(counter)), TaskPriority::Normal)
                        .expect("spawn"),
                );
            },
            common::SMALL_BATCH,
        )
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_spawn_single,
    bench_spawn_burst,
    bench_spawn_counter
);
criterion_main!(benches);

//! Insertion throughput (start-path) — Shard-level.
//!
//! All benches here push `TimerCmd::Start` through the real lock-free MPSC
//! queue, then drive `Shard::tick()` once so the worker-side state absorbs
//! the commands. The measurement isolates the **wheel-algorithm + slab**
//! cost from worker parking and cross-thread synchronisation overhead.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::sync::atomic::Ordering;
use timer_wheel::scheduler::shard::TimerCmd;

use common::{BULK_SIZES, DEFAULT_BENCH_CAPACITY, isolated_shard};

/// Baseline: a single `start` from empty.
fn insert_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert/single");
    group.throughput(Throughput::Elements(1));
    group.bench_function("start", |b| {
        b.iter_batched(
            || {
                let (shard, channels, clock) = isolated_shard(DEFAULT_BENCH_CAPACITY);
                let now = clock.now_ticks();
                channels.outstanding.fetch_add(1, Ordering::Relaxed);
                ((shard, channels), now)
            },
            |((mut shard, channels), now)| {
                channels
                    .cmd_tx
                    .push(TimerCmd::Start {
                        task_id: 1,
                        deadline_ticks: now + 5,
                    })
                    .expect("push");
                shard.tick();
                black_box(&shard);
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Bulk insertion across BULK_SIZES — reported as timers/sec.
fn insert_bulk(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert/bulk");
    for &n in &BULK_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let cap = n + 16;
                    let (shard, channels, clock) = isolated_shard(cap);
                    let now = clock.now_ticks();
                    let deadline = now + 30_000; // far enough that nothing fires mid-bench
                    channels.outstanding.fetch_add(n as u64, Ordering::Relaxed);
                    ((shard, channels), deadline)
                },
                |((mut shard, channels), deadline)| {
                    for i in 1..=n as u64 {
                        channels
                            .cmd_tx
                            .push(TimerCmd::Start {
                                task_id: i,
                                deadline_ticks: deadline,
                            })
                            .expect("push");
                    }
                    shard.tick();
                    black_box(&shard);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// 10k timers at a 1 ms deadline (level 0). Isolates level-0 bucket cost.
fn insert_short_deadline(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert/short_deadline");
    group.throughput(Throughput::Elements(10_000));
    group.bench_function("ack_1ms", |b| {
        b.iter_batched(
            || {
                let (shard, channels, clock) = isolated_shard(DEFAULT_BENCH_CAPACITY);
                let now = clock.now_ticks();
                channels.outstanding.fetch_add(10_000, Ordering::Relaxed);
                ((shard, channels), now + 2) // 2 ticks ≈ 2 ms (level 0)
            },
            |((mut shard, channels), deadline)| {
                for i in 1..=10_000u64 {
                    channels
                        .cmd_tx
                        .push(TimerCmd::Start {
                            task_id: i,
                            deadline_ticks: deadline,
                        })
                        .expect("push");
                }
                shard.tick();
                black_box(&shard);
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// 10k timers at a 30 s deadline (level 2). Isolates higher-level routing cost.
fn insert_long_deadline(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert/long_deadline");
    group.throughput(Throughput::Elements(10_000));
    group.bench_function("retransmit_30s", |b| {
        b.iter_batched(
            || {
                let (shard, channels, clock) = isolated_shard(DEFAULT_BENCH_CAPACITY);
                let now = clock.now_ticks();
                channels.outstanding.fetch_add(10_000, Ordering::Relaxed);
                ((shard, channels), now + 30_000) // 30 s ≈ level 2
            },
            |((mut shard, channels), deadline)| {
                for i in 1..=10_000u64 {
                    channels
                        .cmd_tx
                        .push(TimerCmd::Start {
                            task_id: i,
                            deadline_ticks: deadline,
                        })
                        .expect("push");
                }
                shard.tick();
                black_box(&shard);
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    insert_single,
    insert_bulk,
    insert_short_deadline,
    insert_long_deadline,
);
criterion_main!(benches);

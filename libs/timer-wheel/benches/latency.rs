//! Per-op tail latency — Facade-level.
//!
//! Criterion's HTML report surfaces p50 / p90 / p99 / max directly when
//! `sample_size` is large. No HDR-histogram dependency is required. The
//! `latency/*` group should be read via `target/criterion/latency/report/index.html`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::Duration;

/// Per-call `start()` latency. Hot path.
///
/// Uses `iter_batched` with a fresh wheel per sample so the outstanding
/// counter cannot accumulate across samples (which would trip the slab
/// gate during a long measurement window).
fn latency_start(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency/start");
    group.sample_size(50);
    group.bench_function("call", |b| {
        let mut id: u64 = 1;
        b.iter_batched(
            || common::live_wheel(4, 1 << 14),
            |wheel| {
                let h = wheel.start(id, Duration::from_secs(30)).expect("start");
                id = id.wrapping_add(1);
                black_box(h);
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Per-call `cancel()` latency. Hot path.
///
/// Pre-seeds 1 000 handles per sample, then cancels all of them inside the
/// measured routine. Criterion reports the aggregate; per-call latency is
/// `time / 1000`. For p99 of a single cancel, look at the per-sample
/// distribution in the HTML report.
fn latency_cancel(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency/cancel");
    group.throughput(criterion::Throughput::Elements(1_000));
    group.sample_size(20);
    group.bench_function("cancel_1k", |b| {
        b.iter_batched(
            || {
                let wheel = common::live_wheel(4, 1 << 14);
                let handles: Vec<timer_wheel::TimerHandle> = (1..=1_000)
                    .map(|i| wheel.start(i, Duration::from_secs(60)).expect("start"))
                    .collect();
                (wheel, handles)
            },
            |(wheel, handles)| {
                for h in handles {
                    let _ = wheel.cancel(h);
                }
                black_box(());
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// `drain_expired()` on an empty wheel. Baseline.
fn latency_drain_empty(c: &mut Criterion) {
    c.bench_function("latency/drain_empty", |b| {
        let wheel = common::live_wheel(4, 1 << 20);
        b.iter(|| {
            let v = wheel.drain_expired();
            black_box(v);
        });
    });
}

/// `drain_expired()` when the expired queue is full of buffered timers.
fn latency_drain_full(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency/drain_full");
    group.sample_size(20);
    group.bench_function("1k_buffered", |b| {
        b.iter_batched(
            || {
                let wheel = common::live_wheel(4, 1 << 14);
                for i in 1..=1_000u64 {
                    let _ = wheel.start(i, Duration::from_millis(2));
                }
                std::thread::sleep(Duration::from_millis(40));
                wheel
            },
            |wheel| {
                let v = wheel.drain_expired();
                black_box(v);
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Per-call `Shard::tick()` latency (Shard-level).
fn latency_tick_shard(c: &mut Criterion) {
    c.bench_function("latency/tick_shard", |b| {
        let (mut shard, channels, clock) = common::isolated_shard(common::DEFAULT_BENCH_CAPACITY);
        let deadline = clock.now_ticks() + 30_000;
        common::seed_shard(&mut shard, &channels, 10_000, deadline);
        b.iter(|| {
            shard.tick();
            black_box(&shard);
        });
        let _ = channels;
    });
}

criterion_group!(
    benches,
    latency_start,
    latency_cancel,
    latency_drain_empty,
    latency_drain_full,
    latency_tick_shard,
);
criterion_main!(benches);

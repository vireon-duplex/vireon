//! Realistic workload patterns — Facade-level.
//!
//! These benches exercise the full path (facade → MPSC → worker → wheel)
//! under load shapes closer to production than the bulk-insert sweep.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::Duration;
use timer_wheel::TimerHandle;

const OPS: usize = 10_000;

/// Alternating start/cancel — keeps net population near zero.
fn insert_cancel_50_50(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed/insert_cancel_50_50");
    group.throughput(Throughput::Elements(OPS as u64));
    group.sample_size(10);
    group.bench_function("10k_ops", |b| {
        b.iter_batched(
            || common::live_wheel(4, 1 << 20),
            |mut wheel| {
                let mut last: Option<TimerHandle> = None;
                for i in 1..=OPS as u64 {
                    if i % 2 == 0 {
                        // Start this iteration.
                        last = Some(wheel.start(i, Duration::from_secs(30)).expect("start"));
                    } else if let Some(h) = last.take() {
                        // Cancel the previous start.
                        let _ = wheel.cancel(h);
                    }
                }
                black_box(last);
                wheel.shutdown();
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Spike: 10k starts as fast as possible, then a drain. Tests slab/wheel
/// scaling under sudden load.
fn burst(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed/burst");
    group.throughput(Throughput::Elements(10_000));
    group.sample_size(10);
    group.bench_function("10k_spike", |b| {
        b.iter_batched(
            || common::live_wheel(4, 1 << 20),
            |mut wheel| {
                for i in 1..=10_000u64 {
                    let _ = wheel.start(i, Duration::from_secs(60)).expect("start");
                }
                let _ = wheel.drain_expired();
                wheel.shutdown();
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Steady insert stream at 1 ms deadlines (level 0 / ACK pressure).
fn short_timeout(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed/short_timeout");
    group.throughput(Throughput::Elements(OPS as u64));
    group.sample_size(10);
    group.bench_function("10k_ack_1ms", |b| {
        b.iter_batched(
            || common::live_wheel(4, 1 << 20),
            |mut wheel| {
                for i in 1..=OPS as u64 {
                    let _ = wheel.start(i, Duration::from_millis(1)).expect("start");
                }
                // Drain whatever fired; do not wait for all.
                let _ = wheel.drain_expired();
                wheel.shutdown();
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Steady insert stream at 30 s deadlines (level 2 / retransmit pressure).
fn long_timeout(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed/long_timeout");
    group.throughput(Throughput::Elements(OPS as u64));
    group.sample_size(10);
    group.bench_function("10k_retransmit_30s", |b| {
        b.iter_batched(
            || common::live_wheel(4, 1 << 20),
            |mut wheel| {
                for i in 1..=OPS as u64 {
                    let _ = wheel.start(i, Duration::from_secs(30)).expect("start");
                }
                let _ = wheel.drain_expired();
                wheel.shutdown();
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    insert_cancel_50_50,
    burst,
    short_timeout,
    long_timeout,
);
criterion_main!(benches);

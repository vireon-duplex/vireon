//! Benchmark: kernel-side timer operations.
//!
//! Measures SQE build + submit cost for `IORING_OP_TIMEOUT`,
//! `IORING_OP_TIMEOUT_REMOVE`, and the bookkeeping overhead of the
//! active-timer `HashMap`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::Duration;

use io::timer_wheel::TimerWheel;

/// Durations swept in add_timeout benches.
const TIMEOUT_DURATIONS: &[u64] = &[1, 10, 100, 1_000, 10_000]; // ms

fn bench_add_timeout(c: &mut Criterion) {
    let mut g = c.benchmark_group("add_timeout");

    for &ms in TIMEOUT_DURATIONS {
        g.bench_function(format!("{ms}ms"), |b| {
            b.iter_batched(
                || {
                    let ring = common::fresh_ring(256);
                    let tw = TimerWheel::new();
                    (ring, tw)
                },
                |(mut ring, mut tw)| {
                    tw.add_timeout(&mut ring, 42, Duration::from_millis(ms))
                        .expect("add");
                    black_box(tw.active_count());
                },
                BatchSize::PerIteration,
            )
        });
    }
    g.finish();
}

fn bench_add_periodic_tick(c: &mut Criterion) {
    let mut g = c.benchmark_group("add_periodic_tick");

    for &ms in &[1u64, 10, 100] {
        g.bench_function(format!("{ms}ms"), |b| {
            b.iter_batched(
                || {
                    let ring = common::fresh_ring(256);
                    let tw = TimerWheel::new();
                    (ring, tw)
                },
                |(mut ring, mut tw)| {
                    tw.add_periodic_tick(&mut ring, 99, Duration::from_millis(ms))
                        .expect("periodic");
                    black_box(tw);
                },
                BatchSize::PerIteration,
            )
        });
    }
    g.finish();
}

fn bench_cancel_timeout(c: &mut Criterion) {
    let mut g = c.benchmark_group("cancel_timeout");
    g.bench_function("single", |b| {
        b.iter_batched(
            || {
                let mut ring = common::fresh_ring(256);
                let mut tw = TimerWheel::new();
                tw.add_timeout(&mut ring, 42, Duration::from_secs(60))
                    .expect("add");
                (ring, tw)
            },
            |(mut ring, mut tw)| {
                tw.cancel_timeout(&mut ring, 42).expect("cancel");
                black_box(tw);
            },
            BatchSize::PerIteration,
        )
    });
    g.finish();
}

fn bench_retire(c: &mut Criterion) {
    // Pure HashMap operations — no kernel interaction.
    let mut g = c.benchmark_group("retire");

    g.bench_function("hit", |b| {
        b.iter_batched(
            || {
                let mut tw = TimerWheel::new();
                tw.add_timeout(&mut common::fresh_ring(64), 1, Duration::from_secs(60))
                    .expect("add");
                tw
            },
            |mut tw| {
                let r = tw.retire(1);
                black_box(r);
            },
            BatchSize::SmallInput,
        )
    });

    g.bench_function("miss", |b| {
        let mut tw = TimerWheel::new();
        b.iter(|| {
            let r = tw.retire(999);
            black_box(r);
        })
    });
    g.finish();
}

fn bench_is_active(c: &mut Criterion) {
    let mut g = c.benchmark_group("is_active");

    let mut ring = common::fresh_ring(64);
    let mut tw = TimerWheel::new();
    tw.add_timeout(&mut ring, 1, Duration::from_secs(60))
        .expect("add");

    g.bench_function("active", |b| {
        b.iter(|| {
            let r = tw.is_active(1);
            black_box(r);
        })
    });

    g.bench_function("inactive", |b| {
        b.iter(|| {
            let r = tw.is_active(999);
            black_box(r);
        })
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_add_timeout,
    bench_add_periodic_tick,
    bench_cancel_timeout,
    bench_retire,
    bench_is_active,
);
criterion_main!(benches);

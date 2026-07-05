//! Benchmark: backpressure gate overhead.
//!
//! Measures [`BackpressureGate::admit`] (pre-push check) and
//! [`BackpressureGate::post_change`] (post-push/pop level update with
//! hysteresis). Both run on every submit and every flush.
//!
//! Expected latency: < 5 ns per call (branch + relaxed atomic swap).

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use send_queue::backpressure::BackpressureGate;

/// Pre-push admission check at fixed depths.
fn bench_admit(c: &mut Criterion) {
    let gate = BackpressureGate::new(3072, 3686, 410, 4096);
    let mut g = c.benchmark_group("admit");
    for &depth in &[0usize, 1024, 3072, 3500] {
        g.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            b.iter(|| {
                let r = gate.admit(black_box(depth));
                let _ = black_box(r);
            })
        });
    }
    g.finish();
}

/// Post-change level computation (includes hysteresis branch).
fn bench_post_change(c: &mut Criterion) {
    let mut g = c.benchmark_group("post_change");
    for &depth in &[0usize, 1024, 3072, 3686] {
        g.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            // Reset to Normal between samples so transitions are stable.
            b.iter_batched(
                || BackpressureGate::new(3072, 3686, 410, 4096),
                |gate| {
                    let lvl = gate.post_change(black_box(depth));
                    black_box(lvl);
                },
                common::SMALL_BATCH,
            )
        });
    }
    g.finish();
}

/// Relaxed load of the current level — cheapest accessor.
fn bench_level(c: &mut Criterion) {
    let gate = BackpressureGate::new(3072, 3686, 410, 4096);
    c.bench_function("level", |b| {
        b.iter(|| {
            black_box(gate.level());
        })
    });
}

criterion_group!(benches, bench_admit, bench_post_change, bench_level);
criterion_main!(benches);

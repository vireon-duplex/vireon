//! Benchmark: atomic-counter overhead for `MetricsCollector`.
//!
//! These are the hottest-path operations in the reactor's CQE dispatch
//! loop — every completion bumps at least one counter. Expected latency:
//! < 10 ns per op (single `Relaxed` atomic).

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

use io::metrics::MetricsCollector;

fn bench_record_cqe_completion(c: &mut Criterion) {
    let m = MetricsCollector::new();
    let mut g = c.benchmark_group("record_cqe_completion");

    g.bench_function("rx_success", |b| {
        b.iter(|| {
            m.record_cqe_completion(1024, true);
            black_box(());
        })
    });

    g.bench_function("tx_success", |b| {
        b.iter(|| {
            m.record_cqe_completion(512, false);
            black_box(());
        })
    });

    g.bench_function("error_negative_res", |b| {
        b.iter(|| {
            m.record_cqe_completion(-libc::EBUSY, true);
            black_box(());
        })
    });

    g.finish();
}

fn bench_record_bytes(c: &mut Criterion) {
    let m = MetricsCollector::new();
    let mut g = c.benchmark_group("record_bytes");

    g.bench_function("rx", |b| {
        b.iter(|| {
            m.record_bytes_rx(2048);
            black_box(());
        })
    });

    g.bench_function("tx", |b| {
        b.iter(|| {
            m.record_bytes_tx(2048);
            black_box(());
        })
    });

    g.finish();
}

fn bench_record_other(c: &mut Criterion) {
    let m = MetricsCollector::new();
    let mut g = c.benchmark_group("record_other");

    g.bench_function("cq_overflow", |b| {
        b.iter(|| {
            m.record_cq_overflow();
            black_box(());
        })
    });

    g.bench_function("buffer_exhaustion", |b| {
        b.iter(|| {
            m.track_buffer_exhaustion(1);
            black_box(());
        })
    });

    g.bench_function("submit_backpressure", |b| {
        b.iter(|| {
            m.record_submit_backpressure();
            black_box(());
        })
    });

    g.bench_function("timer_cancel", |b| {
        b.iter(|| {
            m.record_timer_cancel();
            black_box(());
        })
    });

    g.finish();
}

fn bench_snapshot(c: &mut Criterion) {
    let mut g = c.benchmark_group("snapshot");

    g.bench_function("fresh_collector", |b| {
        let m = MetricsCollector::new();
        b.iter(|| {
            let snap = m.snapshot();
            black_box(snap);
        })
    });

    g.bench_function("after_10k_ops", |b| {
        let m = MetricsCollector::new();
        for _ in 0..10_000 {
            m.record_cqe_completion(1024, true);
            m.record_cqe_completion(512, false);
            m.record_bytes_rx(1024);
        }
        b.iter(|| {
            let snap = m.snapshot();
            black_box(snap);
        })
    });

    g.finish();
}

criterion_group!(
    benches,
    bench_record_cqe_completion,
    bench_record_bytes,
    bench_record_other,
    bench_snapshot,
);
criterion_main!(benches);

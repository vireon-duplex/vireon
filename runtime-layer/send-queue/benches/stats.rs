//! Benchmark: atomic-counter overhead for `StatsCollector`.
//!
//! Every submit, flush, CQE, retry, and drop bumps at least one
//! counter. The snapshot is read by the metrics exporter on a timer.
//!
//! Expected latency: < 5 ns per `Relaxed` atomic; ~10 ns per field for
//! snapshot (10 `Relaxed` loads).

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

use send_queue::stats::StatsCollector;

fn bench_record_submit(c: &mut Criterion) {
    let s = StatsCollector::new();
    c.bench_function("record_submit_pair", |b| {
        b.iter(|| {
            s.record_submit();
            s.record_submit_success();
        })
    });
}

fn bench_record_flush(c: &mut Criterion) {
    let s = StatsCollector::new();
    c.bench_function("record_flush_64", |b| {
        b.iter(|| {
            s.record_flush(64);
        })
    });
}

fn bench_record_cqe(c: &mut Criterion) {
    let s = StatsCollector::new();
    let mut g = c.benchmark_group("record_cqe");
    g.bench_function("send", |b| {
        b.iter(|| {
            s.record_send_cqe();
        })
    });
    g.bench_function("reclaim", |b| {
        b.iter(|| {
            s.record_reclaim_cqe();
        })
    });
    g.finish();
}

fn bench_snapshot(c: &mut Criterion) {
    let mut g = c.benchmark_group("snapshot");

    g.bench_function("fresh", |b| {
        let s = StatsCollector::new();
        b.iter(|| {
            black_box(s.snapshot(0, 0, 0, 0));
        })
    });

    g.bench_function("after_10k_ops", |b| {
        let s = StatsCollector::new();
        for _ in 0..10_000 {
            s.record_submit();
            s.record_submit_success();
            s.record_flush(32);
            s.record_send_cqe();
            s.record_reclaim_cqe();
        }
        b.iter(|| {
            black_box(s.snapshot(128, 64, 4, 1));
        })
    });

    g.finish();
}

criterion_group!(
    benches,
    bench_record_submit,
    bench_record_flush,
    bench_record_cqe,
    bench_snapshot,
);
criterion_main!(benches);

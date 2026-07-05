//! Benchmark: StatsCollector increment + snapshot.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;
use std::sync::atomic::Ordering;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use cqe_processor::StatsCollector;

use common::BATCH_SIZES;

fn bench_stats_increment(c: &mut Criterion) {
    let mut g = c.benchmark_group("stats_increment");
    for &n in BATCH_SIZES {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched_ref(
                StatsCollector::new,
                |s| {
                    for i in 0..n as u64 {
                        s.cqes_total.store(i, Ordering::Relaxed);
                        s.zc_notifications.fetch_add(1, Ordering::Relaxed);
                    }
                    black_box(s.cqes_total.load(Ordering::Relaxed));
                },
                common::SMALL_BATCH,
            )
        });
    }
    g.finish();
}

fn bench_stats_snapshot(c: &mut Criterion) {
    let mut g = c.benchmark_group("stats_snapshot");
    g.throughput(Throughput::Elements(1));
    g.bench_function("single", |b| {
        b.iter_batched_ref(
            || {
                let s = StatsCollector::new();
                s.cqes_total.store(1_000_000, Ordering::Relaxed);
                s.zc_notifications.store(42, Ordering::Relaxed);
                s
            },
            |s| {
                let snap = s.snapshot();
                black_box(snap);
            },
            common::SMALL_BATCH,
        )
    });
    g.finish();
}

criterion_group!(benches, bench_stats_increment, bench_stats_snapshot);
criterion_main!(benches);

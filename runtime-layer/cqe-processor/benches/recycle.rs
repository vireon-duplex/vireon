//! Benchmark: RecyclePipeline push / drain performance.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use cqe_processor::RecyclePipeline;

use common::BATCH_SIZES;

fn bench_recycle_push(c: &mut Criterion) {
    let mut g = c.benchmark_group("recycle_push");
    for &n in BATCH_SIZES {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched_ref(
                || RecyclePipeline::new(n.max(16)),
                |p| {
                    for i in 0..n as u16 {
                        p.push(i);
                    }
                    black_box(p.pending_count());
                },
                common::SMALL_BATCH,
            )
        });
    }
    g.finish();
}

fn bench_recycle_drain(c: &mut Criterion) {
    let mut g = c.benchmark_group("recycle_drain");
    for &n in BATCH_SIZES {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched_ref(
                || {
                    let mut p = RecyclePipeline::new(n.max(16));
                    for i in 0..n as u16 {
                        p.push(i);
                    }
                    p
                },
                |p| {
                    let drained = p.drain_pending();
                    black_box(drained.len());
                },
                common::SMALL_BATCH,
            )
        });
    }
    g.finish();
}

criterion_group!(benches, bench_recycle_push, bench_recycle_drain);
criterion_main!(benches);

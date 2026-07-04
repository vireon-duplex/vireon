//! Batch refill & drain benchmarks (Categories 3–4).
//!
//! The pool moves slots between the thread-local cache and the global sharded
//! cache in batches (controlled by the `batch` config field). This file
//! measures how batch size affects:
//!
//! - **Refill latency** — pulling a batch from global to local on cache miss.
//! - **Drain latency** — pushing a batch from local to global on overflow.
//! - **Amortised get+drop** — steady-state cost at each batch setting.
//!
//! Batch sizes swept: 1, 2, 4, 8, 16, 32, 64.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::{criterion_group, criterion_main};

use buffer_pool::BufferPool;

const BENCH_SIZE: usize = 4096;

// ============================================================================
// Refill: batch size impact on get+drop steady state (Category 3)
// ============================================================================

/// Measures the amortised get+drop cost as `batch` varies. A larger batch
/// means fewer global round-trips but more work per miss.
fn bench_refill_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("refill/batch");
    group.throughput(Throughput::Bytes(BENCH_SIZE as u64));

    for &batch in common::BATCH_SIZES {
        let local_cap = batch.max(64);
        let pool = BufferPool::builder()
            .local_cache(local_cap)
            .global_cache(4096)
            .shards(8)
            .batch(batch)
            .build()
            .expect("build");

        // Pre-populate global cache so refills always have data.
        common::seed_global_cache(&pool, BENCH_SIZE, 512);

        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            let _ = batch;
            b.iter(|| {
                let buf = pool.get(BENCH_SIZE).expect("get");
                black_box(buf.as_ptr());
                black_box(buf.len());
                drop(buf);
            });
        });

        assert_eq!(pool.stats().counters.outstanding, 0);
    }

    group.finish();
}

// ============================================================================
// Drain: batch size impact on release overflow (Category 4)
// ============================================================================

/// Measures the release-side cost when the local cache overflows. With
/// `local_cache == batch`, every full drain cycle moves exactly one batch
/// to the global shard.
fn bench_drain_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("drain/batch");
    group.throughput(Throughput::Bytes(BENCH_SIZE as u64));

    for &batch in common::BATCH_SIZES {
        let pool = BufferPool::builder()
            .local_cache(batch)
            .global_cache(8192)
            .shards(8)
            .batch(batch)
            .build()
            .expect("build");

        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            let _ = batch;
            // Fill the local cache to capacity so the next drop drains.
            let warmup: Vec<_> = (0..pool.config().local_cache)
                .map(|_| pool.get(BENCH_SIZE).expect("get"))
                .collect();
            drop(warmup);

            b.iter(|| {
                // Acquire then drop — when local is full this triggers
                // a drain batch push to the global shard.
                let buf = pool.get(BENCH_SIZE).expect("get");
                black_box(buf.as_ptr());
                drop(buf);
            });
        });
    }

    group.finish();
}

// ============================================================================
// Batch round-trip: allocate N + drop N (Category 3-4 combined)
// ============================================================================

/// Models the realistic pattern of a batch acquire (fill local via refill)
/// followed by a batch release (fill local, then drain). The total time is
/// divided by N to give a per-buffer cost.
fn bench_batch_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch/roundtrip");
    group.throughput(Throughput::Bytes((BENCH_SIZE * 64) as u64));

    for &batch in common::BATCH_SIZES {
        let pool = BufferPool::builder()
            .local_cache(batch.max(16))
            .global_cache(8192)
            .batch(batch)
            .build()
            .expect("build");

        common::seed_global_cache(&pool, BENCH_SIZE, 1024);

        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            b.iter(|| {
                let mut bufs = Vec::with_capacity(batch);
                for _ in 0..batch {
                    let buf = pool.get(BENCH_SIZE).expect("get");
                    black_box(buf.as_ptr());
                    bufs.push(buf);
                }
                for buf in bufs.into_iter().rev() {
                    drop(buf);
                }
            });
        });

        assert_eq!(pool.stats().counters.outstanding, 0);
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_refill_batch,
    bench_drain_batch,
    bench_batch_roundtrip,
);
criterion_main!(benches);

//! Scalability & contention benchmarks (Categories 9-10).
//!
//! **NOTE**: BufferPool is now `!Send` (uses `Rc` internally). The original
//! multi-threaded scaling and contention benchmarks have been converted to
//! single-threaded equivalents. The same total number of operations is run
//! sequentially — the "thread count" parameter is retained as a throughput
//! multiplier so historical comparisons remain meaningful.
//!
//! Category 9 — **Scaling** (was N threads): now `num_threads * OPS_PER_THREAD`
//! sequential get+write+drop cycles on the same pool.
//!
//! Category 10 — **Contention** (was N threads on 1 shard): now sequential
//! operations; without cross-thread contention the lock path is no longer
//! exercised, but per-op cost under the configured shard / cache settings
//! is still measured.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::{criterion_group, criterion_main};

use buffer_pool::BufferPool;

/// Operations per "thread" per iteration (now just a multiplier).
const OPS_PER_THREAD: usize = 2_000;

// ============================================================================
// Scaling: default config (Category 9) — single-threaded equivalent
// ============================================================================

fn bench_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling/roundtrip");

    for &num_threads in common::THREAD_COUNTS {
        let total_ops = (num_threads * OPS_PER_THREAD) as u64;
        group.throughput(Throughput::Elements(total_ops));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            &num_threads,
            |b, &num_threads| {
                let pool = BufferPool::default();
                let total_iters = num_threads * OPS_PER_THREAD;

                b.iter(|| {
                    for _ in 0..total_iters {
                        for &(size, _) in common::WORKLOAD {
                            let mut buf = pool.get(size).expect("get");
                            buf[0] = 0xAA;
                            black_box(buf.as_ptr());
                            drop(buf);
                        }
                    }
                });

                let stats = pool.stats();
                assert_eq!(
                    stats.counters.outstanding, 0,
                    "leak at {num_threads} threads (sequential)"
                );
            },
        );
    }

    group.finish();
}

// ============================================================================
// Scaling efficiency: same-class single-size (Category 9 — isolated)
// ============================================================================

fn bench_scaling_single_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling/single_size");
    const SIZE: usize = 4096;

    for &num_threads in common::THREAD_COUNTS {
        let total_ops = (num_threads * OPS_PER_THREAD) as u64;
        group.throughput(Throughput::Elements(total_ops));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            &num_threads,
            |b, &num_threads| {
                let pool = BufferPool::default();
                let total_iters = num_threads * OPS_PER_THREAD;

                b.iter(|| {
                    for _ in 0..total_iters {
                        let mut buf = pool.get(SIZE).expect("get");
                        buf[0] = 1;
                        black_box(buf.as_ptr());
                        drop(buf);
                    }
                });

                assert_eq!(pool.stats().counters.outstanding, 0);
            },
        );
    }

    group.finish();
}

// ============================================================================
// Contention: local_cache(1) + shards(1) — sequential (Cat 10)
// ============================================================================

fn bench_contention_single_shard(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention/single_shard");
    const SIZE: usize = 4096;

    for &num_threads in common::THREAD_COUNTS {
        let total_ops = (num_threads * OPS_PER_THREAD) as u64;
        group.throughput(Throughput::Elements(total_ops));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            &num_threads,
            |b, &num_threads| {
                let pool = BufferPool::builder()
                    .local_cache(1) // force global access
                    .global_cache(8192)
                    .shards(1) // single shard
                    .batch(1)
                    .build()
                    .expect("build");
                let total_iters = num_threads * OPS_PER_THREAD;

                b.iter(|| {
                    for _ in 0..total_iters {
                        let buf = pool.get(SIZE).expect("get");
                        black_box(buf.as_ptr());
                        drop(buf);
                    }
                });

                assert_eq!(pool.stats().counters.outstanding, 0);
            },
        );
    }

    group.finish();
}

// ============================================================================
// Contention comparison: shards 1 vs 8 vs 16 (Category 10)
// ============================================================================

fn bench_contention_shard_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention/shard_compare");
    const NUM_THREADS: usize = 8;
    const SIZE: usize = 4096;
    let total_ops = (NUM_THREADS * OPS_PER_THREAD) as u64;
    group.throughput(Throughput::Elements(total_ops));

    for &shards in &[1usize, 2, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::from_parameter(shards),
            &shards,
            |b, &shards| {
                let pool = BufferPool::builder()
                    .local_cache(1)
                    .global_cache(8192)
                    .shards(shards)
                    .batch(1)
                    .build()
                    .expect("build");
                let total_iters = NUM_THREADS * OPS_PER_THREAD;

                b.iter(|| {
                    for _ in 0..total_iters {
                        let buf = pool.get(SIZE).expect("get");
                        black_box(buf.as_ptr());
                        drop(buf);
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_scaling,
    bench_scaling_single_size,
    bench_contention_single_shard,
    bench_contention_shard_comparison,
);
criterion_main!(benches);

//! Scalability & contention benchmarks (Categories 9–10).
//!
//! Category 9 — **Scaling**: N threads each doing get+write+drop cycles on
//! the same pool. With the default config each thread gets its own
//! thread-local cache, so contention is minimal. Measures how close to
//! linear the pool scales.
//!
//! Category 10 — **Contention**: `local_cache(1)` + `shards(1)` forces every
//! operation through the single global shard Mutex. Reveals lock contention,
//! CAS retry storms, and throughput collapse at high thread counts.
//!
//! Thread counts: 1, 2, 4, 8, 16, 32, 64.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::thread;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::{criterion_group, criterion_main};

use buffer_pool::BufferPool;

/// Operations per thread per iteration.
const OPS_PER_THREAD: usize = 2_000;

// ============================================================================
// Scaling: default config, per-thread local cache (Category 9)
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
                let pool_ref = &pool;

                b.iter(|| {
                    thread::scope(|s| {
                        for _ in 0..num_threads {
                            s.spawn(|| {
                                for &(size, _) in common::WORKLOAD {
                                    let mut buf = pool_ref.get(size).expect("get");
                                    buf[0] = 0xAA;
                                    black_box(buf.as_ptr());
                                    drop(buf);
                                }
                            });
                        }
                    });
                });

                let stats = pool.stats();
                assert_eq!(
                    stats.counters.outstanding, 0,
                    "leak at {num_threads} threads"
                );
            },
        );
    }

    group.finish();
}

// ============================================================================
// Scaling efficiency: same-class single-size (Category 9 — isolated)
// ============================================================================

/// All threads allocate the same size (4 KiB). With per-thread caches this
/// should scale near-linearly; any deviation indicates counter or allocator
/// contention.
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
                let pool_ref = &pool;

                b.iter(|| {
                    thread::scope(|s| {
                        for _ in 0..num_threads {
                            s.spawn(|| {
                                for _ in 0..OPS_PER_THREAD {
                                    let mut buf = pool_ref.get(SIZE).expect("get");
                                    buf[0] = 1;
                                    black_box(buf.as_ptr());
                                    drop(buf);
                                }
                            });
                        }
                    });
                });

                assert_eq!(pool.stats().counters.outstanding, 0);
            },
        );
    }

    group.finish();
}

// ============================================================================
// Contention: local_cache(1) + shards(1) — maximise lock contention (Cat 10)
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
                    .shards(1) // single shard → all threads contend
                    .batch(1)
                    .build()
                    .expect("build");
                let pool_ref = &pool;

                b.iter(|| {
                    thread::scope(|s| {
                        for _ in 0..num_threads {
                            s.spawn(|| {
                                for _ in 0..OPS_PER_THREAD {
                                    let buf = pool_ref.get(SIZE).expect("get");
                                    black_box(buf.as_ptr());
                                    drop(buf);
                                }
                            });
                        }
                    });
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

/// Fixed thread count (8), varying shard count. Shows how sharding reduces
/// contention.
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
                let pool_ref = &pool;

                b.iter(|| {
                    thread::scope(|s| {
                        for _ in 0..NUM_THREADS {
                            s.spawn(|| {
                                for _ in 0..OPS_PER_THREAD {
                                    let buf = pool_ref.get(SIZE).expect("get");
                                    black_box(buf.as_ptr());
                                    drop(buf);
                                }
                            });
                        }
                    });
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

//! Warm & cold cache latency benchmarks (Categories 6–7).
//!
//! Category 6 — **Warm cache**: steady-state get+drop with a hot thread-local
//! cache. Measures the absolute fast path and its tail latency (P50/P90/P99).
//!
//! Category 7 — **Cold cache**: fresh pool per batch so every get hits the
//! arena allocator. Measures raw allocation cost including page faults on
//! first touch.
//!
//! ## Reading the results
//!
//! Criterion reports the mean and median by default. For tail latency, look
//! at the `slope` distribution and enable `--noplot` to see the full
//! percentile table. Use `--profile-time 10` to get more samples for stable
//! tail percentiles.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::{criterion_group, criterion_main};

use buffer_pool::BufferPool;

// ============================================================================
// Warm cache: steady-state get+drop (Category 6)
// ============================================================================

fn bench_warm(c: &mut Criterion) {
    let mut group = c.benchmark_group("warm/get_drop");

    for &(size, label) in common::REPRESENTATIVE {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new(label, size), &size, |b, &size| {
            let pool = BufferPool::default();

            // Warm: seed the thread-local cache.
            let warmup = pool.get(size).expect("get");
            drop(warmup);

            b.iter(|| {
                let buf = pool.get(size).expect("get");
                black_box(buf.as_ptr());
                black_box(buf.len());
                drop(buf);
            });

            let stats = pool.stats();
            assert!(stats.hit_rate() > 0.9, "expected >90% hit rate");
            assert_eq!(stats.counters.outstanding, 0);
        });
    }

    group.finish();
}

// ============================================================================
// Warm cache: sustained burst (Category 6 — tail latency)
// ============================================================================

/// Runs many get+drop cycles per Criterion sample so tail latency
/// (P99 / P99.9) is dominated by rare cache-miss / refill events rather
/// than measurement noise.
fn bench_warm_sustained(c: &mut Criterion) {
    let mut group = c.benchmark_group("warm/sustained");
    group.throughput(Throughput::Bytes((64 * 10_000) as u64));

    group.bench_function("64B_x10k", |b| {
        let pool = BufferPool::default();
        let warmup = pool.get(64).expect("get");
        drop(warmup);

        b.iter(|| {
            for _ in 0..10_000 {
                let buf = pool.get(64).expect("get");
                black_box(buf.as_ptr());
                drop(buf);
            }
        });

        assert_eq!(pool.stats().counters.outstanding, 0);
    });

    group.finish();
}

// ============================================================================
// Cold cache: fresh arena allocation (Category 7)
// ============================================================================

fn bench_cold(c: &mut Criterion) {
    let mut group = c.benchmark_group("cold/get_drop");

    for &(size, label) in common::REPRESENTATIVE {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new(label, size), &size, |b, &size| {
            // Fresh pool per batch: every get is an arena allocation.
            b.iter_batched(
                BufferPool::default,
                |pool| {
                    let buf = pool.get(size).expect("get");
                    black_box(buf.as_ptr());
                    black_box(buf.len());
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Cold cache: first-touch page-fault cost (Category 7)
// ============================================================================

/// Allocates a buffer and writes to every page so the OS commits physical
/// memory. Measures the combined alloc + page-fault cost.
fn bench_cold_page_fault(c: &mut Criterion) {
    let mut group = c.benchmark_group("cold/page_fault");

    for &(size, label) in common::REPRESENTATIVE {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new(label, size), &size, |b, &size| {
            b.iter_batched(
                BufferPool::default,
                |pool| {
                    let mut buf = pool.get(size).expect("get");
                    // Touch every page (4 KiB) to force commits.
                    let mut off = 0;
                    while off < buf.len() {
                        buf[off] = 1;
                        off += 4096;
                    }
                    black_box(buf.as_ptr());
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_warm,
    bench_warm_sustained,
    bench_cold,
    bench_cold_page_fault,
);
criterion_main!(benches);

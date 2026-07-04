//! Huge allocation benchmarks (Category 15).
//!
//! Sizes above the `huge_threshold` (default 1 MiB) bypass the cache
//! hierarchy entirely — every `get` is a fresh `std::alloc::alloc` and every
//! `drop` is a direct `std::alloc::dealloc`. This file measures:
//!
//! - **roundtrip**: get + drop for 2 / 4 / 8 / 16 MiB.
//! - **write**: fill the buffer with a pattern (memory bandwidth limited).
//! - **page_fault**: alloc + first-touch of every 4 KiB page.
//! - **boundary**: largest class (1 MiB, cached) vs smallest huge (2 MiB,
//!   uncached) — shows the cache crossover cliff.

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
// Round-trip: get + drop for huge sizes (Category 15)
// ============================================================================

fn bench_huge_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("huge/roundtrip");

    for &(size, label) in common::HUGE_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(label), &size, |b, &size| {
            let pool = BufferPool::default();

            b.iter_batched(
                || pool.get(size).expect("get"),
                |buf| {
                    black_box(buf.as_ptr());
                    black_box(buf.len());
                    // drop runs after the timer captures the routine body;
                    // PerIteration ensures only one buffer is live at a time.
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Write: fill huge buffer with a pattern (Category 15 — bandwidth)
// ============================================================================

fn bench_huge_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("huge/write");

    for &(size, label) in common::HUGE_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(label), &size, |b, &size| {
            let pool = BufferPool::default();

            b.iter_batched(
                || pool.get(size).expect("get"),
                |mut buf| {
                    common::fill_pattern(&mut buf, 0x42);
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Page fault: alloc + first-touch every page (Category 15)
// ============================================================================

fn bench_huge_page_fault(c: &mut Criterion) {
    let mut group = c.benchmark_group("huge/page_fault");

    for &(size, label) in common::HUGE_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(label), &size, |b, &size| {
            let pool = BufferPool::default();

            b.iter_batched(
                || pool.get(size).expect("get"),
                |mut buf| {
                    // Touch one byte per page to force commits.
                    let mut off = 0;
                    while off < buf.len() {
                        buf[off] = 1;
                        black_box(buf[off]);
                        off += 4096;
                    }
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Boundary: cached 1 MiB vs uncached 2 MiB (Category 15 — crossover)
// ============================================================================

/// Shows the performance cliff when crossing the `huge_threshold`:
/// a 1 MiB buffer is served from the slab cache (warm path); a 2 MiB buffer
/// goes through `std::alloc` every time.
fn bench_huge_boundary(c: &mut Criterion) {
    let mut group = c.benchmark_group("huge/boundary");

    let cases: &[(usize, &str)] = &[
        (1_048_576, "1MiB_cached"), // largest class — cached
        (1_048_577, "1MiB+huge"),   // just over threshold — uncached
        (2_097_152, "2MiB_huge"),   // clearly huge
    ];

    for &(size, label) in cases {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(label), &size, |b, &size| {
            let pool = BufferPool::default();

            // Warm the cache for the 1 MiB class (no effect for huge).
            let warmup = pool.get(1_048_576).expect("get");
            drop(warmup);

            b.iter_batched(
                || pool.get(size).expect("get"),
                |buf| {
                    black_box(buf.as_ptr());
                    black_box(buf.len());
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_huge_roundtrip,
    bench_huge_write,
    bench_huge_page_fault,
    bench_huge_boundary,
);
criterion_main!(benches);

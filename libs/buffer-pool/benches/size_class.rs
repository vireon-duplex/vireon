//! Size-class benchmarks (Category 5).
//!
//! Measures `get + drop` for every one of the 15 power-of-2 size classes
//! (64 B – 1 MiB). This reveals per-class latency, throughput (GiB/s), and
//! memory bandwidth characteristics.
//!
//! The default pool config is used so the thread-local cache warms after
//! the first iteration — results reflect steady-state cache-hit performance.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::{criterion_group, criterion_main};

use buffer_pool::BufferPool;

// ============================================================================
// get + drop per size class (Category 5)
// ============================================================================

fn bench_class_get_drop(c: &mut Criterion) {
    let mut group = c.benchmark_group("class/get_drop");

    for (i, &size) in common::ALL_CLASS_SIZES.iter().enumerate() {
        let label = common::ALL_CLASS_LABELS[i];
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new(label, size), &size, |b, &size| {
            let pool = BufferPool::default();

            // Warm the cache.
            let warmup = pool.get(size).expect("get");
            drop(warmup);

            b.iter(|| {
                let buf = pool.get(size).expect("get");
                black_box(buf.as_ptr());
                black_box(buf.len());
                drop(buf);
            });

            assert_eq!(
                pool.stats().counters.outstanding,
                0,
                "leak in class {label}"
            );
        });
    }

    group.finish();
}

// ============================================================================
// get + write + drop per size class (Category 5 — with payload write)
// ============================================================================

/// Same as above but writes a deterministic pattern to every byte, modelling
/// real buffer usage where the caller fills the acquired buffer.
fn bench_class_get_write_drop(c: &mut Criterion) {
    let mut group = c.benchmark_group("class/get_write_drop");

    for (i, &size) in common::ALL_CLASS_SIZES.iter().enumerate() {
        let label = common::ALL_CLASS_LABELS[i];
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new(label, size), &size, |b, &size| {
            let pool = BufferPool::default();
            let warmup = pool.get(size).expect("get");
            drop(warmup);

            b.iter(|| {
                let mut buf = pool.get(size).expect("get");
                common::fill_pattern(&mut buf, 0x42);
                drop(buf);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_class_get_drop, bench_class_get_write_drop,);
criterion_main!(benches);

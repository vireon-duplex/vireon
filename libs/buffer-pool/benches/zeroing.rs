//! Zeroing strategy benchmarks (Category 14).
//!
//! Compares the four [`ZeroingPolicy`] variants across representative sizes:
//!
//! | Policy | Release cost | Acquire cost | Use case |
//! |---|---|---|---|
//! | `None` | none | none (stale data) | trusted internals |
//! | `Clear` | `memset` | none | safe default |
//! | `Secure` | volatile `memset` | none | secrets / PII |
//! | `Lazy` | none | `memset` on next get | defer cost |
//!
//! ## What to look for
//!
//! - **roundtrip** (get+drop): `None` fastest; `Clear`/`Lazy` similar; `Secure`
//!   slowest due to volatile writes.
//! - **drop** (release only): `None` ≈ `Lazy` (no work); `Clear` medium;
//!   `Secure` slowest.
//! - **get_cached** (acquire only): `Lazy` slowest (zeroes on acquire); others
//!   equal.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::{criterion_group, criterion_main};

use buffer_pool::{BufferPool, ZeroingPolicy};

const POLICIES: [(ZeroingPolicy, &str); 4] = [
    (ZeroingPolicy::None, "none"),
    (ZeroingPolicy::Clear, "clear"),
    (ZeroingPolicy::Secure, "secure"),
    (ZeroingPolicy::Lazy, "lazy"),
];

// ============================================================================
// Round-trip: get + drop (Category 14 — overall cost)
// ============================================================================

fn bench_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("zeroing/roundtrip");

    for &(size, size_label) in common::REPRESENTATIVE {
        group.throughput(Throughput::Bytes(size as u64));

        for (policy, policy_label) in POLICIES {
            let bench_id = BenchmarkId::new(policy_label, size_label);
            let pool = BufferPool::builder()
                .zeroing(policy)
                .build()
                .expect("build");

            // Warm cache.
            let warmup = pool.get(size).expect("get");
            drop(warmup);

            group.bench_with_input(bench_id, &(policy, size), |b, &(_, size)| {
                b.iter(|| {
                    let mut buf = pool.get(size).expect("get");
                    // Write some data so zeroing has real bytes to clear.
                    buf[0] = 0xFF;
                    black_box(buf.as_ptr());
                    drop(buf);
                });
            });

            assert_eq!(pool.stats().counters.outstanding, 0);
        }
    }

    group.finish();
}

// ============================================================================
// Drop only: release path (Category 14 — zeroing-on-release cost)
// ============================================================================

fn bench_drop(c: &mut Criterion) {
    let mut group = c.benchmark_group("zeroing/drop");

    for &(size, size_label) in common::REPRESENTATIVE {
        group.throughput(Throughput::Bytes(size as u64));

        for (policy, policy_label) in POLICIES {
            let bench_id = BenchmarkId::new(policy_label, size_label);
            let pool = BufferPool::builder()
                .zeroing(policy)
                .build()
                .expect("build");

            // Warm cache so the drop hits the push branch.
            let warmup = pool.get(size).expect("get");
            drop(warmup);

            group.bench_with_input(bench_id, &(policy, size), |b, &(policy, size)| {
                let _ = policy;
                b.iter_batched(
                    || {
                        let mut buf = pool.get(size).expect("get");
                        buf[0] = 0xFF; // dirty the buffer
                        buf
                    },
                    |buf| {
                        black_box(buf.as_ptr());
                        drop(buf);
                    },
                    BatchSize::PerIteration,
                );
            });
        }
    }

    group.finish();
}

// ============================================================================
// Get from cache: acquire path (Category 14 — lazy-zeroing-on-acquire cost)
// ============================================================================

fn bench_get_cached(c: &mut Criterion) {
    let mut group = c.benchmark_group("zeroing/get_cached");

    for &(size, size_label) in common::REPRESENTATIVE {
        group.throughput(Throughput::Bytes(size as u64));

        for (policy, policy_label) in POLICIES {
            let bench_id = BenchmarkId::new(policy_label, size_label);
            let pool = BufferPool::builder()
                .zeroing(policy)
                .build()
                .expect("build");

            // Warm cache.
            let warmup = pool.get(size).expect("get");
            drop(warmup);

            group.bench_with_input(bench_id, &(policy, size), |b, &(policy, size)| {
                let _ = policy;
                b.iter_batched(
                    || {
                        // Seed the cache so the timed get is a cache hit.
                        let seed = pool.get(size).expect("get");
                        drop(seed);
                    },
                    |()| {
                        let buf = pool.get(size).expect("get");
                        black_box(buf.as_ptr());
                        black_box(buf.len());
                    },
                    BatchSize::PerIteration,
                );
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_roundtrip, bench_drop, bench_get_cached,);
criterion_main!(benches);

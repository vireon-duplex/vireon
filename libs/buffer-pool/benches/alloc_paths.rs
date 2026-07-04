//! Allocation & release path isolation (Categories 1–2).
//!
//! The pool has four distinct acquire paths and three release paths. This
//! file measures each one independently so you can see exactly where time is
//! spent inside the allocator hierarchy.
//!
//! ## Acquire paths
//!
//! ```text
//! get(size) → classify
//!   1. thread-local cache pop  (fast path, ~no contention)
//!   2. global shard refill     (batch pop, lock-protected)
//!   3. arena / slab alloc      (fresh memory, bitmap alloc)
//!   4. huge alloc              (std::alloc, bypasses caches)
//! ```
//!
//! ## Release paths
//!
//! ```text
//! drop(Buffer) → release_buffer
//!   1. thread-local cache push (fast path)
//!   2. global shard drain      (local full → batch push)
//!   3. huge dealloc            (std::dealloc)
//! ```

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

const BENCH_SIZE: usize = 4096;

// ============================================================================
// Acquire: thread-local cache hit (Category 1 — fast path)
// ============================================================================

fn bench_acquire_local_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("acquire/local_hit");
    group.throughput(Throughput::Bytes(BENCH_SIZE as u64));

    group.bench_function("4KiB", |b| {
        let pool = BufferPool::default();

        // Pre-warm: one get+drop seeds the thread-local cache so every
        // subsequent iteration is a guaranteed cache hit.
        let warmup = pool.get(BENCH_SIZE).expect("get");
        drop(warmup);

        b.iter(|| {
            let buf = pool.get(BENCH_SIZE).expect("get");
            black_box(buf.as_ptr());
            black_box(buf.len());
            drop(buf);
        });

        assert_eq!(pool.stats().counters.outstanding, 0);
    });

    group.finish();
}

// ============================================================================
// Acquire: global shard refill (Category 1 — batch refill path)
// ============================================================================

fn bench_acquire_global_refill(c: &mut Criterion) {
    let mut group = c.benchmark_group("acquire/global_refill");
    group.throughput(Throughput::Bytes(BENCH_SIZE as u64));

    group.bench_function("4KiB", |b| {
        let pool = BufferPool::builder()
            .local_cache(1) // tiny local → every miss goes to global
            .batch(1) // refill one at a time
            .build()
            .expect("build");

        // Seed the global cache with 1024 slots.
        common::seed_global_cache(&pool, BENCH_SIZE, 1024);

        // iter_batched: flush local between iterations so each get
        // must refill from the global shard.
        b.iter_batched(
            || pool.flush(), // drain thread-local → global
            |()| {
                let buf = pool.get(BENCH_SIZE).expect("get");
                black_box(buf.as_ptr());
                black_box(buf.len());
            },
            BatchSize::PerIteration,
        );

        // Return borrowed buffers.
        let cleanup = pool.get(BENCH_SIZE).expect("get");
        drop(cleanup);
    });

    group.finish();
}

// ============================================================================
// Acquire: fresh arena allocation (Category 1 — cold path)
// ============================================================================

fn bench_acquire_fresh(c: &mut Criterion) {
    let mut group = c.benchmark_group("acquire/fresh_alloc");
    group.throughput(Throughput::Bytes(BENCH_SIZE as u64));

    group.bench_function("4KiB", |b| {
        // Fresh pool per batch so the arena has never been touched.
        b.iter_batched(
            BufferPool::default,
            |pool| {
                let buf = pool.get(BENCH_SIZE).expect("get");
                black_box(buf.as_ptr());
                black_box(buf.len());
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ============================================================================
// Acquire: huge allocation (Category 1 — bypasses caches)
// ============================================================================

fn bench_acquire_huge(c: &mut Criterion) {
    let mut group = c.benchmark_group("acquire/huge_alloc");

    for &(size, label) in common::HUGE_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(label), &size, |b, &size| {
            let pool = BufferPool::default();

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

// ============================================================================
// Release: thread-local cache push (Category 2 — fast path)
// ============================================================================

fn bench_release_local_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("release/local_push");
    group.throughput(Throughput::Bytes(BENCH_SIZE as u64));

    group.bench_function("4KiB", |b| {
        let pool = BufferPool::default();

        // Pre-warm so the release path hits the cache-push branch.
        let warmup = pool.get(BENCH_SIZE).expect("get");
        drop(warmup);

        b.iter_batched(
            || pool.get(BENCH_SIZE).expect("get"),
            |buf| {
                black_box(buf.as_ptr());
                drop(buf);
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

// ============================================================================
// Release: global shard drain (Category 2 — overflow path)
// ============================================================================

fn bench_release_global_drain(c: &mut Criterion) {
    let mut group = c.benchmark_group("release/global_drain");
    group.throughput(Throughput::Bytes(BENCH_SIZE as u64));

    group.bench_function("4KiB", |b| {
        // local_cache(1): every drop after the first must drain to global.
        let pool = BufferPool::builder()
            .local_cache(1)
            .global_cache(4096)
            .batch(1)
            .build()
            .expect("build");

        // Seed the local cache so the first drop fills the single slot.
        let seed = pool.get(BENCH_SIZE).expect("get");
        drop(seed);

        b.iter_batched(
            || pool.get(BENCH_SIZE).expect("get"),
            |buf| {
                black_box(buf.as_ptr());
                drop(buf); // local is full → drain to global, then push.
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

// ============================================================================
// Release: huge deallocation (Category 2 — direct dealloc)
// ============================================================================

fn bench_release_huge(c: &mut Criterion) {
    let mut group = c.benchmark_group("release/huge_drop");

    for &(size, label) in common::HUGE_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(label), &size, |b, &size| {
            let pool = BufferPool::default();

            b.iter_batched(
                || pool.get(size).expect("get"),
                |buf| {
                    black_box(buf.as_ptr());
                    drop(buf); // direct std::dealloc
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_acquire_local_hit,
    bench_acquire_global_refill,
    bench_acquire_fresh,
    bench_acquire_huge,
    bench_release_local_push,
    bench_release_global_drain,
    bench_release_huge,
);
criterion_main!(benches);

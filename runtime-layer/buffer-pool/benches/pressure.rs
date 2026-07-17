//! Memory pressure & backpressure benchmarks (Categories 12–13).
//!
//! Category 12 — **Memory pressure**: artificially limited `max_memory` so
//! the pool must recycle aggressively. Measures reuse rate, rejection rate,
//! and the cost of operating under tight caps.
//!
//! Category 13 — **Backpressure policies**: `try_get` rejection cost, `get`
//! wait/wake latency, and multi-thread throughput under a tight byte ceiling.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::{criterion_group, criterion_main};

use buffer_pool::BufferPool;

const SIZE: usize = 4096;

// ============================================================================
// Pressure: recycle rate under tight cap (Category 12)
// ============================================================================

/// Pool capped at 128 KiB (~32 × 4 KiB buffers outstanding). After the
/// initial fill, every get+drop must recycle a previously released slot.
fn bench_pressure_recycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("pressure/recycle");
    group.throughput(Throughput::Bytes(SIZE as u64));

    group.bench_function("128KiB_cap", |b| {
        let pool = BufferPool::builder()
            .max_memory(128 * 1024)
            .build()
            .expect("build");

        b.iter(|| {
            let buf = pool.get(SIZE).expect("get");
            black_box(buf.as_ptr());
            black_box(buf.len());
            drop(buf);
        });

        let stats = pool.stats();
        // At least one reuse should have occurred after the warm-up phase.
        let total = stats.counters.hits + stats.counters.misses + stats.counters.reuses;
        let _ = black_box((total, stats.reuse_rate()));
        assert_eq!(stats.counters.outstanding, 0);
    });

    group.finish();
}

// ============================================================================
// Pressure: try_get success path (Category 12 — baseline)
// ============================================================================

fn bench_try_get_ok(c: &mut Criterion) {
    let mut group = c.benchmark_group("pressure/try_get_ok");
    group.throughput(Throughput::Bytes(SIZE as u64));

    group.bench_function("under_cap", |b| {
        let pool = BufferPool::default();

        let warmup = pool.get(SIZE).expect("get");
        drop(warmup);

        b.iter(|| {
            let buf = pool.try_get(SIZE).expect("try_get");
            black_box(buf.as_ptr());
            drop(buf);
        });
    });

    group.finish();
}

// ============================================================================
// Pressure: try_get rejection (Category 12 — rejection cost)
// ============================================================================

fn bench_try_get_rejected(c: &mut Criterion) {
    let mut group = c.benchmark_group("pressure/try_get_rejected");
    group.throughput(Throughput::Elements(1));

    group.bench_function("at_capacity", |b| {
        // 1 KiB cap → a single 1 KiB buffer fills it.
        let pool = BufferPool::builder()
            .max_memory(1024)
            .build()
            .expect("build");

        // Hold the sole buffer so capacity is full for the entire bench.
        let _held = pool.get(1024).expect("get");

        b.iter(|| {
            let result = pool.try_get(1024);
            assert!(
                matches!(result, Err(buffer_pool::BufferPoolError::Rejected { .. })),
                "expected Rejected"
            );
            let _ = black_box(result);
        });
    });

    group.finish();
}

// ============================================================================
// Backpressure: throughput under tight cap (Category 13)
// ============================================================================
//
// Was multi-threaded; now single-threaded because BufferPool is `!Send`.
// Each iteration runs `num_threads * 1000` get+drop cycles sequentially,
// preserving the same total work.

fn bench_backpressure_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("backpressure/throughput");

    for &num_threads in &[2usize, 4, 8] {
        let ops = (num_threads * 1000) as u64;
        group.throughput(Throughput::Elements(ops));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            &num_threads,
            |b, &num_threads| {
                let pool = BufferPool::builder()
                    .max_memory(256 * 1024) // ~64 × 4 KiB
                    .build()
                    .expect("build");
                let total_ops = num_threads * 1000;

                b.iter(|| {
                    for _ in 0..total_ops {
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
// Backpressure: coordinated wait + wake latency (Category 13)
// ============================================================================
//
// DISABLED: This benchmark fundamentally requires multi-threading (one
// thread holds the buffer while the main thread parks on `get`). BufferPool
// is now `!Send` (uses `Rc` internally), so sharing a pool across threads
// is no longer possible. The park/notify mechanism has been removed from
// the pool, so this benchmark would not be meaningful even if it compiled.
//
/*
/// Measures the park -> notify -> wake round-trip. A helper thread holds the
/// sole buffer; the benchmark thread signals release, then calls `get`
/// which parks until the helper drops.
///
/// **Caveat**: the measured time includes thread-scheduling overhead and a
/// small artificial delay (~10 us) that ensures the main thread actually
/// enters the park state. Compare against `pressure/try_get_ok` to isolate
/// the wake-up penalty.
fn bench_wait_wake(c: &mut Criterion) {
    let mut group = c.benchmark_group("backpressure/wait_wake");

    group.bench_function("coordinated", |b| {
        let pool = BufferPool::builder()
            .max_memory(128)
            .build()
            .expect("build");

        b.iter_batched(
            || {
                // Fill capacity.
                let held = pool.get(128).expect("get");
                let (tx, rx) = mpsc::channel::<()>();
                let dropper_pool = pool.clone();
                let handle = thread::spawn(move || {
                    rx.recv().ok();
                    thread::sleep(Duration::from_micros(10));
                    drop(held);
                    // Keep pool alive until dropper finishes.
                    let _ = dropper_pool;
                });
                (tx, handle)
            },
            |(tx, handle)| {
                let _ = tx.send(());
                // This get parks: 128 outstanding + 64 > 128 capacity.
                let buf = pool.get(64).expect("get");
                black_box(buf.as_ptr());
                drop(buf);
                handle.join().expect("join");
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}
*/

criterion_group!(
    benches,
    bench_pressure_recycle,
    bench_try_get_ok,
    bench_try_get_rejected,
    bench_backpressure_throughput,
);
criterion_main!(benches);

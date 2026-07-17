// Benchmarks are test-adjacent: a malformed fixture SHOULD panic loudly so a
// bad workload is caught immediately. The workspace denies unwrap/expect in
// production code but allows them in tests and benchmarks.
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]

//! Criterion benchmarks for the `buffer-pool` crate.
//!
//! ## What is measured
//!
//! The two hot operations every consumer performs:
//!
//! - **acquire** — `pool.get(size)` → returns a [`Buffer`]
//! - **release** — `drop(Buffer)` → returns the slot to the cache
//!
//! Payload sizes are adapted from the FluxIO workload generator: 11 unique
//! sizes spanning 64 B (Heartbeat) to 320 KiB (VideoFrame), covering 7 of the
//! pool's 15 size classes.
//!
//! ## Anti-optimisation strategy
//!
//! Following the patterns established in `message-layer/frame/benches/frame_bench.rs`:
//!
//! - `criterion::black_box` on buffer pointers / lengths after each get so
//!   LLVM cannot hoist or elide the allocation.
//! - `iter_batched` for the cold / drop benches so setup runs outside the
//!   measured closure.
//! - Post-loop asserts on `stats.outstanding` guard against silent leaks
//!   that would skew results.
//!
//! ## Bench groups
//!
//! | Group | What it measures |
//! |---|---|
//! | `get/per_size` | `get(size)` + immediate `drop`, per realistic size |
//! | `get/warm` | Pure thread-local cache hit (fast path) |
//! | `get/cold` | Arena allocation (fresh pool each batch) |
//! | `drop/per_size` | Release path only (zeroing + cache push) |
//! | `roundtrip/workload` | All 11 sizes back-to-back (FluxIO batch) |
//! | `concurrent/roundtrip` | Sequential get+drop (was N-thread; pool is now !Send) |

use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::{black_box, criterion_group, criterion_main};

use buffer_pool::BufferPool;

// ============================================================================
// Realistic workload sizes (adapted from the FluxIO workload generator)
// ============================================================================

/// Per-type workload entry: `(requested_size, label)`.
///
/// Sizes mirror real FluxIO session traffic:
///
/// | Label | Size | Source |
/// |---|---|---|
/// | `heartbeat` | 64 B | Heartbeat / Ack / StreamClose |
/// | `stream_reset` | 256 B | StreamReset |
/// | `stream_open` | 1 KiB | StreamOpen |
/// | `error` | 2 KiB | Error / Handshake |
/// | `tool_call` | 4 KiB | ToolCall / StateUpdate |
/// | `audio_frame` | 80 KiB | Opus ~10 s @ 64 kbps |
/// | `text_token` | 100 KiB | LLM chunk |
/// | `fragment` | 120 KiB | Video NAL continuation |
/// | `tool_result` | 148 KiB | RAG results |
/// | `image_frame` | 256 KiB | JPEG |
/// | `video_frame` | 320 KiB | H.264 I-frame |
const WORKLOAD: &[(usize, &str)] = &[
    (64, "heartbeat"),
    (256, "stream_reset"),
    (1024, "stream_open"),
    (2048, "error"),
    (4096, "tool_call"),
    (80_000, "audio_frame"),
    (100_000, "text_token"),
    (120_000, "fragment"),
    (148_000, "tool_result"),
    (256_000, "image_frame"),
    (320_000, "video_frame"),
];

/// Sum of all workload sizes — used for throughput calculations.
fn total_workload_bytes() -> usize {
    WORKLOAD.iter().map(|(s, _)| *s).sum()
}

// ============================================================================
// get/per_size — acquire + immediate drop, per realistic size
// ============================================================================

fn bench_get_per_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("get/per_size");

    for &(size, name) in WORKLOAD {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new(name, size), &size, |b, &size| {
            let pool = BufferPool::default();

            b.iter(|| {
                let buf = pool.get(size).expect("get");
                // Force the compiler to materialise the allocation.
                black_box(buf.as_ptr());
                black_box(buf.len());
                // Drop returns the slot to the thread-local cache.
                drop(buf);
            });
        });

        // Verify no leaks after the benchmark loop.
        let pool = BufferPool::default();
        let buf = pool.get(size).unwrap();
        drop(buf);
        let stats = pool.stats();
        assert_eq!(
            stats.counters.outstanding, 0,
            "outstanding should be 0 after drop"
        );
    }

    group.finish();
}

// ============================================================================
// get/warm — pure thread-local cache hit
// ============================================================================

fn bench_get_warm(c: &mut Criterion) {
    let mut group = c.benchmark_group("get/warm");

    // Representative sizes: small, medium, large.
    let warm_sizes: &[(usize, &str)] = &[(64, "64B"), (4096, "4KiB"), (131_072, "128KiB")];

    for &(size, label) in warm_sizes {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new(label, size), &size, |b, &size| {
            let pool = BufferPool::default();

            // Pre-warm: allocate and drop so the slot sits in the thread-local
            // cache. Every subsequent get+drop in the loop is a cache hit.
            let warmup = pool.get(size).unwrap();
            drop(warmup);

            b.iter(|| {
                let buf = pool.get(size).expect("get");
                black_box(buf.as_ptr());
                black_box(buf.len());
                drop(buf);
            });
        });
    }

    group.finish();
}

// ============================================================================
// get/cold — arena allocation path (fresh pool per batch)
// ============================================================================

fn bench_get_cold(c: &mut Criterion) {
    let mut group = c.benchmark_group("get/cold");

    let cold_sizes: &[(usize, &str)] = &[(64, "64B"), (4096, "4KiB"), (131_072, "128KiB")];

    for &(size, label) in cold_sizes {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new(label, size), &size, |b, &size| {
            // iter_batched: setup (fresh pool) runs outside the timer;
            // the closure measures only get + drop on a cold pool.
            b.iter_batched(
                BufferPool::default,
                |pool| {
                    let buf = pool.get(size).expect("get");
                    black_box(buf.as_ptr());
                    black_box(buf.len());
                    // buf + pool both drop here.
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ============================================================================
// drop/per_size — release path only
// ============================================================================

fn bench_drop_per_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("drop/per_size");

    for &(size, name) in WORKLOAD {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new(name, size), &size, |b, &size| {
            let pool = BufferPool::default();

            // Pre-warm so the release path hits the cache-push branch
            // (the common case in production).
            let warmup = pool.get(size).unwrap();
            drop(warmup);

            // iter_batched: acquire in setup (outside timer), measure
            // only the drop cost.
            //
            // PerIteration (not SmallInput): SmallInput accumulates a large
            // batch of live buffers before running any drops, which can push
            // cumulative bytes_outstanding past the pool's max_memory cap and
            // engage backpressure — the setup `get` then parks until its
            // timeout because no release happens during batch fill.
            // PerIteration runs setup immediately before each routine, so at
            // most one buffer is outstanding at a time.
            b.iter_batched(
                || pool.get(size).expect("get"),
                |buf| {
                    black_box(buf.as_ptr());
                    drop(buf);
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ============================================================================
// roundtrip/workload — all 11 sizes back-to-back (FluxIO batch)
// ============================================================================

fn bench_roundtrip_workload(c: &mut Criterion) {
    let total = total_workload_bytes();
    let mut group = c.benchmark_group("roundtrip/workload");
    group.throughput(Throughput::Bytes(total as u64));

    group.bench_function("fluxio_batch", |b| {
        let pool = BufferPool::default();

        b.iter(|| {
            let mut bufs = Vec::with_capacity(WORKLOAD.len());
            for &(size, _) in WORKLOAD {
                let buf = pool.get(size).expect("get");
                black_box(buf.as_ptr());
                bufs.push(buf);
            }
            // Drop in reverse order (LIFO — mirrors typical scope exit).
            for buf in bufs.into_iter().rev() {
                drop(buf);
            }
        });

        // Verify no leaks.
        let stats = pool.stats();
        assert_eq!(
            stats.counters.outstanding, 0,
            "outstanding should be 0 after workload roundtrip"
        );
    });

    group.finish();
}

// ============================================================================
// concurrent/roundtrip — single-threaded equivalent (was multi-threaded)
// ============================================================================
//
// BufferPool is now `!Send` (uses `Rc` internally). The original benchmark
// spawned N threads each doing `WORKLOAD.len()` get+drop cycles. We now run
// the same total number of iterations (`num_threads * WORKLOAD.len()`)
// sequentially on a single thread so throughput numbers remain comparable.

fn bench_concurrent_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent/roundtrip");

    for &num_threads in &[1usize, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            &num_threads,
            |b, &num_threads| {
                let pool = BufferPool::default();
                let total_iters = num_threads * WORKLOAD.len();

                b.iter(|| {
                    for i in 0..total_iters {
                        let (size, _) = WORKLOAD[i % WORKLOAD.len()];
                        let buf = pool.get(size).expect("get");
                        black_box(buf.as_ptr());
                        drop(buf);
                    }
                });

                let stats = pool.stats();
                assert_eq!(
                    stats.counters.outstanding, 0,
                    "outstanding should be 0 after roundtrip"
                );
            },
        );
    }

    group.finish();
}

// ============================================================================
// Criterion entry points
// ============================================================================

criterion_group!(
    benches,
    bench_get_per_size,
    bench_get_warm,
    bench_get_cold,
    bench_drop_per_size,
    bench_roundtrip_workload,
    bench_concurrent_roundtrip,
);
criterion_main!(benches);

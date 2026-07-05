//! Benchmark: `flush()` throughput at various queue depths.
//!
//! Measures the reactor drain path: scheduler drain → batch composer →
//! sink submit → in-flight track. The [`NullSink`](common::NullSink)
//! removes IO-Layer noise so only queue machinery is timed.
//!
//! Uses `iter_batched_ref` so queue construction and teardown run
//! outside the timed region.
//!
//! Expected latency: scales linearly with depth (no allocation after
//! the first flush; the batch buffer is reused).

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use send_queue::{PacketMeta, SendQueue};

use common::{NullSink, SMALL_BATCH, fill_queue, fresh_queue};

/// Pre-fills a queue to `depth`, then measures a single `flush()`.
fn bench_flush_at_depth(c: &mut Criterion) {
    let mut g = c.benchmark_group("flush_at_depth");
    for &depth in common::DEPTHS {
        g.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            b.iter_batched_ref(
                || {
                    let mut sq = fresh_queue(4096);
                    fill_queue(&mut sq, depth);
                    sq
                },
                |sq: &mut SendQueue| {
                    let mut sink = NullSink;
                    let n = sq.flush(&mut sink, 0).expect("flush");
                    black_box(n);
                },
                SMALL_BATCH,
            )
        });
    }
    g.finish();
}

/// Flush on an empty queue — measures the fast-path (no drain, no
/// batch, immediate return).
fn bench_flush_empty(c: &mut Criterion) {
    c.bench_function("flush_empty", |b| {
        b.iter_batched_ref(
            || fresh_queue(4096),
            |sq: &mut SendQueue| {
                let mut sink = NullSink;
                let n = sq.flush(&mut sink, 0).expect("flush");
                black_box(n);
            },
            SMALL_BATCH,
        )
    });
}

/// `force_flush` draining more than `batch_max_packets`.
fn bench_force_flush(c: &mut Criterion) {
    let dest = common::dest_addr();
    let mut g = c.benchmark_group("force_flush");
    for &depth in &[128usize, 512] {
        g.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            b.iter_batched_ref(
                || {
                    let mut sq = fresh_queue(4096);
                    for i in 0..depth {
                        sq.submit(i as u16, 64, dest, PacketMeta::new())
                            .expect("submit");
                    }
                    sq
                },
                |sq: &mut SendQueue| {
                    let mut sink = NullSink;
                    let n = sq.force_flush(&mut sink, 0, depth).expect("flush");
                    black_box(n);
                },
                SMALL_BATCH,
            )
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_flush_at_depth,
    bench_flush_empty,
    bench_force_flush,
);
criterion_main!(benches);

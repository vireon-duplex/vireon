//! Benchmark: `submit()` throughput (single-threaded).
//!
//! Measures the producer hot path: stats bump → backpressure admit →
//! tracking-slot alloc → slot fill → ring push → gate post_change.
//!
//! Uses `iter_batched_ref` so queue construction and teardown run
//! outside the timed region.
//!
//! Expected latency: < 100 ns per submit (no syscalls, no allocation).

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use send_queue::{PacketMeta, SendQueue};

use common::{SMALL_BATCH, dest_addr, fresh_queue};

/// Single `submit()` call on a fresh queue.
fn bench_submit_single(c: &mut Criterion) {
    let dest = dest_addr();
    c.bench_function("submit_single", |b| {
        b.iter_batched_ref(
            || fresh_queue(4096),
            |sq: &mut SendQueue| {
                sq.submit(0, 64, dest, PacketMeta::new()).expect("submit");
            },
            SMALL_BATCH,
        )
    });
}

/// Submits a burst of `N` packets into a fresh queue, measuring the
/// amortised per-packet cost.
fn bench_submit_burst(c: &mut Criterion) {
    let dest = dest_addr();
    let mut g = c.benchmark_group("submit_burst");
    for &burst in &[16usize, 64, 256] {
        g.bench_with_input(BenchmarkId::from_parameter(burst), &burst, |b, &burst| {
            b.iter_batched_ref(
                || fresh_queue(4096),
                |sq: &mut SendQueue| {
                    for i in 0..burst {
                        sq.submit(i as u16, 64, dest, PacketMeta::new())
                            .expect("submit");
                    }
                    black_box(sq.depth());
                },
                SMALL_BATCH,
            )
        });
    }
    g.finish();
}

/// Submits at varying packet sizes to check for len-sensitivity.
fn bench_submit_pkt_size(c: &mut Criterion) {
    let dest = dest_addr();
    let mut g = c.benchmark_group("submit_pkt_size");
    for &sz in common::PKT_SIZES {
        g.bench_with_input(BenchmarkId::from_parameter(sz), &sz, |b, &sz| {
            b.iter_batched_ref(
                || fresh_queue(4096),
                |sq: &mut SendQueue| {
                    sq.submit(0, sz as u32, dest, PacketMeta::new())
                        .expect("submit");
                },
                SMALL_BATCH,
            )
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_submit_single,
    bench_submit_burst,
    bench_submit_pkt_size,
);
criterion_main!(benches);

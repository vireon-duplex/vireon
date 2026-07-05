//! Benchmark: cache locality & packet-size effects.
//!
//! Two concerns:
//!
//! 1. **Ring-capacity sweep** — the ingress ring is an array of
//!    64-byte-aligned `PacketSlot`s. As the ring grows from 256 → 16 384
//!    slots, the working set crosses L1 (32 KiB) → L2 (256 KiB) → L3
//!    (8 MiB+) boundaries. Submit and flush latency should remain flat
//!    (O(1) per op) but may show cache-miss penalties at large sizes.
//!
//! 2. **Packet-size indifference** — the queue stores only metadata
//!    (`buf_index`, `len`, `dest`, …), never the payload. So submit
//!    cost should be identical for 64 B and 16 KiB packets. The bench
//!    proves the zero-copy claim.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use send_queue::{PacketMeta, SendQueue};

use common::{NullSink, SMALL_BATCH, dest_addr, fresh_queue};

/// Submit latency vs ring capacity (working-set size).
fn bench_submit_vs_ring_capacity(c: &mut Criterion) {
    let dest = dest_addr();
    let mut g = c.benchmark_group("submit_vs_capacity");
    for &cap in common::CACHE_CAPACITIES {
        g.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
            b.iter_batched_ref(
                || fresh_queue(cap),
                |sq: &mut SendQueue| {
                    sq.submit(0, 64, dest, PacketMeta::new()).expect("submit");
                },
                SMALL_BATCH,
            )
        });
    }
    g.finish();
}

/// Flush throughput vs ring capacity (drain working-set size).
fn bench_flush_vs_ring_capacity(c: &mut Criterion) {
    let dest = dest_addr();
    let mut g = c.benchmark_group("flush_vs_capacity");
    for &cap in common::CACHE_CAPACITIES {
        // Depth to flush: min(cap/2, 1024) to stay within a reasonable
        // batch while exercising the ring's working set.
        let depth = (cap / 2).min(1024);
        g.bench_with_input(
            BenchmarkId::new(format!("{cap}@{depth}"), depth),
            &(cap, depth),
            |b, &(cap, depth)| {
                b.iter_batched_ref(
                    || {
                        let mut sq = fresh_queue(cap);
                        for i in 0..depth {
                            sq.submit(i as u16, 64, dest, PacketMeta::new())
                                .expect("submit");
                        }
                        sq
                    },
                    |sq: &mut SendQueue| {
                        let mut sink = NullSink;
                        let n = sq.flush(&mut sink, 0).expect("flush");
                        black_box(n);
                    },
                    SMALL_BATCH,
                )
            },
        );
    }
    g.finish();
}

/// Submit latency vs payload size — proves the queue is zero-copy
/// (payload never enters the queue, only the buffer index).
fn bench_submit_vs_pkt_size(c: &mut Criterion) {
    let dest = dest_addr();
    let mut g = c.benchmark_group("submit_vs_pkt_size");
    for &sz in common::PKT_SIZES_EXT {
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

/// Full lifecycle (submit → flush → reclaim) vs packet size — again
/// should be flat since no payload bytes are touched.
fn bench_lifecycle_vs_pkt_size(c: &mut Criterion) {
    let dest = dest_addr();
    let mut g = c.benchmark_group("lifecycle_vs_pkt_size");
    for &sz in common::PKT_SIZES_EXT {
        g.bench_with_input(BenchmarkId::from_parameter(sz), &sz, |b, &sz| {
            b.iter_batched_ref(
                || fresh_queue(4096),
                |sq: &mut SendQueue| {
                    let id = sq
                        .submit(0, sz as u32, dest, PacketMeta::new())
                        .expect("submit");
                    let mut sink = NullSink;
                    sq.flush(&mut sink, 0).expect("flush");
                    sq.on_send_cqe(id.raw());
                    sq.on_reclaim_cqe(id.raw());
                },
                SMALL_BATCH,
            )
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_submit_vs_ring_capacity,
    bench_flush_vs_ring_capacity,
    bench_submit_vs_pkt_size,
    bench_lifecycle_vs_pkt_size,
);
criterion_main!(benches);

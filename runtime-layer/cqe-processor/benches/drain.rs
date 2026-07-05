//! Benchmark: synthetic drain via `dispatch_one` at varying batch sizes.
//!
//! We measure the full decode + dispatch path without going through a real
//! io_uring ring (which would require kernel-side CQE injection). This
//! reflects the per-CQE overhead added by the processor on top of raw
//! ring drain.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use cqe_processor::CqeProcessor;
use io::ops::OpKind;

use common::{BATCH_SIZES, LARGE_BATCH, fresh_processor, recv_completion, user_data_for};

/// Drains N synthetic Recv completions via `dispatch_one`. Measures the
/// per-CQE decode + dispatch cost at varying batch sizes.
fn bench_dispatch_at_depth(c: &mut Criterion) {
    let mut g = c.benchmark_group("dispatch_at_depth");
    for &n in BATCH_SIZES {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched_ref(
                || {
                    let proc = fresh_processor(n.max(64));
                    let ids: Vec<u64> = (0..n as u64).collect();
                    (proc, ids)
                },
                |(proc, ids)| {
                    drain_recv_batch(proc, ids);
                },
                LARGE_BATCH,
            )
        });
    }
    g.finish();
}

fn drain_recv_batch(proc: &mut CqeProcessor, ids: &[u64]) {
    for &id in ids {
        let c = recv_completion(id, 128);
        let ud = user_data_for(OpKind::Recv, id);
        let outcome = proc.dispatch_one(ud, &c, 0);
        black_box(outcome);
    }
}

/// Drains a mix of completion types — Recv 60%, Send 20%, Reclaim 10%,
/// Timeout 10%. Approximates a real reactor's workload.
fn bench_dispatch_mixed(c: &mut Criterion) {
    let n: usize = 256;
    let mut g = c.benchmark_group("dispatch_mixed");
    g.throughput(Throughput::Elements(n as u64));
    g.bench_function("n=256", |b| {
        b.iter_batched_ref(
            || fresh_processor(n.max(64)),
            |proc| {
                for i in 0..n as u64 {
                    let (c, ud) = match i % 10 {
                        0..=5 => (
                            common::recv_completion(i, 128),
                            user_data_for(OpKind::Recv, i),
                        ),
                        6..=7 => (common::send_completion(i), user_data_for(OpKind::Send, i)),
                        8 => {
                            // Reclaim — register first so the handler can resolve buf_id.
                            proc.register_send(i, (i & 0xFF) as u16);
                            (
                                common::reclaim_completion(i),
                                user_data_for(OpKind::Send, i),
                            )
                        }
                        _ => (
                            common::timeout_completion(i),
                            user_data_for(OpKind::Timeout, i),
                        ),
                    };
                    let outcome = proc.dispatch_one(ud, &c, 0);
                    black_box(outcome);
                }
            },
            LARGE_BATCH,
        )
    });
    g.finish();
}

criterion_group!(benches, bench_dispatch_at_depth, bench_dispatch_mixed);
criterion_main!(benches);

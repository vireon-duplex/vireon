//! Benchmark: large-batch drain scaling.
//!
//! Proves the processor scales O(N) with batch size and reveals any
//! per-CQE overhead growth as N increases (which would indicate cache
//! or branch-prediction regressions).

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use io::ops::OpKind;

use common::{LARGE_BATCH, fresh_processor, recv_completion, user_data_for};

/// Drains batches of 1k, 4k, 16k, 64k synthetic Recv completions. The
/// processor is rebuilt each iteration to keep stats cold.
fn bench_large_batch(c: &mut Criterion) {
    let mut g = c.benchmark_group("large_batch");
    for &n in &[1024usize, 4096, 16384, 65536] {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched_ref(
                || {
                    let proc = fresh_processor(n.max(64));
                    let ids: Vec<u64> = (0..n as u64).collect();
                    (proc, ids)
                },
                |(proc, ids)| {
                    for &id in &*ids {
                        let c = recv_completion(id, 128);
                        let ud = user_data_for(OpKind::Recv, id);
                        let outcome = proc.dispatch_one(ud, &c, 0);
                        black_box(outcome);
                    }
                },
                LARGE_BATCH,
            )
        });
    }
    g.finish();
}

/// Sustained dispatch — many short batches back-to-back. Detects per-tick
/// overhead growth (e.g. reallocation in pending_* vectors).
fn bench_sustained(c: &mut Criterion) {
    let rounds: u32 = 256;
    let per_round: u32 = 64;
    let total = (rounds as u64) * (per_round as u64);
    let mut g = c.benchmark_group("sustained_dispatch");
    g.throughput(Throughput::Elements(total));
    g.bench_function("256x64", |b| {
        b.iter_batched_ref(
            || fresh_processor(per_round as usize),
            |proc| {
                for r in 0..rounds {
                    for i in 0..per_round {
                        let id = (r as u64) * (per_round as u64) + i as u64;
                        let c = recv_completion(id, 128);
                        let ud = user_data_for(OpKind::Recv, id);
                        let outcome = proc.dispatch_one(ud, &c, 0);
                        black_box(outcome);
                    }
                    // Drain pending events to keep memory bounded.
                    let _ = proc.drain_pending_recycles();
                    let _ = proc.drain_pending_sendq_events();
                    let _ = proc.drain_pending_timer_retires();
                    let _ = proc.drain_pending_tasks();
                }
            },
            LARGE_BATCH,
        )
    });
    g.finish();
}

criterion_group!(benches, bench_large_batch, bench_sustained);
criterion_main!(benches);

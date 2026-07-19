//! `apply()` benchmarks — the per-frame hot path.
//!
//! Each group measures one policy under one workload. All benches:
//!
//! * pre-bind the stream so the lazy-default path does not pollute the
//!   measurement;
//! * use `iter_batched(... PerIteration)` so the policy state resets
//!   between samples (otherwise `last_delivered` would just keep advancing
//!   and every subsequent frame would be stale);
//! * fence inputs and the `Decision` return with `black_box` so LLVM
//!   cannot hoist or constant-fold.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "common.rs"]
mod common;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

use delivery_policy::{Decision, DeliveryPolicy, DeliveryPolicyTable};
use frame::header::{Seq, StreamId};

use common::{PAYLOAD_SMALL, payload};

const CONN: usize = 0;
const SID: StreamId = StreamId::new(1);

/// Bulk sizes — covers small batches (audio frames) up to large bursts
/// (file transfer / replay).
const BULK_SIZES: &[usize] = &[1, 16, 256, 4096];

// ---------------------------------------------------------------------------
// ReliableOrdered
// ---------------------------------------------------------------------------

fn reliable_ordered_in_order(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply/reliable_ordered/in_order");
    for &n in BULK_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let mut t = DeliveryPolicyTable::new();
                    t.bind(CONN, SID, DeliveryPolicy::ReliableOrdered).unwrap();
                    (t, payload(PAYLOAD_SMALL))
                },
                |(mut t, p)| {
                    let mut delivered = 0usize;
                    for i in 0..n as u64 {
                        let d = t.apply(CONN, SID, Seq::new(i), p.clone());
                        if matches!(d, Decision::Deliver(_)) {
                            delivered += 1;
                        }
                    }
                    black_box(delivered);
                    black_box(t);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn reliable_ordered_out_of_order(c: &mut Criterion) {
    // Pre-shuffled seq pattern that triggers buffer→drain cycles: send the
    // odd seqs first (all buffered because evens are missing), then the
    // even seqs in order (each fills a gap and drains a large suffix).
    let mut group = c.benchmark_group("apply/reliable_ordered/out_of_order");
    for &n in BULK_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let odds: Vec<u64> = (0..n as u64).filter(|i| i % 2 == 1).collect();
            let evens: Vec<u64> = (0..n as u64).filter(|i| i % 2 == 0).collect();
            b.iter_batched(
                || {
                    let mut t = DeliveryPolicyTable::new();
                    t.bind(CONN, SID, DeliveryPolicy::ReliableOrdered).unwrap();
                    (t, payload(PAYLOAD_SMALL))
                },
                |(mut t, p)| {
                    // Phase 1: all odds buffer.
                    for &i in &odds {
                        let _ = t.apply(CONN, SID, Seq::new(i), p.clone());
                    }
                    // Phase 2: evens deliver + drain.
                    let mut delivered = 0usize;
                    for &i in &evens {
                        let d = t.apply(CONN, SID, Seq::new(i), p.clone());
                        if matches!(d, Decision::Deliver(_)) {
                            delivered += 1;
                        }
                    }
                    let mut out = Vec::new();
                    t.drain_ready_for_connection(CONN, &mut out);
                    delivered += out.len();
                    black_box(delivered);
                    black_box(t);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// ReliableUnordered
// ---------------------------------------------------------------------------

fn reliable_unordered_in_order(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply/reliable_unordered/in_order");
    for &n in BULK_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let mut t = DeliveryPolicyTable::new();
                    t.bind(CONN, SID, DeliveryPolicy::ReliableUnordered)
                        .unwrap();
                    (t, payload(PAYLOAD_SMALL))
                },
                |(mut t, p)| {
                    let mut delivered = 0usize;
                    for i in 0..n as u64 {
                        let d = t.apply(CONN, SID, Seq::new(i), p.clone());
                        if matches!(d, Decision::Deliver(_)) {
                            delivered += 1;
                        }
                    }
                    black_box(delivered);
                    black_box(t);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn reliable_unordered_dup_heavy(c: &mut Criterion) {
    // Send N frames where 75% are duplicates of recent seqs. Measures
    // the dedup-set lookup cost under contention.
    let mut group = c.benchmark_group("apply/reliable_unordered/dup_heavy");
    for &n in BULK_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            // Pattern: 0,0,0,1,1,1,2,2,2,... — three copies of each seq.
            let seqs: Vec<u64> = (0..n as u64).map(|i| i / 3).collect();
            b.iter_batched(
                || {
                    let mut t = DeliveryPolicyTable::new();
                    t.bind(CONN, SID, DeliveryPolicy::ReliableUnordered)
                        .unwrap();
                    (t, payload(PAYLOAD_SMALL))
                },
                |(mut t, p)| {
                    let mut delivered = 0usize;
                    let mut dropped = 0usize;
                    for &i in &seqs {
                        match t.apply(CONN, SID, Seq::new(i), p.clone()) {
                            Decision::Deliver(_) => delivered += 1,
                            Decision::Drop => dropped += 1,
                            Decision::Buffer => {}
                        }
                    }
                    black_box((delivered, dropped));
                    black_box(t);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// RealtimeDropOld
// ---------------------------------------------------------------------------

fn realtime_drop_old_in_order(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply/realtime_drop_old/in_order");
    for &n in BULK_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let mut t = DeliveryPolicyTable::new();
                    t.bind(CONN, SID, DeliveryPolicy::RealtimeDropOld).unwrap();
                    (t, payload(PAYLOAD_SMALL))
                },
                |(mut t, p)| {
                    let mut delivered = 0usize;
                    for i in 0..n as u64 {
                        let d = t.apply(CONN, SID, Seq::new(i), p.clone());
                        if matches!(d, Decision::Deliver(_)) {
                            delivered += 1;
                        }
                    }
                    black_box(delivered);
                    black_box(t);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn realtime_drop_old_stale_heavy(c: &mut Criterion) {
    // Pattern: deliver seq=0..n-1 in order (sets cursor to n-1), then
    // re-send seqs 0..n-1 — every one should drop. Measures the cheap
    // "stale" path that dominates audio/video workloads.
    let mut group = c.benchmark_group("apply/realtime_drop_old/stale_heavy");
    for &n in BULK_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let mut t = DeliveryPolicyTable::new();
                    t.bind(CONN, SID, DeliveryPolicy::RealtimeDropOld).unwrap();
                    (t, payload(PAYLOAD_SMALL))
                },
                |(mut t, p)| {
                    // Warm up cursor.
                    for i in 0..n as u64 {
                        let _ = t.apply(CONN, SID, Seq::new(i), p.clone());
                    }
                    // All stale.
                    let mut dropped = 0usize;
                    for i in 0..n as u64 {
                        if matches!(t.apply(CONN, SID, Seq::new(i), p.clone()), Decision::Drop) {
                            dropped += 1;
                        }
                    }
                    black_box(dropped);
                    black_box(t);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// LatestOnly
// ---------------------------------------------------------------------------

fn latest_only_rapid_replace(c: &mut Criterion) {
    // N applies with increasing seqs — each replaces the prior pending
    // slot, so only the last survives the drain.
    let mut group = c.benchmark_group("apply/latest_only/rapid_replace");
    for &n in BULK_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let mut t = DeliveryPolicyTable::new();
                    t.bind(CONN, SID, DeliveryPolicy::LatestOnly).unwrap();
                    (t, payload(PAYLOAD_SMALL))
                },
                |(mut t, p)| {
                    for i in 0..n as u64 {
                        let _ = t.apply(CONN, SID, Seq::new(i), p.clone());
                    }
                    let mut out = Vec::new();
                    t.drain_ready_for_connection(CONN, &mut out);
                    black_box(out.len());
                    black_box(t);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Lazy-default path (apply to an unbound stream — measures the
// "data frame arrived before StreamOpen" warning + insert cost).
// ---------------------------------------------------------------------------

fn apply_lazy_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply/lazy_default");
    group.throughput(Throughput::Elements(1));
    group.bench_function("single", |b| {
        b.iter_batched(
            || {
                let t = DeliveryPolicyTable::new();
                (t, payload(PAYLOAD_SMALL))
            },
            |(mut t, p)| {
                let d = t.apply(CONN, SID, Seq::new(0), p);
                black_box(d);
                black_box(t);
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    reliable_ordered_in_order,
    reliable_ordered_out_of_order,
    reliable_unordered_in_order,
    reliable_unordered_dup_heavy,
    realtime_drop_old_in_order,
    realtime_drop_old_stale_heavy,
    latest_only_rapid_replace,
    apply_lazy_default,
);
criterion_main!(benches);

//! `drain_ready_for_connection` benchmarks.
//!
//! Measures the cost of flushing buffered frames. The two sub-paths are:
//!
//! * `ReliableOrdered` gap-fill — walk forward through a populated reorder
//!   map after a missing frame arrives.
//! * `LatestOnly` drain — single-slot flush.
//!
//! A third group (`scan_no_pending`) measures the early-exit cost when a
//! connection has many streams but none of them have pending work. This
//! is the steady-state hot path on the engine's per-tick drain call.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "common.rs"]
mod common;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

use delivery_policy::{DeliveryPolicy, DeliveryPolicyTable};
use frame::header::{Seq, StreamId};

use common::payload;

const CONN: usize = 0;

const DRAIN_SIZES: &[usize] = &[1, 16, 256, 1024];

/// `ReliableOrdered` — fill the reorder window with `n` future frames, then
/// deliver the missing anchor and measure drain time.
fn reliable_ordered_drain(c: &mut Criterion) {
    let mut group = c.benchmark_group("drain/reliable_ordered");
    for &n in DRAIN_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let sid = StreamId::new(1);
            b.iter_batched(
                || {
                    let mut t = DeliveryPolicyTable::new();
                    t.bind(CONN, sid, DeliveryPolicy::ReliableOrdered).unwrap();
                    // Anchor at seq=0 so all of 1..=n buffer.
                    let _ = t.apply(CONN, sid, Seq::new(0), payload(128));
                    for i in 1..=n as u64 {
                        let _ = t.apply(CONN, sid, Seq::new(i), payload(128));
                    }
                    (t, sid)
                },
                |(mut t, sid)| {
                    // seq=0 already delivered; drain walks the n buffered
                    // entries immediately.
                    let mut out = Vec::new();
                    t.drain_ready_for_connection(CONN, &mut out);
                    black_box(out.len());
                    black_box(t);
                    let _ = sid;
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// `LatestOnly` — N applies replace the pending slot; drain emits one.
fn latest_only_drain(c: &mut Criterion) {
    let mut group = c.benchmark_group("drain/latest_only");
    for &n in &[1usize, 16, 256, 1024] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let sid = StreamId::new(2);
            b.iter_batched(
                || {
                    let mut t = DeliveryPolicyTable::new();
                    t.bind(CONN, sid, DeliveryPolicy::LatestOnly).unwrap();
                    for i in 0..n as u64 {
                        let _ = t.apply(CONN, sid, Seq::new(i), payload(128));
                    }
                    (t, sid)
                },
                |(mut t, sid)| {
                    let mut out = Vec::new();
                    t.drain_ready_for_connection(CONN, &mut out);
                    black_box(out.len());
                    black_box(t);
                    let _ = sid;
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// Steady-state scan: `streams_per_conn` bound streams on a connection,
/// none of which have pending work. The drain call walks all of them.
/// This is the dominant cost on the engine's per-tick drain call when
/// most streams are `RealtimeDropOld` / `ReliableUnordered`.
fn scan_no_pending(c: &mut Criterion) {
    let mut group = c.benchmark_group("drain/scan_no_pending");
    for streams in [1usize, 64, 1024, 8192] {
        group.throughput(Throughput::Elements(streams as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(streams),
            &streams,
            |b, &streams| {
                b.iter_batched(
                    || {
                        let mut t = DeliveryPolicyTable::new();
                        for i in 0..streams as u64 {
                            let sid = StreamId::new(i);
                            t.bind(CONN, sid, DeliveryPolicy::RealtimeDropOld).unwrap();
                            // Deliver one frame to fully initialise state.
                            let _ = t.apply(CONN, sid, Seq::new(0), payload(128));
                        }
                        t
                    },
                    |mut t| {
                        let mut out = Vec::new();
                        t.drain_ready_for_connection(CONN, &mut out);
                        black_box(out.len());
                        black_box(t);
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    reliable_ordered_drain,
    latest_only_drain,
    scan_no_pending,
);
criterion_main!(benches);

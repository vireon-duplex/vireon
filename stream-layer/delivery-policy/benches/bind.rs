//! `bind()` benchmarks — stream-open and policy-upgrade paths.
//!
//! * `fresh` — bind a stream id that has never been seen.
//! * `upgrade_from_default` — lazy-default ReliableOrdered state already
//!   exists with `pending_count` buffered frames; a late `StreamOpen`
//!   arrives and the table flushes everything before swapping to the
//!   new policy.
//! * `upgrade_same_policy` — bind the same policy twice (no-op flush).

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "common.rs"]
mod common;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

use delivery_policy::{DeliveryPolicy, DeliveryPolicyTable};
use frame::header::{Seq, StreamId};

use common::payload;

const CONN: usize = 0;

const UPGRADE_PENDING: &[usize] = &[0, 4, 16, 64, 256];

/// Fresh bind (no prior state) — measures FxHashMap insert + PerStreamState::new.
fn fresh(c: &mut Criterion) {
    let mut group = c.benchmark_group("bind/fresh");
    group.throughput(Throughput::Elements(1));
    group.bench_function("single", |b| {
        b.iter_batched(
            DeliveryPolicyTable::new,
            |mut t| {
                t.bind(CONN, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
                    .unwrap();
                black_box(t);
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Lazy-default ReliableOrdered already exists with `pending_count`
/// frames buffered. Late StreamOpen arrives with a new policy → flush + swap.
fn upgrade_from_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("bind/upgrade_from_default");
    for &pending in UPGRADE_PENDING {
        group.throughput(Throughput::Elements(pending as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(pending),
            &pending,
            |b, &pending| {
                let sid = StreamId::new(1);
                b.iter_batched(
                    || {
                        let mut t = DeliveryPolicyTable::new();
                        // Anchor + pending_count out-of-order frames.
                        let _ = t.apply(CONN, sid, Seq::new(0), payload(128));
                        for i in 1..=pending as u64 {
                            // seqs 2,4,6,... — leaves odd gaps so all buffer.
                            let _ = t.apply(CONN, sid, Seq::new(i * 2), payload(128));
                        }
                        (t, sid)
                    },
                    |(mut t, sid)| {
                        let drained = t.bind(CONN, sid, DeliveryPolicy::RealtimeDropOld).unwrap();
                        black_box(drained.len());
                        black_box(t);
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

/// Bind the same policy twice — second call flushes nothing (Vec empty)
/// but still walks the lookup + state-swap code path.
fn upgrade_same_policy(c: &mut Criterion) {
    let mut group = c.benchmark_group("bind/same_policy");
    group.throughput(Throughput::Elements(1));
    group.bench_function("single", |b| {
        let sid = StreamId::new(1);
        b.iter_batched(
            || {
                let mut t = DeliveryPolicyTable::new();
                t.bind(CONN, sid, DeliveryPolicy::ReliableOrdered).unwrap();
                t
            },
            |mut t| {
                let drained = t.bind(CONN, sid, DeliveryPolicy::ReliableOrdered).unwrap();
                black_box(drained.len());
                black_box(t);
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, fresh, upgrade_from_default, upgrade_same_policy);
criterion_main!(benches);

//! Benchmark: retry queue scaling & timeout callback cost.
//!
//! Measures two concerns:
//!
//! 1. **`on_timeout` at scale** — the timer-wheel callback fires once
//!    per timed-out packet. With 1 000 / 10 000 in-flight packets, the
//!    `InFlightTable` (direct-mapped by slot index) should still be
//!    O(1) per lookup. The bench sweeps the in-flight count to verify.
//!
//! 2. **Retry accumulation** — repeatedly timing out the same packet
//!    bumps its retry counter until `max_retries` is hit, then drops.
//!    Measures the full retry → drop lifecycle.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use send_queue::config::RetryConfig;
use send_queue::lifecycle::InFlightTable;
use send_queue::lifecycle::encode_id;
use send_queue::retry::{DropReason, RetryTracker};

/// Measures `on_timeout` for a single packet when the in-flight table
/// holds N entries. Direct-mapped table → O(1) regardless of N.
fn bench_on_timeout_at_scale(c: &mut Criterion) {
    let mut g = c.benchmark_group("on_timeout_at_scale");
    for &n in &[1usize, 100, 1_000, 10_000] {
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let cap = n.next_power_of_two().max(16);
                    let mut table = InFlightTable::new(cap);
                    // Track N-1 filler packets, then track the target.
                    for i in 1..n as u16 {
                        let id = encode_id(i, 1);
                        table.track(id, 0);
                    }
                    let target = encode_id(0, 1);
                    table.track(target, 0);
                    let tracker = RetryTracker::new(RetryConfig {
                        max_retries: 255,
                        ..RetryConfig::default()
                    });
                    (table, tracker, target)
                },
                |(mut table, tracker, target)| {
                    let r = tracker.on_timeout(&mut table, target);
                    let _ = black_box(r);
                },
                common::SMALL_BATCH,
            )
        });
    }
    g.finish();
}

/// Measures `rto_for_attempt` — the RTO lookup called on every retry.
fn bench_rto_for_attempt(c: &mut Criterion) {
    let tracker = RetryTracker::new(RetryConfig::default());
    let mut g = c.benchmark_group("rto_for_attempt");
    for &attempt in &[0u8, 3, 7, 15] {
        g.bench_with_input(
            BenchmarkId::from_parameter(attempt),
            &attempt,
            |b, &attempt| {
                b.iter(|| {
                    let rto = tracker.rto_for_attempt(black_box(attempt));
                    let _ = black_box(rto);
                })
            },
        );
    }
    g.finish();
}

/// Repeated timeouts on the same packet until it is dropped. Measures
/// the full retry accumulation + drop path.
fn bench_retry_until_drop(c: &mut Criterion) {
    let mut g = c.benchmark_group("retry_until_drop");
    for &max_retries in &[3u8, 10, 50, 255] {
        g.bench_with_input(
            BenchmarkId::from_parameter(max_retries),
            &max_retries,
            |b, &max_retries| {
                b.iter_batched(
                    || {
                        let table = InFlightTable::new(64);
                        let tracker = RetryTracker::new(RetryConfig {
                            max_retries,
                            ..RetryConfig::default()
                        });
                        (table, tracker)
                    },
                    |(mut table, tracker)| {
                        let id = encode_id(0, 1);
                        table.track(id, 0);
                        loop {
                            match tracker.on_timeout(&mut table, id) {
                                Ok(_) => continue,
                                Err(DropReason::MaxRetriesExceeded) => break,
                                Err(_) => break,
                            }
                        }
                    },
                    common::SMALL_BATCH,
                )
            },
        );
    }
    g.finish();
}

/// Measures the `on_timeout` call that the TimerWheel would make on
/// every retransmission fire. This is the "flush timeout callback"
/// cost: `SendQueue::on_timeout(id)` → retry table → in-flight table.
fn bench_sendqueue_on_timeout(c: &mut Criterion) {
    let mut g = c.benchmark_group("sendqueue_on_timeout");
    for &n_inflight in &[1usize, 100, 1_000] {
        g.bench_with_input(
            BenchmarkId::from_parameter(n_inflight),
            &n_inflight,
            |b, &n_inflight| {
                use send_queue::{PacketMeta, SendQueueBuilder};
                b.iter_batched_ref(
                    || {
                        // Build a queue with high max_retries so the
                        // timeout returns Ok(rto) instead of dropping.
                        SendQueueBuilder::new()
                            .capacity(4096)
                            .retry(RetryConfig {
                                max_retries: 255,
                                ..RetryConfig::default()
                            })
                            .build()
                            .expect("build")
                    },
                    |sq: &mut send_queue::SendQueue| {
                        let dest = common::dest_addr();
                        // Submit + flush to move packets into inflight.
                        let mut ids = Vec::with_capacity(n_inflight);
                        for i in 0..n_inflight {
                            ids.push(
                                sq.submit(i as u16, 64, dest, PacketMeta::new())
                                    .expect("submit"),
                            );
                        }
                        let mut sink = common::NullSink;
                        sq.flush(&mut sink, 0).expect("flush");
                        // Now fire on_timeout for each inflight packet.
                        for id in &ids {
                            let _ = sq.on_timeout(id.raw());
                        }
                    },
                    common::LARGE_BATCH,
                )
            },
        );
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_on_timeout_at_scale,
    bench_rto_for_attempt,
    bench_retry_until_drop,
    bench_sendqueue_on_timeout,
);
criterion_main!(benches);

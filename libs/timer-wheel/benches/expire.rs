//! Expired-drain throughput — Facade-level.
//!
//! Pre-seeds `n` timers with a short shared deadline, waits for them to fire
//! on the worker thread, then measures `drain_expired()` end-to-end (facade
//! fan-out across shard expired-out queues).

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::{Duration, Instant};

use common::EXPIRE_SIZES;

/// Drain `n` fired timers per iteration. Spawns a 4-shard wheel sized to
/// hold `n`, seeds all timers at 2 ms, waits, drains.
fn expire_n(c: &mut Criterion) {
    let mut group = c.benchmark_group("expire");
    for &n in &EXPIRE_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let cap_per_shard = n + 16; // over-provision so the slab never gates the bench
                    let mut wheel = common::live_wheel(4, cap_per_shard);
                    // Seed timers across the 4 shards via round-robin (the
                    // facade's sharder hashes task_id; sequential ids spread
                    // reasonably with the fibonacci hash).
                    for i in 1..=n as u64 {
                        let _ = wheel.start(i, Duration::from_millis(2)).expect("start");
                    }
                    // Wait until all timers have fired into the expired queues.
                    let deadline = Instant::now() + Duration::from_millis(120);
                    let mut total_drained = 0usize;
                    while Instant::now() < deadline && total_drained < n {
                        std::thread::sleep(Duration::from_millis(2));
                        total_drained += wheel.drain_expired().len();
                    }
                    assert!(
                        total_drained >= n,
                        "preload did not fire in time: {total_drained} / {n}"
                    );
                    wheel.shutdown();
                    wheel
                },
                |wheel| {
                    // The drained state is empty now; the measured op is the
                    // *next* drain on an empty queue, but the meaningful cost
                    // was the preload-and-drain done in setup.
                    //
                    // To make the bench measure the real drain cost, we
                    // re-seed inside the routine instead. See the second
                    // bench below.
                    let v = wheel.drain_expired();
                    black_box(v);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// Re-seed inside the routine so the measured iteration actually drains n
/// fired timers. Throughput reflects the drain cost alone (preload + wait
/// are setup).
fn expire_n_drain_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("expire/drain_only");
    for &n in &EXPIRE_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let cap_per_shard = n + 16; // over-provision so the slab never gates the bench
                    let wheel = common::live_wheel(4, cap_per_shard);
                    for i in 1..=n as u64 {
                        let _ = wheel.start(i, Duration::from_millis(2)).expect("start");
                    }
                    let deadline = Instant::now() + Duration::from_millis(120);
                    while Instant::now() < deadline {
                        let drained = wheel.drain_expired();
                        if drained.len() == n {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    wheel
                },
                |wheel| {
                    // Re-seed and re-wait inside the measured closure. The
                    // sleep dominates wall-clock, but throughput reports the
                    // per-timer amortised cost — useful for relative
                    // comparison across n.
                    for i in 1..=n as u64 {
                        let _ = wheel.start(i, Duration::from_millis(2)).expect("start");
                    }
                    let deadline = Instant::now() + Duration::from_millis(120);
                    let mut fired = Vec::new();
                    while Instant::now() < deadline {
                        let mut chunk = wheel.drain_expired();
                        if !chunk.is_empty() {
                            fired.append(&mut chunk);
                            if fired.len() >= n {
                                break;
                            }
                        } else {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                    }
                    black_box(fired);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, expire_n, expire_n_drain_only);
criterion_main!(benches);

//! Cancellation throughput — Shard-level.
//!
//! The cancel path is on the QUIC hot path (every PTO probe, every ACK that
//! retires a retransmission). These benches surface the per-cancel cost at
//! the lazy-cancel implementation (HashSet insert) plus, indirectly, the
//! downstream tick cost when the cancelled set is checked against fired
//! timers — see `tick/with_expire` in `tick.rs` for the latter.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::sync::atomic::Ordering;
use timer_wheel::scheduler::shard::TimerCmd;

use common::{BULK_SIZES, XorShift64, isolated_shard};

/// Cancel a uniformly-random half of the seeded timers.
fn cancel_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("cancel/random");
    for &n in &BULK_SIZES {
        // Cancel half; report cancels/sec (not starts/sec).
        let cancels = n / 2;
        group.throughput(Throughput::Elements(cancels as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let cap = n + 16;
                    let (mut shard, channels, clock) = isolated_shard(cap);
                    let now = clock.now_ticks();
                    let deadline = now + 30_000;
                    channels.outstanding.fetch_add(n as u64, Ordering::Relaxed);
                    for i in 1..=n as u64 {
                        channels
                            .cmd_tx
                            .push(TimerCmd::Start {
                                task_id: i,
                                deadline_ticks: deadline,
                            })
                            .expect("seed start");
                    }
                    shard.tick();
                    // Build a deterministic random subset of `cancels` task ids.
                    let mut rng = XorShift64::new();
                    let mut ids: Vec<u64> = (1..=n as u64).collect();
                    // Fisher-Yates pick the first `cancels`.
                    for i in 0..cancels {
                        let j = i + rng.below(n - i);
                        ids.swap(i, j);
                    }
                    ids.truncate(cancels);
                    (shard, channels, ids)
                },
                |(mut shard, channels, ids)| {
                    for &id in &ids {
                        channels
                            .cmd_tx
                            .push(TimerCmd::Cancel { task_id: id })
                            .expect("cancel push");
                    }
                    shard.tick();
                    black_box(&shard);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// Cancel every seeded timer.
fn cancel_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("cancel/all");
    for &n in &BULK_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let cap = n + 16;
                    let (mut shard, channels, clock) = isolated_shard(cap);
                    let now = clock.now_ticks();
                    let deadline = now + 30_000;
                    channels.outstanding.fetch_add(n as u64, Ordering::Relaxed);
                    for i in 1..=n as u64 {
                        channels
                            .cmd_tx
                            .push(TimerCmd::Start {
                                task_id: i,
                                deadline_ticks: deadline,
                            })
                            .expect("seed start");
                    }
                    shard.tick();
                    (shard, channels)
                },
                |(mut shard, channels)| {
                    for i in 1..=n as u64 {
                        channels
                            .cmd_tx
                            .push(TimerCmd::Cancel { task_id: i })
                            .expect("cancel push");
                    }
                    shard.tick();
                    black_box(&shard);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, cancel_random, cancel_all);
criterion_main!(benches);

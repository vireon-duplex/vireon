//! Worker-tick cost — Shard-level.
//!
//! A `Shard::tick()` does: (1) drain pending expirations, (2) drain the
//! command MPSC, (3) `wheel.advance(now)` (cascade + drain expired buckets),
//! (4) route fired/cancelled task ids. These benches isolate each leg.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::time::Duration;
use timer_wheel::scheduler::shard::TimerCmd;

use common::{DEFAULT_BENCH_CAPACITY, EXPIRE_SIZES, TICK_SIZES, isolated_shard, seed_shard};

/// Tick an empty shard. Pure loop + MPSC-empty-check overhead.
fn tick_empty(c: &mut Criterion) {
    c.bench_function("tick/empty", |b| {
        b.iter_batched(
            || isolated_shard(16),
            |(mut shard, _channels, _clock)| {
                shard.tick();
                black_box(&shard);
            },
            criterion::BatchSize::PerIteration,
        );
    });
}

/// Tick a shard with `n` live timers, none of which expire this tick.
/// Measures the wheel-scan + next_deadline cost as population grows.
fn tick_full(c: &mut Criterion) {
    let mut group = c.benchmark_group("tick/full");
    for &n in &TICK_SIZES {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let cap = n + 16;
                    let (mut shard, channels, clock) = isolated_shard(cap);
                    let deadline = clock.now_ticks() + 30_000;
                    seed_shard(&mut shard, &channels, n, deadline);
                    (shard, channels)
                },
                |(mut shard, channels)| {
                    shard.tick();
                    black_box(&shard);
                    let _ = &channels; // keep the Arc alive
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// Tick that fires `n` timers in one bucket. Surfaces drain_all + deliver
/// cost. The bucket is sized to fire all `n` on the same tick.
fn tick_with_expire(c: &mut Criterion) {
    let mut group = c.benchmark_group("tick/with_expire");
    for &n in &EXPIRE_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let cap = n + 16;
                    let (mut shard, channels, _clock) = isolated_shard(cap);
                    // Schedule all timers for tick = 1 (one tick in the future),
                    // then advance the clock by sleeping one tick before the
                    // measured call.
                    //
                    // `seed_shard` itself calls `shard.tick()`, so the start
                    // commands are absorbed but their deadline is still in the
                    // future relative to `last_tick`. After sleeping one tick
                    // (1 ms), the next `shard.tick()` will fire them.
                    let deadline = 1; // tick 1
                    seed_shard(&mut shard, &channels, n, deadline);
                    std::thread::sleep(Duration::from_millis(2));
                    (shard, channels)
                },
                |(mut shard, channels)| {
                    shard.tick();
                    black_box(&shard);
                    let _ = &channels;
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// Tick that crosses a level-0 wrap (tick 63 → 64 in a 6-bit wheel) with
/// timers queued in the level-1 bucket that must cascade down.
fn tick_with_cascade(c: &mut Criterion) {
    c.bench_function("tick/with_cascade", |b| {
        b.iter_batched(
            || {
                let (mut shard, channels, _clock) = isolated_shard(DEFAULT_BENCH_CAPACITY);
                // 6-bit wheel wraps at 64. Schedule timers in the level-1
                // bucket by giving them a deadline >= 64 ticks out.
                let deadline = 128u64; // level 1
                channels.outstanding.fetch_add(64, Ordering::Relaxed);
                for i in 1..=64u64 {
                    channels
                        .cmd_tx
                        .push(TimerCmd::Start {
                            task_id: i,
                            deadline_ticks: deadline,
                        })
                        .expect("seed");
                }
                shard.tick();
                // Now advance `last_tick` to just before the level-0 wrap by
                // re-seeding with a deadline far in the future won't help.
                // Instead, drive many ticks so the wheel cursor approaches 64.
                // We loop 60 ticks to land just before the wrap; the measured
                // tick will then complete the wrap and trigger the cascade.
                for _ in 0..60 {
                    shard.tick();
                }
                (shard, channels)
            },
            |(mut shard, channels)| {
                shard.tick(); // this tick crosses the wrap
                black_box(&shard);
                let _ = &channels;
            },
            criterion::BatchSize::PerIteration,
        );
    });
}

criterion_group!(
    benches,
    tick_empty,
    tick_full,
    tick_with_expire,
    tick_with_cascade,
);
criterion_main!(benches);

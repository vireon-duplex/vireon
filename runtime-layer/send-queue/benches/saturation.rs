//! Benchmark: queue saturation & backpressure lifecycle.
//!
//! Two concerns:
//!
//! 1. **Fill-level latency** — does `submit()` get slower as the queue
//!    fills from 50 % → 99 %? It shouldn't: every operation is O(1)
//!    (free-list pop + ring push + branch). Any deviation indicates
//!    cache-locality degradation at high occupancy.
//!
//! 2. **Backpressure cycle** — the full `enqueue → Saturated → reject
//!    → drain → resume` transition. Measures the cost of
//!    `PressureLevel` transitions and hysteresis re-arm.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use send_queue::{PacketMeta, PressureLevel, SendQueue};

use common::{NullSink, SMALL_BATCH, dest_addr, fresh_queue};

/// Pre-fills the queue to `pct` % of its high-watermark capacity, then
/// measures a single `submit()` call.
fn bench_submit_at_fill(c: &mut Criterion) {
    let dest = dest_addr();
    let mut g = c.benchmark_group("submit_at_fill");
    for &pct in common::FILL_PERCENTS {
        g.bench_with_input(BenchmarkId::from_parameter(pct), &pct, |b, &pct| {
            b.iter_batched_ref(
                || {
                    let mut sq = fresh_queue(4096);
                    // Compute how many packets yield the target fill
                    // relative to the high watermark.
                    let hw = (4096 * 90 / 100) - 1; // high_watermark - 1
                    let target = (hw * pct as usize) / 100;
                    for i in 0..target {
                        sq.submit(i as u16, 64, dest, PacketMeta::new())
                            .expect("submit");
                    }
                    sq
                },
                |sq: &mut SendQueue| {
                    sq.submit(0, 64, dest, PacketMeta::new()).expect("submit");
                },
                SMALL_BATCH,
            )
        });
    }
    g.finish();
}

/// Measures the reject cost when the queue is at 100 % (Saturated).
fn bench_submit_rejected(c: &mut Criterion) {
    let dest = dest_addr();
    c.bench_function("submit_rejected_saturated", |b| {
        b.iter_batched_ref(
            || {
                let mut sq = fresh_queue(256);
                // Fill past the high watermark to trigger Saturated.
                while sq.submit(0, 64, dest, PacketMeta::new()).is_ok() {}
                sq
            },
            |sq: &mut SendQueue| {
                let r = sq.submit(0, 64, dest, PacketMeta::new());
                let _ = black_box(r);
            },
            SMALL_BATCH,
        )
    });
}

/// Full backpressure cycle: fill to Saturated, reject, drain to Normal,
/// submit succeeds again. Measures the transition + hysteresis cost.
fn bench_backpressure_cycle(c: &mut Criterion) {
    let dest = dest_addr();
    c.bench_function("backpressure_cycle", |b| {
        b.iter_batched_ref(
            || fresh_queue(1024),
            |sq: &mut SendQueue| {
                // Phase 1: enqueue until Saturated.
                let mut pushed = Vec::new();
                while let Ok(id) = sq.submit(0, 64, dest, PacketMeta::new()) {
                    pushed.push(id);
                }
                debug_assert_eq!(sq.pressure(), PressureLevel::Saturated);

                // Phase 2: attempt submit while Saturated (rejected).
                let _ = sq.submit(0, 64, dest, PacketMeta::new());

                // Phase 3: drain everything via flush.
                let mut sink = NullSink;
                sq.flush(&mut sink, 0).expect("flush");

                // Phase 4: complete lifecycle for all pushed packets
                // to free tracking slots and drop pressure to Normal.
                for id in &pushed {
                    sq.on_send_cqe(id.raw());
                    sq.on_reclaim_cqe(id.raw());
                }

                // Phase 5: submit succeeds again (pressure cycled back).
                let _ = sq.submit(0, 64, dest, PacketMeta::new());
            },
            SMALL_BATCH,
        )
    });
}

/// Measures the cost of checking pressure level alone (relaxed atomic
/// load) at various fill levels.
fn bench_pressure_check(c: &mut Criterion) {
    let mut g = c.benchmark_group("pressure_check");
    for &pct in &[0u8, 50, 90, 99] {
        g.bench_with_input(BenchmarkId::from_parameter(pct), &pct, |b, &pct| {
            b.iter_batched_ref(
                || {
                    let mut sq = fresh_queue(1024);
                    let hw = (1024 * 90 / 100) - 1;
                    let target = (hw * pct as usize) / 100;
                    let dest = dest_addr();
                    for i in 0..target {
                        sq.submit(i as u16, 64, dest, PacketMeta::new())
                            .expect("submit");
                    }
                    sq
                },
                |sq: &mut SendQueue| {
                    black_box(sq.pressure());
                    black_box(sq.depth());
                },
                SMALL_BATCH,
            )
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_submit_at_fill,
    bench_submit_rejected,
    bench_backpressure_cycle,
    bench_pressure_check,
);
criterion_main!(benches);

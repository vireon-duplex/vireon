//! Benchmark: scheduler drain isolation.
//!
//! Isolates the `SchedulePolicy::drain` cost from the full flush path
//! (batch composer, submission sink, in-flight tracking). This is the
//! right place to compare policies once `PriorityPolicy`,
//! `DeadlinePolicy`, and `WeightedFairPolicy` are implemented.
//!
//! ## Phase 1
//!
//! Only `FifoPolicy` exists. The bench measures FIFO drain (ring pop
//! loop) at various depths. Future policies will be added here with
//! the same workload shape.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use send_queue::PacketSlot;
use send_queue::SchedulePolicyKind;
use send_queue::queue::IngressRing;
use send_queue::scheduler::{FifoPolicy, SchedulePolicy, build_policy};

use common::{SMALL_BATCH, dest_addr};

/// Fills a ring with `depth` slots, then drains them all.
fn bench_fifo_drain(c: &mut Criterion) {
    let dest = dest_addr();
    let mut g = c.benchmark_group("fifo_drain");
    for &depth in &[16usize, 64, 256, 1024, 4096] {
        g.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            b.iter_batched_ref(
                || {
                    let cap = depth.next_power_of_two().max(16);
                    let ring = IngressRing::new(cap).expect("ring");
                    for i in 0..depth {
                        let mut slot = PacketSlot::empty();
                        slot.fill(i as u64, 0, 64, dest, 0, 0, i as u64);
                        ring.push(slot).expect("push");
                    }
                    ring
                },
                |ring| {
                    let mut policy = FifoPolicy::new();
                    let slots = policy.drain(ring, depth);
                    black_box(slots.len());
                },
                SMALL_BATCH,
            )
        });
    }
    g.finish();
}

/// Measures `build_policy` construction — currently trivial for all
/// kinds (FIFO fallback), but will matter when policies own heaps.
fn bench_build_policy(c: &mut Criterion) {
    let kinds = [
        ("fifo", SchedulePolicyKind::Fifo),
        ("priority", SchedulePolicyKind::Priority),
        ("deadline", SchedulePolicyKind::Deadline),
        ("wfq", SchedulePolicyKind::WeightedFair),
    ];

    let mut g = c.benchmark_group("build_policy");
    for (name, kind) in kinds {
        g.bench_function(name, |b| {
            b.iter(|| {
                let policy = build_policy(black_box(kind));
                black_box(policy);
            })
        });
    }
    g.finish();
}

/// Drain in small batches (64) from a large ring — simulates the
/// reactor's steady-state tick where only a slice of the queue is
/// drained per flush.
fn bench_fifo_drain_partial(c: &mut Criterion) {
    let dest = dest_addr();
    let mut g = c.benchmark_group("fifo_drain_partial");
    for &(fill, batch) in &[(4096usize, 64usize), (4096, 256), (8192, 64)] {
        let label = format!("{fill}@{batch}");
        g.bench_with_input(
            BenchmarkId::new(label.clone(), batch),
            &(fill, batch),
            |b, &(fill, batch)| {
                b.iter_batched_ref(
                    || {
                        let cap = fill.next_power_of_two();
                        let ring = IngressRing::new(cap).expect("ring");
                        for i in 0..fill {
                            let mut slot = PacketSlot::empty();
                            slot.fill(i as u64, 0, 64, dest, 0, 0, i as u64);
                            ring.push(slot).expect("push");
                        }
                        ring
                    },
                    |ring| {
                        let mut policy = FifoPolicy::new();
                        // Drain only `batch` entries — partial drain.
                        let slots = policy.drain(ring, batch);
                        black_box(slots.len());
                    },
                    SMALL_BATCH,
                )
            },
        );
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_fifo_drain,
    bench_build_policy,
    bench_fifo_drain_partial,
);
criterion_main!(benches);

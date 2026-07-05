//! Benchmark: sustained-load soak test.
//!
//! Runs a high-throughput submit → flush → reclaim loop for a
//! configurable duration (default 5 s; override with
//! `--measurement-time`). Throughput should remain **constant** across
//! iterations — any degradation indicates a resource leak (tracking
//! slots, batch entries, inflight records not being reclaimed).
//!
//! ## Checking for memory leaks
//!
//! Criterion reports throughput per iteration. If later iterations are
//! slower than earlier ones, suspect a leak. To cross-check RSS:
//!
//! ```sh
//! # Run a 5-minute soak and track resident set size.
//! /usr/bin/time -v cargo bench -p send-queue --bench soak -- \
//!     --measurement-time 300 --sample-size 3
//! ```
//!
//! For multi-hour soaks use a standalone harness:
//!
//! ```sh
//! cargo bench -p send-queue --bench soak -- \
//!     --measurement-time 3600  # 1 hour
//! ```

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use send_queue::{PacketMeta, SendQueueBuilder};

use common::{NullSink, dest_addr};

const SOAK_BATCH: usize = 4_096;

/// Builds a queue with `batch_max_packets = SOAK_BATCH` so a single
/// `flush()` drains the entire batch. This creates a clean steady-state
/// cycle: submit → flush → complete → ring/tracking/inflight all empty.
fn soak_queue() -> send_queue::SendQueue {
    SendQueueBuilder::new()
        .capacity(SOAK_BATCH * 2)
        .batch_max_packets(SOAK_BATCH)
        .build()
        .expect("build")
}

/// Sustained submit → flush → reclaim cycle. Each criterion iteration
/// processes `SOAK_BATCH` complete lifecycles. If there is any leak,
/// later iterations will slow down as tracking slots or inflight
/// records accumulate.
fn bench_sustained_lifecycle(c: &mut Criterion) {
    let dest = dest_addr();
    let mut g = c.benchmark_group("soak");
    g.throughput(Throughput::Elements(SOAK_BATCH as u64));

    g.bench_function("sustained_lifecycle", |b| {
        let mut sq = soak_queue();
        let mut sink = NullSink;
        b.iter(|| {
            // Phase 1: submit a full batch.
            let mut ids = Vec::with_capacity(SOAK_BATCH);
            for i in 0..SOAK_BATCH {
                ids.push(
                    sq.submit(i as u16, 64, dest, PacketMeta::new())
                        .expect("submit"),
                );
            }
            // Phase 2: flush all (drains ring → batch → sink → inflight).
            let n = sq.flush(&mut sink, 0).expect("flush");
            debug_assert_eq!(n, SOAK_BATCH);
            // Phase 3: complete two-CQE lifecycle for every packet.
            for id in &ids {
                sq.on_send_cqe(id.raw());
                sq.on_reclaim_cqe(id.raw());
            }
            // Steady state: ring empty, inflight empty, tracking all free.
            black_box(&sq);
        })
    });

    g.finish();
}

criterion_group!(benches, bench_sustained_lifecycle);
criterion_main!(benches);

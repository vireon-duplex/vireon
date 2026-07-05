//! Benchmark: packet scheduler, pacing, GSO batching.
//!
//! Phase 10 will populate this. Targets:
//! - PacketScheduler.schedule() per tick
//! - Pacer token-bucket arithmetic
//! - GSO batch coalescing + send-queue flush

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_scheduler(c: &mut Criterion) {
    // TODO(phase-10): scheduler / pacer / GSO micro-benches.
    let _g = c.benchmark_group("scheduler");
}

criterion_group!(benches, bench_scheduler);
criterion_main!(benches);

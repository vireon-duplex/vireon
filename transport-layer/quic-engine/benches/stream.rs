//! Benchmark: stream send / recv and scheduling.
//!
//! Phase 8 will populate this. Targets:
//! - stream_send / stream_recv throughput at 1 KiB / 64 KiB / 1 MiB
//! - StreamScheduler next_ready at 1 / 100 / 1K / 10K ready streams
//! - Priority vs WeightedFair vs RoundRobin comparison

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_stream(c: &mut Criterion) {
    // TODO(phase-8): stream I/O + scheduling micro-benches.
    let _g = c.benchmark_group("stream");
}

criterion_group!(benches, bench_stream);
criterion_main!(benches);

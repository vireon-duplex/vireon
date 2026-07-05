//! Benchmark: connection migration + path validation overhead.
//!
//! Phase 12 will populate this. Targets:
//! - PATH_CHALLENGE / PATH_RESPONSE round-trip cost
//! - NAT rebinding detection
//! - Per-path RTT estimator update

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_migration(c: &mut Criterion) {
    // TODO(phase-12): migration + path validation micro-benches.
    let _g = c.benchmark_group("migration");
}

criterion_group!(benches, bench_migration);
criterion_main!(benches);

//! Benchmark: CID table lookup latency.
//!
//! Phase 1 will populate this. Targets:
//! - Inline 8-byte key FxHashMap lookup at 10K / 100K / 1M fill
//! - Stateless-reset token derivation cost
//! - CID rotation overhead

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_cid_lookup(c: &mut Criterion) {
    // TODO(phase-1): populate ConnIdTable at 10K / 100K / 1M fill
    // levels and measure O(1) lookup latency.
    let _g = c.benchmark_group("cid_lookup");
}

criterion_group!(benches, bench_cid_lookup);
criterion_main!(benches);

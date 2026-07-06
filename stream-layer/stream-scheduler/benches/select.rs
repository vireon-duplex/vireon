//! `select` bench entry point.
//!
//! Measures per-tick `StreamSelector::next_stream` cost across
//! policies. Bodies land in Phase 1 alongside the real selector.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_select(_c: &mut Criterion) {
    // TODO(phase-1): FIFO / RR / WFQ / strict-priority selection.
}

criterion_group!(benches, bench_select);
criterion_main!(benches);

//! `flow_control` bench entry point.
//!
//! Measures per-stream credit accounting and
//! `STREAM_DATA_BLOCKED` heuristics. Bodies land in Phase 2
//! alongside the real flow-control implementation.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_flow_control(_c: &mut Criterion) {
    // TODO(phase-2): consume / refresh / blocked-check benches.
}

criterion_group!(benches, bench_flow_control);
criterion_main!(benches);

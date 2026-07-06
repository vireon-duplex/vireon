//! `incremental` bench entry point.
//!
//! Measures RFC 9218 incremental-scheduling cycle-share. Bodies
//! land in Phase 2 alongside the real incremental scheduler.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_incremental(_c: &mut Criterion) {
    // TODO(phase-2): incremental cycle-share selection.
}

criterion_group!(benches, bench_incremental);
criterion_main!(benches);

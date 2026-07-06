//! `table` bench entry point.
//!
//! Measures `StreamTable` lookup / insert / remove cost. Bodies
//! land in Phase 1 alongside the real table implementation.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_table(_c: &mut Criterion) {
    // TODO(phase-1): insert / lookup / remove micro-benches.
}

criterion_group!(benches, bench_table);
criterion_main!(benches);

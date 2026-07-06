//! `slab` bench entry point.
//!
//! Measures `StreamSlab` slot allocation / release and generation
//! advance. Bodies land in Phase 1 alongside the real slab.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_slab(_c: &mut Criterion) {
    // TODO(phase-1): reserve / release / generation advance.
}

criterion_group!(benches, bench_slab);
criterion_main!(benches);

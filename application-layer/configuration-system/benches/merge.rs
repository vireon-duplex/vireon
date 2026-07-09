//! `merge` bench entry point — layer merge performance.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_merge(_c: &mut Criterion) {
    // TODO(phase-1): Multi-layer merge cost.
}

criterion_group!(benches, bench_merge);
criterion_main!(benches);

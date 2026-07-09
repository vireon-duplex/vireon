//! `reload` bench entry point — configuration reload performance.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_reload(_c: &mut Criterion) {
    // TODO(phase-1): Full reload from all sources cost.
}

criterion_group!(benches, bench_reload);
criterion_main!(benches);

//! `select_best` bench entry point — best version selection.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_select_best(_c: &mut Criterion) {
    // TODO(phase-1): Best compatible version selection cost.
}

criterion_group!(benches, bench_select_best);
criterion_main!(benches);

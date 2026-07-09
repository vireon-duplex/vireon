//! `register` bench entry point — hook registration cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_register(_c: &mut Criterion) {
    // TODO(phase-1): hook registration cost.
}

criterion_group!(benches, bench_register);
criterion_main!(benches);

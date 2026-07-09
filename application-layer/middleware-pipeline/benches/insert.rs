//! `insert` bench entry point — middleware insertion cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_insert(_c: &mut Criterion) {
    // TODO(phase-1): MiddlewareChain::register cost.
}

criterion_group!(benches, bench_insert);
criterion_main!(benches);

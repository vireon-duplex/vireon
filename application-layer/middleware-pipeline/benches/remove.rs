//! `remove` bench entry point — middleware removal cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_remove(_c: &mut Criterion) {
    // TODO(phase-1): MiddlewareChain::remove cost.
}

criterion_group!(benches, bench_remove);
criterion_main!(benches);

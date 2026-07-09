//! `execute_chain` bench entry point — chain execution cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_execute_chain(_c: &mut Criterion) {
    // TODO(phase-1): Pipeline::execute cost.
}

criterion_group!(benches, bench_execute_chain);
criterion_main!(benches);

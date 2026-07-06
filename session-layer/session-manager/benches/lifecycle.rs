//! `lifecycle` bench entry point — SessionState FSM transitions.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_lifecycle(_c: &mut Criterion) {
    // TODO(phase-1): state-transition micro-benches.
}

criterion_group!(benches, bench_lifecycle);
criterion_main!(benches);

//! `init_sequence` bench entry point — initialization sequence cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_init_sequence(_c: &mut Criterion) {
    // TODO(phase-1): LifecycleCoordinator::init_all cost.
}

criterion_group!(benches, bench_init_sequence);
criterion_main!(benches);

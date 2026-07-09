//! `shutdown_sequence` bench entry point — shutdown sequence cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_shutdown_sequence(_c: &mut Criterion) {
    // TODO(phase-1): LifecycleCoordinator::stop_all cost.
}

criterion_group!(benches, bench_shutdown_sequence);
criterion_main!(benches);

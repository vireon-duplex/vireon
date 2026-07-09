//! `compatibility_check` bench entry point — compatibility matrix check.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_compatibility_check(_c: &mut Criterion) {
    // TODO(phase-1): Compatibility matrix lookup cost.
}

criterion_group!(benches, bench_compatibility_check);
criterion_main!(benches);

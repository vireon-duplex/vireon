//! `validate` bench entry point — config validation performance.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_validate(_c: &mut Criterion) {
    // TODO(phase-1): ConfigKey and ConfigValue validation cost.
}

criterion_group!(benches, bench_validate);
criterion_main!(benches);

//! `lease` bench entry point — lease acquire/renew cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_lease(_c: &mut Criterion) {
    // TODO(phase-1): measure lease acquire + renew latency.
}

criterion_group!(benches, bench_lease);
criterion_main!(benches);

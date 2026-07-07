//! `endpoint` bench entry point — `NodeEndpoint` construction cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_endpoint(_c: &mut Criterion) {
    // TODO(phase-1): endpoint construction + lookup cost.
}

criterion_group!(benches, bench_endpoint);
criterion_main!(benches);

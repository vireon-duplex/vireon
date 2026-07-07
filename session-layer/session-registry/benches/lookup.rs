//! `lookup` bench entry point — `SessionDirectory::lookup` cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_lookup(_c: &mut Criterion) {
    // TODO(phase-1): SessionId → SessionLocator lookup cost.
}

criterion_group!(benches, bench_lookup);
criterion_main!(benches);

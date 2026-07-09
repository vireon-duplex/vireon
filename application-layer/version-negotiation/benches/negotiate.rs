//! `negotiate` bench entry point — version negotiation cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_negotiate(_c: &mut Criterion) {
    // TODO(phase-1): VersionRange + remote versions negotiation cost.
}

criterion_group!(benches, bench_negotiate);
criterion_main!(benches);

//! `ring` bench entry point — `ConsistentHash::lookup` cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_ring(_c: &mut Criterion) {
    // TODO(phase-1): measure ring lookup latency with varying
    // node counts (10, 100, 1000) and vnode counts.
}

criterion_group!(benches, bench_ring);
criterion_main!(benches);

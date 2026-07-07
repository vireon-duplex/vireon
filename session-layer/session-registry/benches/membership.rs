//! `membership` bench entry point — node add/remove cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_membership(_c: &mut Criterion) {
    // TODO(phase-1): measure node add/remove latency and ring
    // re-sort cost at varying cluster sizes.
}

criterion_group!(benches, bench_membership);
criterion_main!(benches);

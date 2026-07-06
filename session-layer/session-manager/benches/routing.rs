//! `routing` bench entry point — SessionRouter lookup.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_routing(_c: &mut Criterion) {
    // TODO(phase-1): SessionId → ConnectionId → Core resolution.
}

criterion_group!(benches, bench_routing);
criterion_main!(benches);

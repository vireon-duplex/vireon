//! `anycast` bench entry point — `AnycastRouter::select` cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_anycast(_c: &mut Criterion) {
    // TODO(phase-1): anycast candidate selection cost.
}

criterion_group!(benches, bench_anycast);
criterion_main!(benches);

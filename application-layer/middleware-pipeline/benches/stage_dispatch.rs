//! `stage_dispatch` bench entry point — stage dispatch cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_stage_dispatch(_c: &mut Criterion) {
    // TODO(phase-1): Stage-based dispatch cost.
}

criterion_group!(benches, bench_stage_dispatch);
criterion_main!(benches);

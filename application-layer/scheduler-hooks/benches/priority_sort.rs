//! `priority_sort` bench entry point — hook priority ordering cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_priority_sort(_c: &mut Criterion) {
    // TODO(phase-1): sort hooks by priority.
}

criterion_group!(benches, bench_priority_sort);
criterion_main!(benches);

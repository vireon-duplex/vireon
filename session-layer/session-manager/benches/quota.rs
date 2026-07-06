//! `quota` bench entry point — ResourceQuota accounting.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_quota(_c: &mut Criterion) {
    // TODO(phase-2): try_acquire_stream / try_acquire_memory.
}

criterion_group!(benches, bench_quota);
criterion_main!(benches);

//! `tick_overhead` bench entry point — per-tick hook dispatch cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_tick_overhead(_c: &mut Criterion) {
    // TODO(phase-1): measure tick hook overhead.
}

criterion_group!(benches, bench_tick_overhead);
criterion_main!(benches);

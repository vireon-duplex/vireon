//! `cache` bench entry point — `RoutingCache::get` / `put` cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_cache(_c: &mut Criterion) {
    // TODO(phase-1): cache get/put/invalidate cost.
}

criterion_group!(benches, bench_cache);
criterion_main!(benches);

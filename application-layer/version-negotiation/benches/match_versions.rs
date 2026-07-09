//! `match_versions` bench entry point — version matching performance.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_match_versions(_c: &mut Criterion) {
    // TODO(phase-1): Local/remote version matching cost.
}

criterion_group!(benches, bench_match_versions);
criterion_main!(benches);

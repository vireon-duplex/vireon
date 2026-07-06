//! `fairness` bench entry point.
//!
//! Measures `FairnessMetrics` recording + Jain's index computation.
//! Bodies land in Phase 1.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_fairness(_c: &mut Criterion) {
    // TODO(phase-1): record / jain_index / min_max_ratio.
}

criterion_group!(benches, bench_fairness);
criterion_main!(benches);

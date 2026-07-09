//! `topo_sort` bench entry point — topological sort cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_topo_sort(_c: &mut Criterion) {
    // TODO(phase-1): DependencyGraph::topo_sort cost.
}

criterion_group!(benches, bench_topo_sort);
criterion_main!(benches);

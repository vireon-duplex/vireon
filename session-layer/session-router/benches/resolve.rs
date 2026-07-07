//! `resolve` bench entry point — `RouteResolver::resolve` cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_resolve(_c: &mut Criterion) {
    // TODO(phase-1): SessionId → RoutingDecision resolve cost.
}

criterion_group!(benches, bench_resolve);
criterion_main!(benches);

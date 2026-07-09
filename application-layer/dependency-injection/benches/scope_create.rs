//! Benchmark for scope creation operations.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use dependency_injection::*;

fn bench_create_single_scope(c: &mut Criterion) {
    let cfg = ContainerConfig::new();
    let mut container = Container::new(cfg);

    c.bench_function("create_single_scope", |b| {
        b.iter(|| {
            let _ = black_box(container.create_scope());
        })
    });
}

fn bench_create_many_scopes(c: &mut Criterion) {
    let cfg = ContainerConfig::new();
    let mut container = Container::new(cfg);

    c.bench_function("create_many_scopes", |b| {
        b.iter(|| {
            for _ in 0..10 {
                let _ = black_box(container.create_scope());
            }
        })
    });
}

fn bench_dispose_scope(c: &mut Criterion) {
    let cfg = ContainerConfig::new();
    let mut container = Container::new(cfg);

    c.bench_function("dispose_scope", |b| {
        b.iter(|| {
            let _ = black_box(container.dispose_scope(black_box(123)));
        })
    });
}

fn bench_scope_lifecycle(c: &mut Criterion) {
    c.bench_function("scope_lifecycle", |b| {
        let cfg = ContainerConfig::new();
        let mut container = Container::new(cfg);

        b.iter(|| {
            if let Ok(scope_id) = black_box(container.create_scope()) {
                let _ = black_box(container.dispose_scope(scope_id));
            }
        })
    });
}

criterion_group!(
    benches,
    bench_create_single_scope,
    bench_create_many_scopes,
    bench_dispose_scope,
    bench_scope_lifecycle
);
criterion_main!(benches);

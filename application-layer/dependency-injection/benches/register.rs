//! Benchmark for service registration operations.

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use dependency_injection::*;

fn bench_register_single(c: &mut Criterion) {
    let cfg = ContainerConfig::new();
    let mut container = Container::new(cfg);

    c.bench_function("register_single", |b| {
        b.iter(|| {
            let _ = black_box(container.register(()));
        })
    });
}

fn bench_register_many(c: &mut Criterion) {
    let cfg = ContainerConfig::new();
    let mut container = Container::new(cfg);

    c.bench_function("register_many", |b| {
        b.iter(|| {
            for _ in 0..10 {
                let _ = black_box(container.register(()));
            }
        })
    });
}

fn bench_register_with_lifetimes(c: &mut Criterion) {
    let mut group = c.benchmark_group("register_with_lifetimes");

    for lifetime in [
        ServiceLifetime::Singleton,
        ServiceLifetime::Scoped,
        ServiceLifetime::Transient,
    ]
    .iter()
    {
        group.bench_with_input(
            BenchmarkId::new("lifetime", format!("{:?}", lifetime)),
            lifetime,
            |b, &_lifetime| {
                let cfg = ContainerConfig::new();
                let mut container = Container::new(cfg);

                b.iter(|| {
                    let _ = black_box(container.register(()));
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_register_single,
    bench_register_many,
    bench_register_with_lifetimes
);
criterion_main!(benches);

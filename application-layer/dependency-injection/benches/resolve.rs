//! Benchmark for service resolution operations.

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use dependency_injection::*;

fn bench_resolve_single(c: &mut Criterion) {
    let cfg = ContainerConfig::new();
    let mut container = Container::new(cfg);

    c.bench_function("resolve_single", |b| {
        b.iter(|| {
            let token = black_box(TokenId::new(1));
            let _ = black_box(container.resolve(token));
        })
    });
}

fn bench_resolve_many(c: &mut Criterion) {
    let cfg = ContainerConfig::new();
    let mut container = Container::new(cfg);

    c.bench_function("resolve_many", |b| {
        b.iter(|| {
            for i in 1..=10 {
                let token = black_box(TokenId::new(i));
                let _ = black_box(container.resolve(token));
            }
        })
    });
}

fn bench_resolve_with_depth(c: &mut Criterion) {
    let mut group = c.benchmark_group("resolve_with_depth");

    for depth in [1, 5, 10, 20].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(depth), depth, |b, &_depth| {
            let cfg = ContainerConfig::new();
            let mut container = Container::new(cfg);

            b.iter(|| {
                for i in 1..=*depth {
                    let token = black_box(TokenId::new(i));
                    let _ = black_box(container.resolve(token));
                }
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_resolve_single,
    bench_resolve_many,
    bench_resolve_with_depth
);
criterion_main!(benches);

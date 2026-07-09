//! Benchmark for dependency graph building operations.

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use dependency_injection::*;

fn bench_build_simple_graph(c: &mut Criterion) {
    c.bench_function("build_simple_graph", |b| {
        b.iter(|| {
            let binding = BindingDescriptor::new(
                black_box(TokenId::new(1)),
                black_box(ServiceLifetime::Transient),
            );

            let _ = black_box(binding);
        })
    });
}

fn bench_build_graph_with_dependencies(c: &mut Criterion) {
    c.bench_function("build_graph_with_dependencies", |b| {
        b.iter(|| {
            let binding = BindingDescriptor::new(
                black_box(TokenId::new(1)),
                black_box(ServiceLifetime::Transient),
            )
            .add_dependency(black_box(TokenId::new(2)))
            .unwrap()
            .add_dependency(black_box(TokenId::new(3)))
            .unwrap();

            let _ = black_box(binding);
        })
    });
}

fn bench_build_complex_graph(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_complex_graph");

    for dep_count in [1, 3, 5, 8].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(dep_count),
            dep_count,
            |b, &count| {
                b.iter(|| {
                    let mut binding = BindingDescriptor::new(
                        black_box(TokenId::new(1)),
                        black_box(ServiceLifetime::Transient),
                    );

                    for i in 2..=(count + 1) {
                        binding = binding.add_dependency(black_box(TokenId::new(i))).unwrap();
                    }

                    let _ = black_box(binding);
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_build_simple_graph,
    bench_build_graph_with_dependencies,
    bench_build_complex_graph
);
criterion_main!(benches);

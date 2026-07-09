#![allow(missing_docs)]

//! Benchmark for composition root operations.
//!
//! Measures the throughput of composition-related operations.

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use application_runtime::{
    AppRuntimeConfig, AppRuntimeStats, CompositionRoot, RuntimeId, RuntimeRole,
};

fn bench_composition_new(c: &mut Criterion) {
    let cfg = AppRuntimeConfig::new();
    c.bench_function("composition_new", |b| {
        b.iter(|| {
            let root = CompositionRoot::new(cfg, AppRuntimeStats::new());
            black_box(root);
        });
    });
}

fn bench_composition_compose(c: &mut Criterion) {
    let cfg = AppRuntimeConfig::new();
    let stats = AppRuntimeStats::new();
    let root = CompositionRoot::new(cfg, stats);
    c.bench_function("composition_compose", |b| {
        b.iter(|| {
            let _ = root.compose();
        });
    });
}

fn bench_composition_resolve(c: &mut Criterion) {
    let cfg = AppRuntimeConfig::new();
    let stats = AppRuntimeStats::new();
    let root = CompositionRoot::new(cfg, stats);
    c.bench_function("composition_resolve", |b| {
        b.iter(|| {
            let _ = root.resolve(black_box("auth"));
        });
    });
}

fn bench_composition_health(c: &mut Criterion) {
    let cfg = AppRuntimeConfig::new();
    let stats = AppRuntimeStats::new();
    let root = CompositionRoot::new(cfg, stats);
    c.bench_function("composition_health", |b| {
        b.iter(|| {
            black_box(root.health());
        });
    });
}

fn bench_runtime_id_new(c: &mut Criterion) {
    c.bench_function("runtime_id_new", |b| {
        b.iter(|| {
            let id = RuntimeId::new(black_box(42));
            black_box(id);
        });
    });
}

fn bench_runtime_id_nil(c: &mut Criterion) {
    c.bench_function("runtime_id_nil", |b| {
        b.iter(|| {
            let id = RuntimeId::nil();
            black_box(id.is_nil());
        });
    });
}

fn bench_runtime_role_is_clustered(c: &mut Criterion) {
    c.bench_function("runtime_role_is_clustered", |b| {
        b.iter(|| {
            black_box(RuntimeRole::Cluster.is_clustered());
        });
    });
}

criterion_group!(
    benches,
    bench_composition_new,
    bench_composition_compose,
    bench_composition_resolve,
    bench_composition_health,
    bench_runtime_id_new,
    bench_runtime_id_nil,
    bench_runtime_role_is_clustered
);
criterion_main!(benches);

#![allow(missing_docs)]

//! Benchmark for runtime startup operations.
//!
//! Measures the throughput of startup-related operations.

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use application_runtime::{
    AppRuntime, AppRuntimeBuilder, AppRuntimeConfig, AppRuntimeStats, Preset, RuntimeId,
};

fn bench_config_new(c: &mut Criterion) {
    c.bench_function("config_new", |b| {
        b.iter(|| {
            let cfg = AppRuntimeConfig::new();
            black_box(cfg);
        });
    });
}

fn bench_config_validate(c: &mut Criterion) {
    let cfg = AppRuntimeConfig::new();
    c.bench_function("config_validate", |b| {
        b.iter(|| {
            let _ = black_box(cfg.validate());
        });
    });
}

fn bench_builder_build(c: &mut Criterion) {
    c.bench_function("builder_build", |b| {
        b.iter(|| {
            let _ = AppRuntimeBuilder::new().build();
        });
    });
}

fn bench_builder_from_preset(c: &mut Criterion) {
    c.bench_function("builder_from_preset", |b| {
        b.iter(|| {
            let _ = AppRuntimeBuilder::from_preset(black_box(Preset::Production)).build();
        });
    });
}

fn bench_runtime_new(c: &mut Criterion) {
    let cfg = AppRuntimeConfig::new();
    let id = RuntimeId::new(1);
    c.bench_function("runtime_new", |b| {
        b.iter(|| {
            let rt = AppRuntime::new(cfg, id, AppRuntimeStats::new());
            black_box(rt);
        });
    });
}

fn bench_runtime_start(c: &mut Criterion) {
    let cfg = AppRuntimeConfig::new();
    let id = RuntimeId::new(1);
    let stats = AppRuntimeStats::new();
    let rt = AppRuntime::new(cfg, id, stats);
    c.bench_function("runtime_start", |b| {
        b.iter(|| {
            let _ = rt.start();
        });
    });
}

fn bench_stats_record_startup(c: &mut Criterion) {
    let stats = AppRuntimeStats::new();
    c.bench_function("stats_record_startup", |b| {
        b.iter(|| {
            stats.record_startup();
        });
    });
}

fn bench_stats_snapshot(c: &mut Criterion) {
    let stats = AppRuntimeStats::new();
    stats.record_startup();
    stats.record_module_loaded();
    c.bench_function("stats_snapshot", |b| {
        b.iter(|| {
            black_box(stats.snapshot());
        });
    });
}

criterion_group!(
    benches,
    bench_config_new,
    bench_config_validate,
    bench_builder_build,
    bench_builder_from_preset,
    bench_runtime_new,
    bench_runtime_start,
    bench_stats_record_startup,
    bench_stats_snapshot
);
criterion_main!(benches);

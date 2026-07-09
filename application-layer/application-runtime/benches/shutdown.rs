#![allow(missing_docs)]

//! Benchmark for runtime shutdown operations.
//!
//! Measures the throughput of shutdown-related operations.

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use application_runtime::{AppRuntime, AppRuntimeConfig, AppRuntimeStats, HealthReport, RuntimeId};

fn bench_runtime_stop(c: &mut Criterion) {
    let cfg = AppRuntimeConfig::new();
    let id = RuntimeId::new(1);
    let stats = AppRuntimeStats::new();
    let rt = AppRuntime::new(cfg, id, stats);
    c.bench_function("runtime_stop", |b| {
        b.iter(|| {
            let _ = rt.stop();
        });
    });
}

fn bench_runtime_health(c: &mut Criterion) {
    let cfg = AppRuntimeConfig::new();
    let id = RuntimeId::new(1);
    let stats = AppRuntimeStats::new();
    let rt = AppRuntime::new(cfg, id, stats);
    c.bench_function("runtime_health", |b| {
        b.iter(|| {
            black_box(rt.health());
        });
    });
}

fn bench_health_derive(c: &mut Criterion) {
    let stats = AppRuntimeStats::new();
    let snapshot = stats.snapshot();
    c.bench_function("health_derive", |b| {
        b.iter(|| {
            black_box(HealthReport::derive(&snapshot));
        });
    });
}

fn bench_stats_record_shutdown(c: &mut Criterion) {
    let stats = AppRuntimeStats::new();
    c.bench_function("stats_record_shutdown", |b| {
        b.iter(|| {
            stats.record_shutdown();
        });
    });
}

criterion_group!(
    benches,
    bench_runtime_stop,
    bench_runtime_health,
    bench_health_derive,
    bench_stats_record_shutdown
);
criterion_main!(benches);

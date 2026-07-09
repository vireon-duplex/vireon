#![allow(missing_docs)]

//! Benchmark for health check operations.
//!
//! Measures the throughput of health-related operations.

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use application_runtime::{AppRuntimeStats, HealthReport, HealthState, StatsSnapshot};

fn bench_stats_snapshot(c: &mut Criterion) {
    let stats = AppRuntimeStats::new();
    for _ in 0..100 {
        stats.record_startup();
        stats.record_module_loaded();
    }
    c.bench_function("health_stats_snapshot", |b| {
        b.iter(|| {
            black_box(stats.snapshot());
        });
    });
}

fn bench_health_derive_healthy(c: &mut Criterion) {
    let snapshot = StatsSnapshot {
        modules_loaded: 100,
        modules_started: 100,
        modules_failed: 0,
        startups_total: 100,
        shutdowns_total: 0,
        health_checks_total: 50,
    };
    c.bench_function("health_derive_healthy", |b| {
        b.iter(|| {
            black_box(HealthReport::derive(black_box(&snapshot)));
        });
    });
}

fn bench_health_derive_degraded(c: &mut Criterion) {
    let snapshot = StatsSnapshot {
        modules_loaded: 100,
        modules_started: 95,
        modules_failed: 5,
        startups_total: 100,
        shutdowns_total: 0,
        health_checks_total: 50,
    };
    c.bench_function("health_derive_degraded", |b| {
        b.iter(|| {
            black_box(HealthReport::derive(black_box(&snapshot)));
        });
    });
}

fn bench_health_state_worst_of(c: &mut Criterion) {
    c.bench_function("health_state_worst_of", |b| {
        b.iter(|| {
            black_box(HealthState::worst_of(
                black_box(HealthState::Healthy),
                black_box(HealthState::Degraded),
            ));
        });
    });
}

fn bench_startup_success_rate_bps(c: &mut Criterion) {
    let snapshot = StatsSnapshot {
        modules_loaded: 100,
        modules_started: 97,
        modules_failed: 3,
        startups_total: 100,
        shutdowns_total: 0,
        health_checks_total: 50,
    };
    c.bench_function("startup_success_rate_bps", |b| {
        b.iter(|| {
            black_box(snapshot.startup_success_rate_bps());
        });
    });
}

criterion_group!(
    benches,
    bench_stats_snapshot,
    bench_health_derive_healthy,
    bench_health_derive_degraded,
    bench_health_state_worst_of,
    bench_startup_success_rate_bps
);
criterion_main!(benches);

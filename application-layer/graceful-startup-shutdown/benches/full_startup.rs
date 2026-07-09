//! Benchmark for full startup sequence.
//!
//! Measures the cost of running through the entire startup phase sequence.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use graceful_startup_shutdown::{GracefulConfig, StartupShutdownCoordinator};

fn bench_coordinator_creation(c: &mut Criterion) {
    c.bench_function("coordinator_new", |b| {
        let cfg = GracefulConfig::new();
        b.iter(|| black_box(StartupShutdownCoordinator::new(black_box(cfg))))
    });
}

fn bench_full_startup_sequence(c: &mut Criterion) {
    c.bench_function("full_startup_sequence", |b| {
        let cfg = GracefulConfig::new();
        b.iter(|| {
            let mut coordinator = StartupShutdownCoordinator::new(black_box(cfg));
            let _ = coordinator.startup();
            for _ in 0..5 {
                let _ = coordinator.advance_startup();
            }
            black_box(coordinator.current_startup_phase())
        })
    });
}

fn bench_health_check(c: &mut Criterion) {
    let coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
    c.bench_function("health_check", |b| {
        b.iter(|| black_box(coordinator.health()))
    });
}

fn bench_current_phases(c: &mut Criterion) {
    let coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
    c.bench_function("current_startup_phase", |b| {
        b.iter(|| black_box(coordinator.current_startup_phase()))
    });

    c.bench_function("current_shutdown_phase", |b| {
        b.iter(|| black_box(coordinator.current_shutdown_phase()))
    });
}

criterion_group!(
    benches,
    bench_coordinator_creation,
    bench_full_startup_sequence,
    bench_health_check,
    bench_current_phases
);
criterion_main!(benches);

//! Benchmark for phase transition performance.
//!
//! Measures the cost of transitioning between startup and shutdown phases.

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use graceful_startup_shutdown::{ShutdownPhase, StartupPhase};

fn bench_startup_transition(c: &mut Criterion) {
    let mut group = c.benchmark_group("startup_transition");

    // Benchmark adjacent phase transitions
    for phase in [
        StartupPhase::Init,
        StartupPhase::LoadConfig,
        StartupPhase::BindPorts,
    ] {
        group.bench_with_input(
            BenchmarkId::new("can_transition_to", format!("{:?}", phase)),
            &phase,
            |b, &phase| {
                b.iter(|| {
                    let next = match phase {
                        StartupPhase::Init => StartupPhase::LoadConfig,
                        StartupPhase::LoadConfig => StartupPhase::BindPorts,
                        StartupPhase::BindPorts => StartupPhase::AcceptConnections,
                        StartupPhase::AcceptConnections => StartupPhase::Ready,
                        StartupPhase::Ready => StartupPhase::Ready,
                        _ => StartupPhase::Ready,
                    };
                    black_box(phase.can_transition_to(black_box(next)))
                })
            },
        );
    }

    group.finish();
}

fn bench_shutdown_transition(c: &mut Criterion) {
    let mut group = c.benchmark_group("shutdown_transition");

    // Benchmark adjacent phase transitions
    for phase in [
        ShutdownPhase::Running,
        ShutdownPhase::StopAccepting,
        ShutdownPhase::DrainConnections,
        ShutdownPhase::StopModules,
        ShutdownPhase::FlushState,
        ShutdownPhase::ReleaseResources,
    ] {
        group.bench_with_input(
            BenchmarkId::new("can_transition_to", format!("{:?}", phase)),
            &phase,
            |b, &phase| {
                b.iter(|| {
                    let next = match phase {
                        ShutdownPhase::Running => ShutdownPhase::StopAccepting,
                        ShutdownPhase::StopAccepting => ShutdownPhase::DrainConnections,
                        ShutdownPhase::DrainConnections => ShutdownPhase::StopModules,
                        ShutdownPhase::StopModules => ShutdownPhase::FlushState,
                        ShutdownPhase::FlushState => ShutdownPhase::ReleaseResources,
                        ShutdownPhase::ReleaseResources => ShutdownPhase::Complete,
                        ShutdownPhase::Complete => ShutdownPhase::Complete,
                        _ => ShutdownPhase::Complete,
                    };
                    black_box(phase.can_transition_to(black_box(next)))
                })
            },
        );
    }

    group.finish();
}

fn bench_phase_order(c: &mut Criterion) {
    c.bench_function("startup_phase_order", |b| {
        b.iter(|| {
            let phase = black_box(StartupPhase::BindPorts);
            black_box(phase.order())
        })
    });

    c.bench_function("shutdown_phase_order", |b| {
        b.iter(|| {
            let phase = black_box(ShutdownPhase::DrainConnections);
            black_box(phase.order())
        })
    });
}

criterion_group!(
    benches,
    bench_startup_transition,
    bench_shutdown_transition,
    bench_phase_order
);
criterion_main!(benches);

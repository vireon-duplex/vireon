//! Benchmark for connection draining checks.
//!
//! Measures the cost of checking drain status and performing drain steps.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use graceful_startup_shutdown::{ConnectionDrainer, GracefulConfig};

fn bench_drainer_creation(c: &mut Criterion) {
    c.bench_function("drainer_new", |b| {
        let cfg = GracefulConfig::new();
        b.iter(|| black_box(ConnectionDrainer::new(black_box(cfg))))
    });
}

fn bench_is_drained(c: &mut Criterion) {
    let drainer = ConnectionDrainer::new(GracefulConfig::new());
    c.bench_function("is_drained", |b| b.iter(|| black_box(drainer.is_drained())));
}

fn bench_active_connections(c: &mut Criterion) {
    let drainer = ConnectionDrainer::new(GracefulConfig::new());
    c.bench_function("active_connections", |b| {
        b.iter(|| black_box(drainer.active_connections()))
    });
}

criterion_group!(
    benches,
    bench_drainer_creation,
    bench_is_drained,
    bench_active_connections
);
criterion_main!(benches);

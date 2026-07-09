//! Benchmark for snapshot operations.
//!
//! Measures the throughput of health report derivation.

#![allow(clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use observability_sdk::{HealthReport, StatsSnapshot};

fn bench_health_report_derive(c: &mut Criterion) {
    let stats = StatsSnapshot::default();

    c.bench_function("health_report_derive", |b| {
        b.iter(|| {
            let report = HealthReport::derive(black_box(&stats));
            black_box(report);
        });
    });
}

fn bench_snapshot_export_failure_rate(c: &mut Criterion) {
    let mut snap = StatsSnapshot::default();
    snap.exports_total = 1000;
    snap.exports_failed = 50;

    c.bench_function("snapshot_export_failure_rate", |b| {
        b.iter(|| {
            let rate = snap.export_failure_rate_bps();
            black_box(rate);
        });
    });
}

criterion_group!(
    benches,
    bench_health_report_derive,
    bench_snapshot_export_failure_rate
);
criterion_main!(benches);

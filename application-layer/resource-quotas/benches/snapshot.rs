//! Benchmark for stats and health snapshot operations.
//!
//! Measures the throughput of taking stats snapshots and deriving health.

#![allow(clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use resource_quotas::{HealthReport, ResourceQuotaStats, StatsSnapshot};

fn bench_stats_snapshot(c: &mut Criterion) {
    let stats = ResourceQuotaStats::new();
    // Prime the stats with some data.
    stats.record_check();
    stats.record_check_passed();

    c.bench_function("stats_snapshot", |b| {
        b.iter(|| {
            let snap = stats.snapshot();
            black_box(snap);
        });
    });
}

fn bench_health_report_derive(c: &mut Criterion) {
    let stats = StatsSnapshot::default();

    c.bench_function("health_report_derive", |b| {
        b.iter(|| {
            let report = HealthReport::derive(black_box(&stats));
            black_box(report);
        });
    });
}

fn bench_exceed_rate_bps(c: &mut Criterion) {
    let mut snap = StatsSnapshot::default();
    snap.checks_total = 1000;
    snap.checks_exceeded = 50;

    c.bench_function("exceed_rate_bps", |b| {
        b.iter(|| {
            let rate = snap.exceed_rate_bps();
            black_box(rate);
        });
    });
}

criterion_group!(
    benches,
    bench_stats_snapshot,
    bench_health_report_derive,
    bench_exceed_rate_bps
);
criterion_main!(benches);

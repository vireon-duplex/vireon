//! Benchmark for export operations.
//!
//! Measures the throughput of taking stats snapshots.

#![allow(clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use observability_sdk::ObservabilityStats;

fn bench_stats_snapshot(c: &mut Criterion) {
    let stats = ObservabilityStats::new();
    // Prime the stats with some data.
    stats.record_metric();
    stats.record_span_open();
    stats.record_span_close();

    c.bench_function("stats_snapshot", |b| {
        b.iter(|| {
            let snap = stats.snapshot();
            black_box(snap);
        });
    });
}

fn bench_snapshot_active_spans(c: &mut Criterion) {
    let mut snap = observability_sdk::StatsSnapshot::default();
    snap.spans_opened = 1000;
    snap.spans_closed = 800;

    c.bench_function("snapshot_active_spans", |b| {
        b.iter(|| {
            let active = snap.active_spans();
            black_box(active);
        });
    });
}

criterion_group!(benches, bench_stats_snapshot, bench_snapshot_active_spans);
criterion_main!(benches);

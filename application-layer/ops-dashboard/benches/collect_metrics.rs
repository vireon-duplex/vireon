#![allow(missing_docs)]

//! Benchmark for collecting metrics.
//!
//! Measures the throughput of metric collection operations.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use ops_dashboard::{
    MetricsCollector, OpsDashboardConfig, OpsDashboardStats, QueryAggregation, QueryRange,
};

fn bench_stats_record_query(c: &mut Criterion) {
    let stats = OpsDashboardStats::new();
    c.bench_function("stats_record_query", |b| {
        b.iter(|| {
            stats.record_query();
        });
    });
}

fn bench_stats_snapshot(c: &mut Criterion) {
    let stats = OpsDashboardStats::new();
    stats.record_query();
    stats.record_query_success();
    c.bench_function("stats_snapshot", |b| {
        b.iter(|| {
            black_box(stats.snapshot());
        });
    });
}

fn bench_collector_collect(c: &mut Criterion) {
    let cfg = OpsDashboardConfig::new();
    let stats = OpsDashboardStats::new();
    let collector = MetricsCollector::new(cfg, stats);

    c.bench_function("collector_collect", |b| {
        b.iter(|| {
            let _ = collector.collect();
        });
    });
}

fn bench_collector_query_range(c: &mut Criterion) {
    let cfg = OpsDashboardConfig::new();
    let stats = OpsDashboardStats::new();
    let collector = MetricsCollector::new(cfg, stats);
    let range = QueryRange::new(1_000, 5_000);

    c.bench_function("collector_query_range", |b| {
        b.iter(|| {
            let _ = collector.query_range(black_box(range), black_box(QueryAggregation::Avg));
        });
    });
}

criterion_group!(
    benches,
    bench_stats_record_query,
    bench_stats_snapshot,
    bench_collector_collect,
    bench_collector_query_range
);
criterion_main!(benches);

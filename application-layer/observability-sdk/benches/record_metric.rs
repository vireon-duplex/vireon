//! Benchmark for recording metrics.
//!
//! Measures the throughput of metric recording operations.

#![allow(clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use observability_sdk::{MetricKey, MetricValue, ObservabilityStats};

fn bench_metric_key_new(c: &mut Criterion) {
    c.bench_function("metric_key_new", |b| {
        b.iter(|| {
            let key = MetricKey::new(black_box("request_count")).unwrap();
            black_box(key);
        });
    });
}

fn bench_metric_value_counter(c: &mut Criterion) {
    c.bench_function("metric_value_counter", |b| {
        b.iter(|| {
            let val = MetricValue::Counter(black_box(42));
            black_box(val);
        });
    });
}

fn bench_stats_record_metric(c: &mut Criterion) {
    let stats = ObservabilityStats::new();
    c.bench_function("stats_record_metric", |b| {
        b.iter(|| {
            stats.record_metric();
        });
    });
}

criterion_group!(
    benches,
    bench_metric_key_new,
    bench_metric_value_counter,
    bench_stats_record_metric
);
criterion_main!(benches);

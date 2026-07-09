//! Benchmark for span operations.
//!
//! Measures the throughput of span opening and closing operations.

#![allow(clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use observability_sdk::{ObservabilityStats, SpanId};

fn bench_span_id_new(c: &mut Criterion) {
    c.bench_function("span_id_new", |b| {
        b.iter(|| {
            let id = SpanId::new(black_box(12345));
            black_box(id);
        });
    });
}

fn bench_stats_record_span_open(c: &mut Criterion) {
    let stats = ObservabilityStats::new();
    c.bench_function("stats_record_span_open", |b| {
        b.iter(|| {
            stats.record_span_open();
        });
    });
}

fn bench_stats_record_span_close(c: &mut Criterion) {
    let stats = ObservabilityStats::new();
    c.bench_function("stats_record_span_close", |b| {
        b.iter(|| {
            stats.record_span_close();
        });
    });
}

criterion_group!(
    benches,
    bench_span_id_new,
    bench_stats_record_span_open,
    bench_stats_record_span_close
);
criterion_main!(benches);

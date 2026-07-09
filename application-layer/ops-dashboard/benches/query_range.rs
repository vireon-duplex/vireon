#![allow(missing_docs)]

//! Benchmark for query range operations.
//!
//! Measures the throughput of query range construction and validation.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use ops_dashboard::{QueryAggregation, QueryRange};

fn bench_query_range_new(c: &mut Criterion) {
    c.bench_function("query_range_new", |b| {
        b.iter(|| {
            let r = QueryRange::new(black_box(1_000), black_box(5_000));
            black_box(r);
        });
    });
}

fn bench_query_range_duration(c: &mut Criterion) {
    let r = QueryRange::new(1_000, 5_000);
    c.bench_function("query_range_duration", |b| {
        b.iter(|| {
            black_box(r.duration_ms());
        });
    });
}

fn bench_query_range_is_valid(c: &mut Criterion) {
    let r = QueryRange::new(1_000, 5_000);
    c.bench_function("query_range_is_valid", |b| {
        b.iter(|| {
            black_box(r.is_valid());
        });
    });
}

fn bench_aggregation_as_str(c: &mut Criterion) {
    c.bench_function("aggregation_as_str", |b| {
        b.iter(|| {
            black_box(QueryAggregation::P95.as_str());
        });
    });
}

criterion_group!(
    benches,
    bench_query_range_new,
    bench_query_range_duration,
    bench_query_range_is_valid,
    bench_aggregation_as_str
);
criterion_main!(benches);

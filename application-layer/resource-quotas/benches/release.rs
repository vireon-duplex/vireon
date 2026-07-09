//! Benchmark for resource release operations.
//!
//! Measures the throughput of releasing resources and resetting counters.

#![allow(clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use resource_quotas::QuotaUsage;

fn bench_reset_request_counter(c: &mut Criterion) {
    let usage = QuotaUsage::new();

    c.bench_function("reset_request_counter", |b| {
        b.iter(|| {
            usage.reset_request_counter();
        });
    });
}

fn bench_usage_snapshot(c: &mut Criterion) {
    let usage = QuotaUsage::new();

    c.bench_function("usage_snapshot", |b| {
        b.iter(|| {
            let snap = usage.snapshot();
            black_box(snap);
        });
    });
}

criterion_group!(benches, bench_reset_request_counter, bench_usage_snapshot);
criterion_main!(benches);

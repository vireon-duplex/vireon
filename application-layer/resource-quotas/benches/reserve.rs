//! Benchmark for resource reservation operations.
//!
//! Measures the throughput of recording resource usage.

#![allow(clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use resource_quotas::QuotaUsage;

fn bench_record_session_add(c: &mut Criterion) {
    let usage = QuotaUsage::new();

    c.bench_function("record_session_add", |b| {
        b.iter(|| {
            usage.record_session_add();
        });
    });
}

fn bench_record_session_remove(c: &mut Criterion) {
    let usage = QuotaUsage::new();

    c.bench_function("record_session_remove", |b| {
        b.iter(|| {
            usage.record_session_remove();
        });
    });
}

fn bench_record_bandwidth(c: &mut Criterion) {
    let usage = QuotaUsage::new();

    c.bench_function("record_bandwidth", |b| {
        b.iter(|| {
            usage.record_bandwidth(black_box(1_000_000));
        });
    });
}

criterion_group!(
    benches,
    bench_record_session_add,
    bench_record_session_remove,
    bench_record_bandwidth
);
criterion_main!(benches);

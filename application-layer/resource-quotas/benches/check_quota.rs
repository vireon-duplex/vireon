//! Benchmark for quota checking operations.
//!
//! Measures the throughput of quota limit checking.

#![allow(clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use resource_quotas::{QuotaLimit, QuotaUsage, TenantTier};

fn bench_quota_limit_for_tier(c: &mut Criterion) {
    c.bench_function("quota_limit_for_tier", |b| {
        b.iter(|| {
            let limit = QuotaLimit::for_tier(black_box(TenantTier::Pro));
            black_box(limit);
        });
    });
}

fn bench_quota_usage_new(c: &mut Criterion) {
    c.bench_function("quota_usage_new", |b| {
        b.iter(|| {
            let usage = QuotaUsage::new();
            black_box(usage);
        });
    });
}

fn bench_quota_check_against(c: &mut Criterion) {
    let usage = QuotaUsage::new();
    let limit = QuotaLimit::for_tier(TenantTier::Pro);

    c.bench_function("quota_check_against", |b| {
        b.iter(|| {
            let result = usage.check_against(black_box(&limit));
            black_box(result);
        });
    });
}

criterion_group!(
    benches,
    bench_quota_limit_for_tier,
    bench_quota_usage_new,
    bench_quota_check_against
);
criterion_main!(benches);

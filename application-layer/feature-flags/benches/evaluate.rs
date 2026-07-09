//! Benchmark for flag evaluation.
//!
//! Measures the performance of evaluating feature flags with
//! various rule types and contexts.

#![allow(missing_docs)]

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use feature_flags::{EvaluationContext, FlagKey, FlagRule, FlagValue, RuleCondition};

fn bench_evaluate_always(c: &mut Criterion) {
    let key = FlagKey::new("always_flag").unwrap();
    let rule = FlagRule::new(key, RuleCondition::Always, FlagValue::bool(true));
    let ctx = EvaluationContext::minimal(1, 100);

    c.bench_function("evaluate_always", |b| {
        b.iter(|| {
            let _ = black_box(rule.matches(black_box(ctx)));
        });
    });
}

fn bench_evaluate_percentage(c: &mut Criterion) {
    let key = FlagKey::new("percentage_flag").unwrap();
    let percentages = [0, 10, 25, 50, 75, 90, 100];
    let ctx = EvaluationContext::minimal(42, 1001);

    let mut group = c.benchmark_group("evaluate_percentage");
    for pct in percentages {
        let rule = FlagRule::new(key, RuleCondition::Percentage(pct), FlagValue::bool(true));
        group.bench_with_input(BenchmarkId::from_parameter(pct), &pct, |b, _| {
            b.iter(|| {
                let _ = black_box(rule.matches(black_box(ctx)));
            });
        });
    }
    group.finish();
}

fn bench_evaluate_tenant_in(c: &mut Criterion) {
    let key = FlagKey::new("tenant_flag").unwrap();
    let mut tenants = [0u64; 8];
    tenants[0] = 42;
    tenants[1] = 99;
    tenants[2] = 200;
    let rule = FlagRule::new(
        key,
        RuleCondition::TenantIn(tenants, 3),
        FlagValue::bool(true),
    );
    let ctx = EvaluationContext::minimal(42, 100);

    c.bench_function("evaluate_tenant_in_match", |b| {
        b.iter(|| {
            let _ = black_box(rule.matches(black_box(ctx)));
        });
    });

    let ctx_miss = EvaluationContext::minimal(999, 100);
    c.bench_function("evaluate_tenant_in_miss", |b| {
        b.iter(|| {
            let _ = black_box(rule.matches(black_box(ctx_miss)));
        });
    });
}

fn bench_evaluate_never(c: &mut Criterion) {
    let key = FlagKey::new("never_flag").unwrap();
    let rule = FlagRule::new(key, RuleCondition::Never, FlagValue::bool(false));
    let ctx = EvaluationContext::minimal(1, 100);

    c.bench_function("evaluate_never", |b| {
        b.iter(|| {
            let _ = black_box(rule.matches(black_box(ctx)));
        });
    });
}

criterion_group!(
    benches,
    bench_evaluate_always,
    bench_evaluate_percentage,
    bench_evaluate_tenant_in,
    bench_evaluate_never,
);
criterion_main!(benches);

//! Benchmark for rule matching performance.
//!
//! Measures the performance of matching rules against different
//! contexts and conditions.

#![allow(missing_docs)]

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use feature_flags::{EvaluationContext, FlagKey, FlagRule, FlagValue, RuleCondition};

fn bench_rule_match_percentage(c: &mut Criterion) {
    let key = FlagKey::new("rollout").unwrap();
    let percentages = [0, 5, 10, 25, 50, 75, 95, 100];
    let ctx = EvaluationContext::minimal(12345, 67890);

    let mut group = c.benchmark_group("rule_match_percentage");
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

fn bench_rule_match_tenant_in(c: &mut Criterion) {
    let key = FlagKey::new("tenant_feature").unwrap();
    let tenant_counts = [1, 2, 4, 8];

    let mut group = c.benchmark_group("rule_match_tenant_in");
    for count in tenant_counts {
        let mut tenants = [0u64; 8];
        for i in 0..count {
            tenants[i as usize] = i as u64 * 100 + 42;
        }
        let rule = FlagRule::new(
            key,
            RuleCondition::TenantIn(tenants, count),
            FlagValue::bool(true),
        );

        // Test hit case (tenant is in the list)
        let ctx_hit = EvaluationContext::minimal(42, 100);
        group.bench_with_input(BenchmarkId::new("hit", count), &count, |b, _| {
            b.iter(|| {
                let _ = black_box(rule.matches(black_box(ctx_hit)));
            });
        });

        // Test miss case (tenant is not in the list)
        let ctx_miss = EvaluationContext::minimal(999, 100);
        group.bench_with_input(BenchmarkId::new("miss", count), &count, |b, _| {
            b.iter(|| {
                let _ = black_box(rule.matches(black_box(ctx_miss)));
            });
        });
    }
    group.finish();
}

fn bench_rule_match_different_users(c: &mut Criterion) {
    let key = FlagKey::new("user_rollout").unwrap();
    let rule = FlagRule::new(key, RuleCondition::Percentage(50), FlagValue::bool(true));
    let users = [1, 100, 1000, 10000, 100000];

    let mut group = c.benchmark_group("rule_match_different_users");
    for user_id in users {
        let ctx = EvaluationContext::minimal(42, user_id);
        group.bench_with_input(BenchmarkId::from_parameter(user_id), &user_id, |b, _| {
            b.iter(|| {
                let _ = black_box(rule.matches(black_box(ctx)));
            });
        });
    }
    group.finish();
}

fn bench_rule_match_different_tenants(c: &mut Criterion) {
    let key = FlagKey::new("tenant_rollout").unwrap();
    let rule = FlagRule::new(key, RuleCondition::Percentage(50), FlagValue::bool(true));
    let tenants = [1, 10, 100, 1000, 10000];

    let mut group = c.benchmark_group("rule_match_different_tenants");
    for tenant_id in tenants {
        let ctx = EvaluationContext::minimal(tenant_id, 100);
        group.bench_with_input(
            BenchmarkId::from_parameter(tenant_id),
            &tenant_id,
            |b, _| {
                b.iter(|| {
                    let _ = black_box(rule.matches(black_box(ctx)));
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_rule_match_percentage,
    bench_rule_match_tenant_in,
    bench_rule_match_different_users,
    bench_rule_match_different_tenants,
);
criterion_main!(benches);

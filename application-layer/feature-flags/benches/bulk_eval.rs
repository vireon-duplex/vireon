//! Benchmark for bulk flag evaluation.
//!
//! Measures the performance of evaluating many flags and rules
//! in sequence, simulating real-world usage patterns.

#![allow(missing_docs)]

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use feature_flags::{EvaluationContext, FlagKey, FlagRule, FlagValue, RuleCondition};

fn bench_bulk_eval_same_flag(c: &mut Criterion) {
    let key = FlagKey::new("bulk_flag").unwrap();
    let rule = FlagRule::new(key, RuleCondition::Percentage(50), FlagValue::bool(true));
    let ctx = EvaluationContext::minimal(42, 100);

    let mut group = c.benchmark_group("bulk_eval_same_flag");
    for count in [10, 100, 1000, 10000] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                for _ in 0..count {
                    let _ = black_box(rule.matches(black_box(ctx)));
                }
            });
        });
    }
    group.finish();
}

fn bench_bulk_eval_different_flags(c: &mut Criterion) {
    let ctx = EvaluationContext::minimal(42, 100);

    let mut group = c.benchmark_group("bulk_eval_different_flags");
    for count in [10, 100, 500] {
        let mut rules = Vec::with_capacity(count as usize);
        for i in 0..count {
            let key = FlagKey::new(&format!("flag_{}", i)).unwrap();
            let rule = FlagRule::new(key, RuleCondition::Percentage(50), FlagValue::bool(true));
            rules.push(rule);
        }

        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                for rule in &rules {
                    let _ = black_box(rule.matches(black_box(ctx)));
                }
            });
        });
    }
    group.finish();
}

fn bench_bulk_eval_different_contexts(c: &mut Criterion) {
    let key = FlagKey::new("context_test").unwrap();
    let rule = FlagRule::new(key, RuleCondition::Percentage(50), FlagValue::bool(true));

    let mut group = c.benchmark_group("bulk_eval_different_contexts");
    for count in [10, 100, 1000, 10000] {
        let mut contexts = Vec::with_capacity(count as usize);
        for i in 0..count {
            contexts.push(EvaluationContext::minimal(i, i * 100 + 1));
        }

        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                for &ctx in &contexts {
                    let _ = black_box(rule.matches(black_box(ctx)));
                }
            });
        });
    }
    group.finish();
}

fn bench_bulk_eval_mixed_rules(c: &mut Criterion) {
    let ctx = EvaluationContext::minimal(42, 100);
    let mut rules = Vec::new();

    // Mix of different rule types
    for i in 0..100 {
        let key = FlagKey::new(&format!("mixed_flag_{}", i)).unwrap();
        let condition = match i % 4 {
            0 => RuleCondition::Always,
            1 => RuleCondition::Never,
            2 => RuleCondition::Percentage(25),
            _ => RuleCondition::Percentage(75),
        };
        let rule = FlagRule::new(key, condition, FlagValue::bool(true));
        rules.push(rule);
    }

    c.bench_function("bulk_eval_mixed_rules", |b| {
        b.iter(|| {
            for rule in &rules {
                let _ = black_box(rule.matches(black_box(ctx)));
            }
        });
    });
}

criterion_group!(
    benches,
    bench_bulk_eval_same_flag,
    bench_bulk_eval_different_flags,
    bench_bulk_eval_different_contexts,
    bench_bulk_eval_mixed_rules,
);
criterion_main!(benches);

//! Benchmark for hook dispatch operations.
//!
//! Measures the performance of invoking hooks with different
//! triggers and priorities.

#![allow(missing_docs)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use scheduler_hooks::{HookPriority, HookTrigger, SchedulerHookId};

fn bench_hook_id_new(c: &mut Criterion) {
    c.bench_function("hook_id_new", |b| {
        b.iter(|| {
            let _ = black_box(SchedulerHookId::new(black_box(42)));
        });
    });
}

fn bench_hook_id_is_nil(c: &mut Criterion) {
    let nil_id = SchedulerHookId::nil();
    let valid_id = SchedulerHookId::new(1);

    c.bench_function("hook_id_is_nil_nil", |b| {
        b.iter(|| {
            let _ = black_box(nil_id.is_nil());
        });
    });

    c.bench_function("hook_id_is_nil_valid", |b| {
        b.iter(|| {
            let _ = black_box(valid_id.is_nil());
        });
    });
}

fn bench_hook_trigger_as_str(c: &mut Criterion) {
    let triggers = [
        HookTrigger::PreTick,
        HookTrigger::PostTick,
        HookTrigger::PreSubmit,
        HookTrigger::PostComplete,
        HookTrigger::OnIdle,
        HookTrigger::OnWakeup,
    ];

    let mut group = c.benchmark_group("hook_trigger_as_str");
    for trigger in triggers {
        group.bench_function(trigger.as_str(), |b| {
            b.iter(|| {
                let _ = black_box(trigger.as_str());
            });
        });
    }
    group.finish();
}

fn bench_hook_trigger_phase(c: &mut Criterion) {
    let triggers = [
        HookTrigger::PreTick,
        HookTrigger::PostTick,
        HookTrigger::OnIdle,
    ];

    let mut group = c.benchmark_group("hook_trigger_phase");
    for trigger in triggers {
        group.bench_function(trigger.as_str(), |b| {
            b.iter(|| {
                let _ = black_box(trigger.phase());
            });
        });
    }
    group.finish();
}

fn bench_hook_priority_new(c: &mut Criterion) {
    c.bench_function("hook_priority_new", |b| {
        b.iter(|| {
            let _ = black_box(HookPriority::new(black_box(64)));
        });
    });
}

fn bench_hook_priority_comparison(c: &mut Criterion) {
    let high = HookPriority::HIGH;
    let low = HookPriority::LOW;

    c.bench_function("hook_priority_is_more_important", |b| {
        b.iter(|| {
            let _ = black_box(high.is_more_important_than(black_box(&low)));
        });
    });
}

criterion_group!(
    benches,
    bench_hook_id_new,
    bench_hook_id_is_nil,
    bench_hook_trigger_as_str,
    bench_hook_trigger_phase,
    bench_hook_priority_new,
    bench_hook_priority_comparison,
);
criterion_main!(benches);

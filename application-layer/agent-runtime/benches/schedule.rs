//! Bench: schedule — measure the cost of scheduler operations.

#![allow(missing_docs)]

use agent_runtime::{AgentId, AgentRuntimeConfig, AgentScheduler};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn bench_scheduler_new(c: &mut Criterion) {
    c.bench_function("scheduler_new", |b| {
        b.iter(|| {
            black_box(AgentScheduler::new(black_box(AgentRuntimeConfig::new())));
        });
    });
}

fn bench_scheduler_health(c: &mut Criterion) {
    c.bench_function("scheduler_health", |b| {
        let scheduler = AgentScheduler::new(AgentRuntimeConfig::new());
        b.iter(|| {
            black_box(scheduler.health());
        });
    });
}

fn bench_agent_id_is_nil(c: &mut Criterion) {
    c.bench_function("agent_id_is_nil", |b| {
        b.iter(|| {
            black_box(AgentId::new(black_box(0)).is_nil());
            black_box(AgentId::new(black_box(1)).is_nil());
        });
    });
}

criterion_group!(
    benches,
    bench_scheduler_new,
    bench_scheduler_health,
    bench_agent_id_is_nil
);
criterion_main!(benches);

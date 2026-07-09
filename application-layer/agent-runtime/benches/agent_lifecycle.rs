//! Bench: agent_lifecycle — measure the cost of agent lifecycle operations.

#![allow(missing_docs)]

use agent_runtime::{AgentId, AgentRuntime, AgentRuntimeConfig, AgentState};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn bench_agent_id_new(c: &mut Criterion) {
    c.bench_function("agent_id_new", |b| {
        b.iter(|| {
            black_box(AgentId::new(black_box(42)));
        });
    });
}

fn bench_agent_state_as_str(c: &mut Criterion) {
    c.bench_function("agent_state_as_str", |b| {
        b.iter(|| {
            black_box(AgentState::Idle.as_str());
            black_box(AgentState::Running.as_str());
            black_box(AgentState::Waiting.as_str());
            black_box(AgentState::Stopped.as_str());
            black_box(AgentState::Failed.as_str());
        });
    });
}

fn bench_runtime_health(c: &mut Criterion) {
    c.bench_function("runtime_health", |b| {
        let runtime = AgentRuntime::new(AgentRuntimeConfig::new());
        b.iter(|| {
            black_box(runtime.health());
        });
    });
}

criterion_group!(
    benches,
    bench_agent_id_new,
    bench_agent_state_as_str,
    bench_runtime_health
);
criterion_main!(benches);

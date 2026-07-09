//! Bench: context_switch — measure the cost of context creation and token tracking.

#![allow(missing_docs)]

use agent_runtime::{AgentContext, AgentId};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use session_manager::SessionId;

fn bench_context_new(c: &mut Criterion) {
    c.bench_function("context_new", |b| {
        b.iter(|| {
            black_box(AgentContext::new(
                black_box(AgentId::new(1)),
                black_box(SessionId::from_pair(1, 1)),
            ));
        });
    });
}

fn bench_context_use_tokens(c: &mut Criterion) {
    c.bench_function("context_use_tokens", |b| {
        b.iter(|| {
            let mut ctx = AgentContext::new(AgentId::new(1), SessionId::from_pair(1, 1));
            ctx.use_tokens(black_box(100));
            black_box(ctx.remaining_tokens());
        });
    });
}

fn bench_context_remaining_tokens(c: &mut Criterion) {
    c.bench_function("context_remaining_tokens", |b| {
        let ctx = AgentContext::new(AgentId::new(1), SessionId::from_pair(1, 1));
        b.iter(|| {
            black_box(ctx.remaining_tokens());
        });
    });
}

criterion_group!(
    benches,
    bench_context_new,
    bench_context_use_tokens,
    bench_context_remaining_tokens
);
criterion_main!(benches);

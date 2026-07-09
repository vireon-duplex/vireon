//! Bench: tool_call — measure the cost of creating and inspecting tool calls.

#![allow(missing_docs)]

use ai_streaming::{ToolCall, ToolCallId, ToolStatus};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn bench_tool_call_new(c: &mut Criterion) {
    c.bench_function("tool_call_new", |b| {
        b.iter(|| {
            black_box(ToolCall::new(
                black_box(ToolCallId::new(1)),
                String::from("search"),
                String::from(r#"{"query":"rust"}"#),
            ));
        });
    });
}

fn bench_tool_status_as_str(c: &mut Criterion) {
    c.bench_function("tool_status_as_str", |b| {
        b.iter(|| {
            black_box(ToolStatus::Pending.as_str());
            black_box(ToolStatus::Executing.as_str());
            black_box(ToolStatus::Completed.as_str());
            black_box(ToolStatus::Failed.as_str());
        });
    });
}

fn bench_tool_status_is_terminal(c: &mut Criterion) {
    c.bench_function("tool_status_is_terminal", |b| {
        b.iter(|| {
            black_box(ToolStatus::Pending.is_terminal());
            black_box(ToolStatus::Executing.is_terminal());
            black_box(ToolStatus::Completed.is_terminal());
            black_box(ToolStatus::Failed.is_terminal());
        });
    });
}

criterion_group!(
    benches,
    bench_tool_call_new,
    bench_tool_status_as_str,
    bench_tool_status_is_terminal
);
criterion_main!(benches);

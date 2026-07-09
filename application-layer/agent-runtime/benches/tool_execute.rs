//! Bench: tool_execute — measure the cost of tool creation and result handling.

#![allow(missing_docs)]

use agent_runtime::{ToolDescriptor, ToolId, ToolResult};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn bench_tool_descriptor_new(c: &mut Criterion) {
    c.bench_function("tool_descriptor_new", |b| {
        b.iter(|| {
            black_box(ToolDescriptor::new(
                black_box(ToolId::new(1)),
                black_box("search"),
            ));
        });
    });
}

fn bench_tool_result_success(c: &mut Criterion) {
    c.bench_function("tool_result_success", |b| {
        b.iter(|| {
            black_box(ToolResult::success(
                black_box(ToolId::new(1)),
                String::from("ok"),
            ));
        });
    });
}

fn bench_tool_result_failure(c: &mut Criterion) {
    c.bench_function("tool_result_failure", |b| {
        b.iter(|| {
            black_box(ToolResult::failure(
                black_box(ToolId::new(1)),
                String::from("error"),
            ));
        });
    });
}

criterion_group!(
    benches,
    bench_tool_descriptor_new,
    bench_tool_result_success,
    bench_tool_result_failure
);
criterion_main!(benches);

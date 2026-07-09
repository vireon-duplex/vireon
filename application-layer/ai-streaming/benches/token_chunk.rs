//! Bench: token_chunk — measure the cost of creating and processing token chunks.

#![allow(missing_docs)]

use ai_streaming::{TokenChunk, TokenType};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn bench_token_chunk_new(c: &mut Criterion) {
    c.bench_function("token_chunk_new", |b| {
        b.iter(|| {
            black_box(TokenChunk::new(black_box(1), String::from("hello world")));
        });
    });
}

fn bench_token_chunk_final(c: &mut Criterion) {
    c.bench_function("token_chunk_final", |b| {
        b.iter(|| {
            black_box(TokenChunk::new_final(
                black_box(42),
                String::from("final token"),
            ));
        });
    });
}

fn bench_token_type_as_str(c: &mut Criterion) {
    c.bench_function("token_type_as_str", |b| {
        b.iter(|| {
            black_box(TokenType::Text.as_str());
            black_box(TokenType::ToolCall.as_str());
            black_box(TokenType::EndOfStream.as_str());
            black_box(TokenType::Error.as_str());
        });
    });
}

criterion_group!(
    benches,
    bench_token_chunk_new,
    bench_token_chunk_final,
    bench_token_type_as_str
);
criterion_main!(benches);

//! Bench: aggregate — measure the cost of token aggregation operations.

#![allow(missing_docs)]

use ai_streaming::{AiStreamConfig, AiStreamId, TokenAggregator, TokenChunk};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn bench_aggregate(c: &mut Criterion) {
    let agg = TokenAggregator::new(AiStreamConfig::new());
    let chunk = TokenChunk::new(1, String::from("hello"));
    c.bench_function("aggregate", |b| {
        b.iter(|| {
            let _ =
                black_box(agg.aggregate(black_box(AiStreamId::new(1)), black_box(chunk.clone())));
        });
    });
}

fn bench_flush(c: &mut Criterion) {
    let agg = TokenAggregator::new(AiStreamConfig::new());
    c.bench_function("flush", |b| {
        b.iter(|| {
            let _ = black_box(agg.flush(black_box(AiStreamId::new(1))));
        });
    });
}

criterion_group!(benches, bench_aggregate, bench_flush);
criterion_main!(benches);

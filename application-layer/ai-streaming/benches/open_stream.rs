//! Bench: open_stream — measure the cost of opening and closing AI streams.

#![allow(missing_docs)]

use ai_streaming::{AiStreamConfig, AiStreamPipeline, StreamMode};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn bench_open_stream(c: &mut Criterion) {
    let pipeline = AiStreamPipeline::new(AiStreamConfig::new());
    c.bench_function("open_stream", |b| {
        b.iter(|| {
            let _ = black_box(pipeline.open_stream(black_box(StreamMode::Text)));
        });
    });
}

fn bench_close_stream(c: &mut Criterion) {
    let pipeline = AiStreamPipeline::new(AiStreamConfig::new());
    c.bench_function("close_stream", |b| {
        b.iter(|| {
            let _ = black_box(pipeline.close_stream(black_box(ai_streaming::AiStreamId::new(1))));
        });
    });
}

criterion_group!(benches, bench_open_stream, bench_close_stream);
criterion_main!(benches);

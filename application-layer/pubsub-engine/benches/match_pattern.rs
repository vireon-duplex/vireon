//! Bench: match_pattern — measure the cost of topic pattern matching.

#![allow(missing_docs)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use pubsub_engine::TopicPattern;

fn bench_match_pattern(c: &mut Criterion) {
    let pattern = TopicPattern::new("sensor.*.high").unwrap();
    c.bench_function("match_pattern", |b| {
        b.iter(|| {
            black_box(pattern.matches(black_box("sensor.temp.high")));
        });
    });
}

criterion_group!(benches, bench_match_pattern);
criterion_main!(benches);

//! Bench: match_pattern — measure the cost of topic pattern matching.

#![allow(missing_docs, clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use pubsub_engine::{Topic, TopicPattern};

fn bench_match_pattern(c: &mut Criterion) {
    let pattern = TopicPattern::new("sensor.*.high").unwrap();
    let topic = Topic::new("sensor.temp.high").unwrap();
    c.bench_function("match_pattern", |b| {
        b.iter(|| {
            black_box(pattern.matches(black_box(&topic)));
        });
    });
}

criterion_group!(benches, bench_match_pattern);
criterion_main!(benches);

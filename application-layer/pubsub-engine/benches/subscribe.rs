//! Bench: subscribe — measure the cost of creating a subscription.

#![allow(missing_docs, clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use pubsub_engine::{PubSubConfig, PubSubEngine, TopicPattern};

fn bench_subscribe(c: &mut Criterion) {
    let mut engine = PubSubEngine::new(PubSubConfig::new());
    let pattern = TopicPattern::new("sensor.*").unwrap();
    c.bench_function("subscribe", |b| {
        b.iter(|| {
            let _ = black_box(engine.subscribe(black_box(pattern.clone())));
        });
    });
}

criterion_group!(benches, bench_subscribe);
criterion_main!(benches);

//! Bench: deliver — measure the cost of delivering a message to a
//! single matching subscription.
//!
//! This isolates per-delivery overhead (pattern match + stat increment)
//! from subscription-fan-out scaling, which is covered by `fanout.rs`.

#![allow(missing_docs, clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use pubsub_engine::{PubSubConfig, PubSubEngine, PubSubMessage, Topic, TopicPattern};

fn bench_deliver(c: &mut Criterion) {
    let mut engine = PubSubEngine::new(PubSubConfig::new());
    engine
        .subscribe(TopicPattern::new("sensor.*").unwrap())
        .unwrap();
    let msg = PubSubMessage::new(Topic::new("sensor.temp").unwrap(), vec![1, 2, 3]);
    c.bench_function("deliver", |b| {
        b.iter(|| {
            let _ = black_box(engine.publish(black_box(msg.clone())));
        });
    });
}

criterion_group!(benches, bench_deliver);
criterion_main!(benches);

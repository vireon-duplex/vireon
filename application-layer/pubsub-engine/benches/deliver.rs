//! Bench: deliver — measure the cost of delivering a message to a subscription.

#![allow(missing_docs)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use pubsub_engine::{PubSubConfig, PubSubEngine, PubSubMessage};

fn bench_deliver(c: &mut Criterion) {
    let engine = PubSubEngine::new(PubSubConfig::new());
    let msg = PubSubMessage::new("sensor.temp", vec![1, 2, 3]);
    c.bench_function("deliver", |b| {
        b.iter(|| {
            let _ = black_box(engine.publish(black_box(msg.clone())));
        });
    });
}

criterion_group!(benches, bench_deliver);
criterion_main!(benches);

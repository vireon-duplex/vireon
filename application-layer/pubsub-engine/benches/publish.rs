//! Bench: publish — measure the cost of publishing a message to the engine.

#![allow(missing_docs)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use pubsub_engine::{PubSubConfig, PubSubEngine, PubSubMessage};

fn bench_publish(c: &mut Criterion) {
    let mut engine = PubSubEngine::new(PubSubConfig::new());
    let msg = PubSubMessage::new("sensor.temp", vec![1, 2, 3, 4]);
    c.bench_function("publish", |b| {
        b.iter(|| {
            let _ = black_box(engine.publish(black_box(msg.clone())));
        });
    });
}

criterion_group!(benches, bench_publish);
criterion_main!(benches);

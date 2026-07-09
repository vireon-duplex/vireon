//! Bench: consume — measure the cost of subscribing a consumer group to a topic.

#![allow(missing_docs)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use event_bus::{ConsumerGroupId, EventBus, EventBusConfig, SubscriptionMode, TopicId};

fn bench_consume(c: &mut Criterion) {
    let bus = EventBus::new(EventBusConfig::new());
    c.bench_function("consume", |b| {
        b.iter(|| {
            let _ = black_box(bus.subscribe(
                black_box(ConsumerGroupId::new(1)),
                black_box(TopicId::new(1)),
                black_box(SubscriptionMode::Earliest),
            ));
        });
    });
}

criterion_group!(benches, bench_consume);
criterion_main!(benches);

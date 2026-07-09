//! Bench: publish — measure the cost of publishing an event to a topic partition.

#![allow(missing_docs)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use event_bus::{EventBus, EventBusConfig, PartitionId, TopicId};

fn bench_publish(c: &mut Criterion) {
    let bus = EventBus::new(EventBusConfig::new());
    c.bench_function("publish", |b| {
        b.iter(|| {
            let _ = black_box(bus.publish(
                black_box(TopicId::new(1)),
                black_box(PartitionId::new(0)),
                black_box(42),
            ));
        });
    });
}

criterion_group!(benches, bench_publish);
criterion_main!(benches);

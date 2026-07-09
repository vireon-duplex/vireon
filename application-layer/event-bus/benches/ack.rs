//! Bench: ack — measure the cost of acknowledging consumption of a message.

#![allow(missing_docs)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use event_bus::{ConsumerGroupId, EventBus, EventBusConfig, Offset, PartitionId};

fn bench_ack(c: &mut Criterion) {
    let bus = EventBus::new(EventBusConfig::new());
    c.bench_function("ack", |b| {
        b.iter(|| {
            let _ = black_box(bus.ack(
                black_box(ConsumerGroupId::new(1)),
                black_box(PartitionId::new(0)),
                black_box(Offset::new(0)),
            ));
        });
    });
}

criterion_group!(benches, bench_ack);
criterion_main!(benches);

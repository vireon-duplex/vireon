//! Bench: rebalance — measure the cost of planning a partition rebalance.

#![allow(missing_docs)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use event_bus::{ConsumerGroupId, EventBusConfig, PartitionId, PartitionRebalancer};

fn bench_rebalance(c: &mut Criterion) {
    let rebalancer = PartitionRebalancer::new(EventBusConfig::new());
    let partitions: Vec<PartitionId> = (0..8).map(PartitionId::new).collect();
    c.bench_function("rebalance", |b| {
        b.iter(|| {
            let _ = black_box(
                rebalancer.plan(black_box(ConsumerGroupId::new(1)), black_box(&partitions)),
            );
        });
    });
}

criterion_group!(benches, bench_rebalance);
criterion_main!(benches);

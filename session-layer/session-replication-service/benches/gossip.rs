//! Benchmark stubs for the gossip protocol hot path.
//!
//! Phase 0: stub only. Phase 1+ adds real gossip benchmarks.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use session_replication_service::GossipEngine;

fn bench_gossip_broadcast(c: &mut Criterion) {
    let mut group = c.benchmark_group("gossip");

    group.bench_function("broadcast", |b| {
        let engine = GossipEngine::new(1_000);
        b.iter(|| {
            black_box(
                engine
                    .broadcast(black_box("key"), black_box(b"value"))
                    .is_ok(),
            )
        });
    });

    group.finish();
}

criterion_group!(benches, bench_gossip_broadcast);
criterion_main!(benches);

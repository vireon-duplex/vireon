//! Benchmark stubs for the replication hot path.
//!
//! Phase 0: stub only. Phase 1+ adds real replication benchmarks.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use session_replication_service::{ReplicationConfig, SyncEngine};

fn bench_sync_replicate(c: &mut Criterion) {
    let mut group = c.benchmark_group("replicate");

    group.bench_function("sync_replicate_3", |b| {
        let mut engine = SyncEngine::new(3);
        b.iter(|| {
            black_box(
                engine
                    .replicate(black_box("key"), black_box(b"value"))
                    .is_ok(),
            )
        });
    });

    group.finish();
}

criterion_group!(benches, bench_sync_replicate);
criterion_main!(benches);

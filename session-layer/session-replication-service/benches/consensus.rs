//! Benchmark stubs for the consensus (Raft) hot path.
//!
//! Phase 0: stub only. Phase 2+ adds real consensus benchmarks.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use session_replication_service::RaftEngine;

fn bench_raft_propose(c: &mut Criterion) {
    let mut group = c.benchmark_group("consensus");

    group.bench_function("raft_propose", |b| {
        let mut engine = RaftEngine::new(1_500);
        b.iter(|| {
            black_box(
                engine
                    .propose(black_box("key"), black_box(b"value"))
                    .is_ok(),
            )
        });
    });

    group.finish();
}

criterion_group!(benches, bench_raft_propose);
criterion_main!(benches);

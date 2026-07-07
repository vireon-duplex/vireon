//! Benchmark stubs for conflict detection and resolution.
//!
//! Phase 0: stub only. Phase 1+ adds real conflict benchmarks.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use session_replication_service::ConflictResolver;

fn bench_conflict_resolve(c: &mut Criterion) {
    let mut group = c.benchmark_group("conflict");

    group.bench_function("resolve_lww", |b| {
        let mut resolver = ConflictResolver::new();
        let v1 = b"value1";
        let v2 = b"value2";
        let conflicting: [(&[u8], &[u8]); 1] = [(&v1[..], &v2[..])];
        b.iter(|| {
            black_box(
                resolver
                    .resolve(black_box("key"), black_box(&conflicting))
                    .is_ok(),
            )
        });
    });

    group.finish();
}

criterion_group!(benches, bench_conflict_resolve);
criterion_main!(benches);

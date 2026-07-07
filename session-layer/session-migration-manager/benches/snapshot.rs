//! `snapshot` bench entry point — `SnapshotMetadata` validation.

use criterion::{Criterion, criterion_group, criterion_main};
use session_manager::SessionId;
use session_migration_manager::{Generation, NodeId, SnapshotMetadata};

fn bench_snapshot_validation(_c: &mut Criterion) {
    // TODO(phase-1): snapshot metadata construction + validation.
    let _meta = SnapshotMetadata::new(
        SessionId::from_pair(1, 1),
        NodeId::new(1),
        0,
        4_096,
        3,
        Generation::INITIAL,
        0xABCD,
    );
}

criterion_group!(benches, bench_snapshot_validation);
criterion_main!(benches);

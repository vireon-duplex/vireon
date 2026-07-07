//! `coordinator` bench entry point — `MigrationCoordinator` lifecycle.

use criterion::{Criterion, criterion_group, criterion_main};
use session_migration_manager::MigrationCoordinator;

fn bench_coordinator_lifecycle(_c: &mut Criterion) {
    // TODO(phase-1): full coordinator initiate → finalize cycle.
    let _coord = MigrationCoordinator::new();
}

criterion_group!(benches, bench_coordinator_lifecycle);
criterion_main!(benches);

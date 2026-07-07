//! `phase` bench entry point — `MigrationPhase` transition checks.

use criterion::{Criterion, criterion_group, criterion_main};
use session_migration_manager::MigrationPhase;

fn bench_phase_transitions(_c: &mut Criterion) {
    // TODO(phase-1): phase transition validation throughput.
    let _ = MigrationPhase::Initiated.can_transition_to(MigrationPhase::SnapshotCaptured);
}

criterion_group!(benches, bench_phase_transitions);
criterion_main!(benches);

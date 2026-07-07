//! `plan` bench entry point — `MigrationPlan` construction and checks.

use criterion::{Criterion, criterion_group, criterion_main};
use session_manager::SessionId;
use session_migration_manager::{Generation, MigrationFlags, MigrationPlan, NodeId};

fn bench_plan_construct_and_check(_c: &mut Criterion) {
    // TODO(phase-1): realistic plan construction + deadline check.
    let _plan = MigrationPlan::new(
        SessionId::from_pair(1, 1),
        NodeId::new(1),
        NodeId::new(2),
        10_000_000_000,
        0,
        MigrationFlags::default(),
        Generation::INITIAL,
    );
}

criterion_group!(benches, bench_plan_construct_and_check);
criterion_main!(benches);

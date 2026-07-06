//! `fsm` bench entry point.
//!
//! Measures per-half FSM transitions (`Idle → Ready → Send → …`)
//! and combined-state derivation. Bodies land in Phase 1 alongside
//! the lifecycle controller.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_fsm(_c: &mut Criterion) {
    // TODO(phase-1): state-transition micro-benches.
}

criterion_group!(benches, bench_fsm);
criterion_main!(benches);

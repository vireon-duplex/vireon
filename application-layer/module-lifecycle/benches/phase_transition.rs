//! `phase_transition` bench entry point — phase transition cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_phase_transition(_c: &mut Criterion) {
    // TODO(phase-1): ModulePhase::can_transition_to cost.
}

criterion_group!(benches, bench_phase_transition);
criterion_main!(benches);

//! Bench: event_loop — one tick round-trip (drain + flush + tick).
//!
//! See `DESIGN.md` §3 (event loop ordering) and §15 (performance).
//!
//! Phase 0 ships only the Criterion harness; bench bodies land in
//! Phase 1 once `EventLoop::run_one_tick` is real.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_event_loop(c: &mut Criterion) {
    let g = c.benchmark_group("event_loop");
    // TODO(phase-1): per-tick latency under various CQE / timer /
    //   task mixes. Compare preset configs.
    g.finish();
}

criterion_group!(benches_, bench_event_loop);
criterion_main!(benches_);

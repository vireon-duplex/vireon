//! Bench: pin — `sched_setaffinity` overhead.
//!
//! See `DESIGN.md` §5 (connection ownership + NUMA) and §16
//! (scalability).
//!
//! Phase 0 ships only the Criterion harness; the body can land at
//! any later phase since [`reactor::CpuPinning::apply`] is already
//! real.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_pin(c: &mut Criterion) {
    let g = c.benchmark_group("pin");
    // TODO: measure `CpuPinning::apply()` cold vs warm. Single-shot
    //   cost is what matters at reactor-start; per-tick cost is
    //   zero (apply is called once at start).
    g.finish();
}

criterion_group!(benches_, bench_pin);
criterion_main!(benches_);

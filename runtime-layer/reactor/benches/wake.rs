//! Bench: wake — eventfd cross-thread wake latency.
//!
//! See `DESIGN.md` §3 (event loop) and §13 (io_uring integration).
//!
//! Phase 0 ships only the Criterion harness; bench bodies land in
//! Phase 3 once `EventfdWake` is real.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_wake(c: &mut Criterion) {
    let g = c.benchmark_group("wake");
    // TODO(phase-3): wake-to-CQE latency under load. Compare
    //   against `std::thread::yield_now` baseline.
    g.finish();
}

criterion_group!(benches_, bench_wake);
criterion_main!(benches_);

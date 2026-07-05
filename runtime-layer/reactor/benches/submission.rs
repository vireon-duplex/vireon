//! Bench: submission — SQE batch submission throughput.
//!
//! See `DESIGN.md` §13 (io_uring integration) and §7 (send-queue
//! integration).
//!
//! Phase 0 ships only the Criterion harness; bench bodies land in
//! Phase 2 once `RingOwner` and `RingSubmissionSink` are real.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_submission(c: &mut Criterion) {
    let g = c.benchmark_group("submission");
    // TODO(phase-2): SEND_ZC SQE push rate at batch sizes
    //   1 / 16 / 64 / 256. Compare with vs without SQPOLL.
    g.finish();
}

criterion_group!(benches_, bench_submission);
criterion_main!(benches_);

//! `queue` bench entry point.
//!
//! Measures `ReadyQueue` enqueue / dequeue cost across the 8
//! urgency buckets. Bodies land in Phase 1.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_queue(_c: &mut Criterion) {
    // TODO(phase-1): enqueue / dequeue / peek per urgency band.
}

criterion_group!(benches, bench_queue);
criterion_main!(benches);

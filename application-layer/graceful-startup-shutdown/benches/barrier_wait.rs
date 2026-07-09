//! Benchmark for phase barrier operations.
//!
//! Measures the cost of barrier creation, progress tracking, and completion.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use graceful_startup_shutdown::PhaseBarrier;

fn bench_barrier_creation(c: &mut Criterion) {
    c.bench_function("barrier_new", |b| {
        b.iter(|| black_box(PhaseBarrier::new(black_box(0), black_box(1_000_000))))
    });
}

fn bench_mark_progress(c: &mut Criterion) {
    c.bench_function("mark_progress", |b| {
        let mut barrier = PhaseBarrier::new(0, 1_000_000);
        b.iter(|| {
            barrier.mark_progress();
            black_box(barrier.completions())
        })
    });
}

fn bench_mark_complete(c: &mut Criterion) {
    c.bench_function("mark_complete", |b| {
        let mut barrier = PhaseBarrier::new(0, 1_000_000);
        b.iter(|| {
            barrier.mark_complete(black_box(2_500_000));
            black_box(barrier.is_done())
        })
    });
}

fn bench_is_done(c: &mut Criterion) {
    let barrier = PhaseBarrier::new(0, 1_000_000);
    c.bench_function("is_done", |b| b.iter(|| black_box(barrier.is_done())));
}

fn bench_duration_ns(c: &mut Criterion) {
    let barrier = PhaseBarrier::new(0, 1_000_000);
    c.bench_function("duration_ns", |b| {
        b.iter(|| black_box(barrier.duration_ns(black_box(2_500_000))))
    });
}

criterion_group!(
    benches,
    bench_barrier_creation,
    bench_mark_progress,
    bench_mark_complete,
    bench_is_done,
    bench_duration_ns
);
criterion_main!(benches);

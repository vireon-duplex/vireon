//! Benchmark: end-to-end packet lifecycle.
//!
//! Measures the complete steady-state cycle for one or more packets:
//!
//! ```text
//! submit → flush → on_send_cqe → on_reclaim_cqe
//! ```
//!
//! This is the realistic per-packet cost the reactor pays. The
//! [`NullSink`](common::NullSink) stands in for the IO Layer so the
//! measurement reflects only queue machinery. Uses `iter_batched_ref`
//! so queue teardown is not timed.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use send_queue::{PacketMeta, SendQueue};

use common::{NullSink, SMALL_BATCH, complete_all, dest_addr, fresh_queue};

/// One complete lifecycle: submit → flush → send_cqe → reclaim_cqe.
fn bench_lifecycle_single(c: &mut Criterion) {
    let dest = dest_addr();
    c.bench_function("lifecycle_single", |b| {
        b.iter_batched_ref(
            || fresh_queue(4096),
            |sq: &mut SendQueue| {
                let id = sq.submit(0, 64, dest, PacketMeta::new()).expect("submit");
                let mut sink = NullSink;
                sq.flush(&mut sink, 0).expect("flush");
                sq.on_send_cqe(id.raw());
                sq.on_reclaim_cqe(id.raw());
            },
            SMALL_BATCH,
        )
    });
}

/// `N` complete lifecycles performed sequentially in a single queue.
fn bench_lifecycle_burst(c: &mut Criterion) {
    let dest = dest_addr();
    let mut g = c.benchmark_group("lifecycle_burst");
    for &n in &[16usize, 64, 256] {
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched_ref(
                || fresh_queue(4096),
                |sq: &mut SendQueue| {
                    let mut sink = NullSink;
                    for i in 0..n {
                        let id = sq
                            .submit(i as u16, 64, dest, PacketMeta::new())
                            .expect("submit");
                        sq.flush(&mut sink, 0).expect("flush");
                        sq.on_send_cqe(id.raw());
                        sq.on_reclaim_cqe(id.raw());
                    }
                    black_box(sq.depth());
                },
                SMALL_BATCH,
            )
        });
    }
    g.finish();
}

/// Submit a burst, flush once, then complete all CQEs — measures the
/// batched path where amortisation hides per-packet overhead.
fn bench_lifecycle_batched(c: &mut Criterion) {
    let dest = dest_addr();
    let mut g = c.benchmark_group("lifecycle_batched");
    for &n in &[16usize, 64, 256] {
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched_ref(
                || fresh_queue(4096),
                |sq: &mut SendQueue| {
                    let mut sink = NullSink;
                    let mut ids = Vec::with_capacity(n);
                    for i in 0..n {
                        ids.push(
                            sq.submit(i as u16, 64, dest, PacketMeta::new())
                                .expect("submit"),
                        );
                    }
                    sq.flush(&mut sink, 0).expect("flush");
                    complete_all(sq, &ids);
                    black_box(sq.depth());
                },
                SMALL_BATCH,
            )
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_lifecycle_single,
    bench_lifecycle_burst,
    bench_lifecycle_batched,
);
criterion_main!(benches);

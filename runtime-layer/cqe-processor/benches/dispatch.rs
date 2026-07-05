//! Benchmark: per-OpKind dispatch overhead.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use io::ops::OpKind;

use common::{
    SMALL_BATCH, cancel_completion, fresh_processor, reclaim_completion, recv_completion,
    send_completion, timeout_completion, user_data_for,
};

fn bench_dispatch_recv(c: &mut Criterion) {
    let mut g = c.benchmark_group("dispatch_recv");
    g.throughput(Throughput::Elements(1));
    g.bench_function("single", |b| {
        b.iter_batched_ref(
            || fresh_processor(64),
            |proc| {
                let c = recv_completion(0, 128);
                let ud = user_data_for(OpKind::Recv, 0);
                let outcome = proc.dispatch_one(ud, &c, 0);
                black_box(outcome);
            },
            SMALL_BATCH,
        )
    });
    g.finish();
}

fn bench_dispatch_send(c: &mut Criterion) {
    let mut g = c.benchmark_group("dispatch_send");
    g.throughput(Throughput::Elements(1));
    g.bench_function("single", |b| {
        b.iter_batched_ref(
            || fresh_processor(64),
            |proc| {
                let c = send_completion(0);
                let ud = user_data_for(OpKind::Send, 0);
                let outcome = proc.dispatch_one(ud, &c, 0);
                black_box(outcome);
            },
            SMALL_BATCH,
        )
    });
    g.finish();
}

fn bench_dispatch_reclaim(c: &mut Criterion) {
    let mut g = c.benchmark_group("dispatch_reclaim");
    g.throughput(Throughput::Elements(1));
    g.bench_function("single", |b| {
        b.iter_batched_ref(
            || fresh_processor(64),
            |proc| {
                proc.register_send(0, 7);
                let c = reclaim_completion(0);
                let ud = user_data_for(OpKind::Send, 0);
                let outcome = proc.dispatch_one(ud, &c, 0);
                black_box(outcome);
            },
            SMALL_BATCH,
        )
    });
    g.finish();
}

fn bench_dispatch_timeout(c: &mut Criterion) {
    let mut g = c.benchmark_group("dispatch_timeout");
    g.throughput(Throughput::Elements(1));
    g.bench_function("single", |b| {
        b.iter_batched_ref(
            || fresh_processor(64),
            |proc| {
                let c = timeout_completion(0);
                let ud = user_data_for(OpKind::Timeout, 0);
                let outcome = proc.dispatch_one(ud, &c, 0);
                black_box(outcome);
            },
            SMALL_BATCH,
        )
    });
    g.finish();
}

fn bench_dispatch_cancel(c: &mut Criterion) {
    let mut g = c.benchmark_group("dispatch_cancel");
    g.throughput(Throughput::Elements(1));
    g.bench_function("single", |b| {
        b.iter_batched_ref(
            || fresh_processor(64),
            |proc| {
                let c = cancel_completion(0);
                let ud = user_data_for(OpKind::Cancel, 0);
                let outcome = proc.dispatch_one(ud, &c, 0);
                black_box(outcome);
            },
            SMALL_BATCH,
        )
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_dispatch_recv,
    bench_dispatch_send,
    bench_dispatch_reclaim,
    bench_dispatch_timeout,
    bench_dispatch_cancel,
);
criterion_main!(benches);

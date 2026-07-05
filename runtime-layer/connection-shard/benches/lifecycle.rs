//! Benchmark: create/destroy lifecycle churn.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use connection_shard::ConnectionShardBuilder;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

fn bench_create(c: &mut Criterion) {
    let mut g = c.benchmark_group("create");
    g.throughput(Throughput::Elements(1));
    g.bench_function("single", |b| {
        b.iter_batched(
            || {
                ConnectionShardBuilder::new()
                    .capacity(256)
                    .build()
                    .expect("shard")
            },
            |mut shard| {
                let id = shard.create(common::make_spec(0)).expect("create");
                black_box(id);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    g.finish();
}

fn bench_create_destroy_cycle(c: &mut Criterion) {
    let mut g = c.benchmark_group("create_destroy");
    g.throughput(Throughput::Elements(2));
    g.bench_function("cycle", |b| {
        b.iter_batched(
            || {
                ConnectionShardBuilder::new()
                    .capacity(256)
                    .build()
                    .expect("shard")
            },
            |mut shard| {
                let id = shard.create(common::make_spec(0)).expect("create");
                shard.destroy(id).expect("destroy");
                black_box(id);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    g.finish();
}

fn bench_touch(c: &mut Criterion) {
    let mut shard = ConnectionShardBuilder::new()
        .capacity(1024)
        .build()
        .expect("shard");
    let id = shard.create(common::make_spec(0)).expect("create");

    let mut g = c.benchmark_group("touch");
    g.throughput(Throughput::Elements(1));
    g.bench_function("recv", |b| {
        b.iter(|| {
            shard.touch_recv(black_box(id), 1024).expect("recv");
        });
    });
    g.bench_function("send", |b| {
        b.iter(|| {
            shard.touch_send(black_box(id), 1024).expect("send");
        });
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_create,
    bench_create_destroy_cycle,
    bench_touch
);
criterion_main!(benches);

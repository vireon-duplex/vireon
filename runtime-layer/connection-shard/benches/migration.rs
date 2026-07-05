//! Benchmark: migration protocol overhead.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use connection_shard::ConnectionShardBuilder;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

fn bench_prepare_migration(c: &mut Criterion) {
    let mut g = c.benchmark_group("migration");
    g.throughput(Throughput::Elements(1));

    g.bench_function("prepare", |b| {
        b.iter_batched(
            || {
                let mut shard = ConnectionShardBuilder::new()
                    .capacity(256)
                    .build()
                    .expect("shard");
                let id = shard.create(common::make_spec(0)).expect("create");
                shard.activate(id).expect("activate");
                (shard, id)
            },
            |(mut shard, id)| {
                let req = shard.prepare_migration(black_box(id), 1).expect("prepare");
                black_box(req);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    g.bench_function("accept", |b| {
        b.iter_batched(
            || {
                let mut source = ConnectionShardBuilder::new()
                    .reactor_id(0)
                    .capacity(256)
                    .build()
                    .expect("source");
                let id = source.create(common::make_spec(0)).expect("create");
                source.activate(id).expect("activate");
                let req = source.prepare_migration(id, 1).expect("prepare");
                let target = ConnectionShardBuilder::new()
                    .reactor_id(1)
                    .capacity(256)
                    .build()
                    .expect("target");
                (target, req)
            },
            |(mut target, req)| {
                let new_id = target.accept_migration(black_box(req)).expect("accept");
                black_box(new_id);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    g.finish();
}

criterion_group!(benches, bench_prepare_migration);
criterion_main!(benches);

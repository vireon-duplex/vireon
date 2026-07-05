//! Benchmark: slab allocation and generation tracking.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use connection_shard::{ConnectionId, ConnectionShardBuilder};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

fn bench_id_encode_decode(c: &mut Criterion) {
    let mut g = c.benchmark_group("id_codec");
    g.throughput(Throughput::Elements(1));
    g.bench_function("slot_decode", |b| {
        b.iter(|| {
            let id = ConnectionId::from_raw(black_box(0x700000002A));
            black_box(id.slot());
        });
    });
    g.bench_function("generation_decode", |b| {
        b.iter(|| {
            let id = ConnectionId::from_raw(black_box(0x700000002A));
            black_box(id.generation());
        });
    });
    g.finish();
}

fn bench_validate_id(c: &mut Criterion) {
    let mut shard = ConnectionShardBuilder::new()
        .capacity(1024)
        .build()
        .expect("shard");
    let id = shard.create(common::make_spec(0)).expect("create");

    let mut g = c.benchmark_group("validate_id");
    g.throughput(Throughput::Elements(1));
    g.bench_function("valid", |b| {
        b.iter(|| {
            let result = shard.view(black_box(id));
            black_box(result);
        });
    });
    g.bench_function("stale", |b| {
        b.iter(|| {
            let stale = ConnectionId::from_raw(
                ((id.generation().wrapping_add(1) as u64) << 20) | (id.slot() as u64),
            );
            let result = shard.view(black_box(stale));
            black_box(result);
        });
    });
    g.finish();
}

fn bench_recycle_churn(c: &mut Criterion) {
    let mut g = c.benchmark_group("recycle_churn");
    for &n in &[100usize, 1_000] {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || common::fresh_shard_no_bp(n + 10),
                |mut shard| {
                    let mut ids = Vec::with_capacity(n);
                    for i in 0..n {
                        ids.push(shard.create(common::make_spec(i as u64)).expect("create"));
                    }
                    for &id in &ids {
                        shard.destroy(id).expect("destroy");
                    }
                    black_box(ids.len());
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_id_encode_decode,
    bench_validate_id,
    bench_recycle_churn
);
criterion_main!(benches);

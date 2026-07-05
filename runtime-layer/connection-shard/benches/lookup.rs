//! Benchmark: stream_id lookup latency.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

fn bench_lookup_by_stream(c: &mut Criterion) {
    let mut g = c.benchmark_group("lookup_by_stream");
    for &fill in &[1_000usize, 16_000, 64_000] {
        let mut shard = common::fresh_shard_no_bp(65_536);
        let ids = common::fill_shard(&mut shard, fill as f64 / 65_536.0);
        g.throughput(Throughput::Elements(1));
        g.bench_with_input(BenchmarkId::from_parameter(fill), &ids, |b, ids| {
            b.iter(|| {
                let mid = ids[ids.len() / 2];
                let stream_id = mid.slot() as u64;
                let result = shard.lookup_by_stream(black_box(stream_id));
                black_box(result);
            });
        });
    }
    g.finish();
}

fn bench_lookup_miss(c: &mut Criterion) {
    let mut shard = common::fresh_shard_no_bp(65_536);
    let _ = common::fill_shard(&mut shard, 0.5);

    let mut g = c.benchmark_group("lookup_miss");
    g.throughput(Throughput::Elements(1));
    g.bench_function("miss", |b| {
        b.iter(|| {
            let result = shard.lookup_by_stream(black_box(u64::MAX));
            black_box(result);
        });
    });
    g.finish();
}

criterion_group!(benches, bench_lookup_by_stream, bench_lookup_miss);
criterion_main!(benches);

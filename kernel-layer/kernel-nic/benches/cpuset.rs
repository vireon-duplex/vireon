//! Benchmark: CpuSet operations.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use kernel_nic::CpuSet;

fn bench_cpuset_set(c: &mut Criterion) {
    let mut g = c.benchmark_group("cpuset_set");
    for &n in &[16usize, 64, 256, 1024] {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut s = CpuSet::empty();
                for i in 0..n as u32 {
                    s.set(i);
                }
                black_box(s.count());
            });
        });
    }
    g.finish();
}

fn bench_cpuset_contains(c: &mut Criterion) {
    let mut g = c.benchmark_group("cpuset_contains");
    let mut s = CpuSet::empty();
    for i in 0..1024u32 {
        s.set(i);
    }
    g.throughput(Throughput::Elements(1024));
    g.bench_function("1024", |b| {
        b.iter(|| {
            let mut count = 0u32;
            for i in 0..1024u32 {
                if s.contains(i) {
                    count += 1;
                }
            }
            black_box(count);
        });
    });
    g.finish();
}

fn bench_cpuset_first(c: &mut Criterion) {
    let mut g = c.benchmark_group("cpuset_first");
    let mut s = CpuSet::empty();
    s.set(0);
    s.set(63);
    s.set(127);
    s.set(255);
    s.set(511);
    s.set(1023);
    g.throughput(Throughput::Elements(1));
    g.bench_function("set_with_6_bits", |b| {
        b.iter(|| {
            black_box(s.first());
        });
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_cpuset_set,
    bench_cpuset_contains,
    bench_cpuset_first
);
criterion_main!(benches);

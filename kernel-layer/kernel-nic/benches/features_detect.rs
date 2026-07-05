//! Benchmark: KernelFeatures::detect latency.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{Criterion, criterion_group, criterion_main};

use kernel_nic::KernelFeatures;

fn bench_features_detect(c: &mut Criterion) {
    let mut g = c.benchmark_group("features");
    g.bench_function("detect", |b| {
        b.iter(|| {
            let f = KernelFeatures::detect().expect("detect");
            let _ = f.bits();
        });
    });
    g.finish();
}

fn bench_features_contains(c: &mut Criterion) {
    let f = KernelFeatures::detect().expect("detect");
    let mut g = c.benchmark_group("features_contains");
    g.bench_function("contains", |b| {
        b.iter(|| {
            let _ = f.contains(KernelFeatures::SO_REUSEPORT);
            let _ = f.contains(KernelFeatures::IO_URING);
            let _ = f.contains(KernelFeatures::UDP_GRO);
        });
    });
    g.finish();
}

criterion_group!(benches, bench_features_detect, bench_features_contains);
criterion_main!(benches);

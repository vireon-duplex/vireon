//! Benchmark: NicDiscovery::scan latency.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{Criterion, criterion_group, criterion_main};

use kernel_nic::NicDiscovery;

fn bench_nic_scan(c: &mut Criterion) {
    let mut g = c.benchmark_group("nic_scan");
    g.bench_function("scan", |b| {
        b.iter(|| {
            let nics = NicDiscovery::scan().expect("scan");
            let _ = nics.len();
        });
    });
    g.finish();
}

criterion_group!(benches, bench_nic_scan);
criterion_main!(benches);

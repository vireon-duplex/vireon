//! Benchmark: Stream cancellation throughput.
//!
//! Measures the overhead of cancelling an active RPC stream.

#![allow(missing_docs)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rpc_framework::{RpcClient, RpcConfig, StreamHandle};

#[cfg(test)]
fn bench_cancel(c: &mut Criterion) {
    let cfg = RpcConfig::new();
    let client = RpcClient::new(cfg);

    let mut group = c.benchmark_group("cancel");

    group.bench_function("cancel", |b| {
        let handle = StreamHandle::new(1);

        b.iter(|| {
            black_box(client.cancel(black_box(handle)));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_cancel);
criterion_main!(benches);

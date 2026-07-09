//! Benchmark: Round-trip RPC call latency.
//!
//! Measures the end-to-end latency of a unary RPC call.

#![allow(missing_docs)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rpc_framework::{RpcClient, RpcConfig, RpcMethodId};

#[cfg(test)]
fn bench_round_trip(c: &mut Criterion) {
    let cfg = RpcConfig::new();
    let client = RpcClient::new(cfg);

    let mut group = c.benchmark_group("round_trip");

    group.bench_function("call", |b| {
        let id = RpcMethodId::new(1);
        let request = b"test request payload";

        b.iter(|| {
            black_box(client.call(black_box(id), black_box(request)));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_round_trip);
criterion_main!(benches);

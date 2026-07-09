//! Benchmark: Stream opening latency.
//!
//! Measures the overhead of opening a new RPC stream.

#![allow(missing_docs)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rpc_framework::{RpcClient, RpcConfig, RpcMethodId, RpcMethodType};

#[cfg(test)]
fn bench_open_stream(c: &mut Criterion) {
    let cfg = RpcConfig::new();
    let client = RpcClient::new(cfg);

    let mut group = c.benchmark_group("open_stream");

    group.bench_function("open_unary", |b| {
        let id = RpcMethodId::new(1);
        let mtype = RpcMethodType::Unary;

        b.iter(|| {
            black_box(client.open_stream(black_box(id), black_box(mtype)));
        });
    });

    group.bench_function("open_bidi_stream", |b| {
        let id = RpcMethodId::new(2);
        let mtype = RpcMethodType::BidiStreaming;

        b.iter(|| {
            black_box(client.open_stream(black_box(id), black_box(mtype)));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_open_stream);
criterion_main!(benches);

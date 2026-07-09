//! Benchmark: Unary RPC dispatch throughput.
//!
//! Measures the overhead of dispatching a unary RPC request from
//! client through dispatcher to handler.

#![allow(missing_docs)]

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use rpc_framework::{RpcConfig, RpcMethodId, RpcMethodType, StreamHandle};

#[cfg(test)]
fn bench_dispatch_unary(c: &mut Criterion) {
    let cfg = RpcConfig::new();
    let dispatcher = rpc_framework::MethodDispatcher::new(cfg);

    let mut group = c.benchmark_group("dispatch_unary");
    group.throughput(Throughput::Bytes(1024));

    group.bench_function("dispatch", |b| {
        let id = RpcMethodId::new(1);
        let handle = StreamHandle::new(1);

        b.iter(|| {
            black_box(dispatcher.dispatch(black_box(id), black_box(handle)));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_dispatch_unary);
criterion_main!(benches);

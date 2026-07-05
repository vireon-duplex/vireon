//! Benchmark: SocketBuilder single-socket create+bind+configure.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use kernel_nic::{KernelFeatures, SocketBuilder, SocketSpec};

fn bench_socket_create(c: &mut Criterion) {
    // We use port 0 (kernel chooses ephemeral) so re-bind works
    // without TIME_WAIT collisions.
    let features = KernelFeatures::detect().expect("detect");
    let mut g = c.benchmark_group("socket_create");
    g.throughput(Throughput::Elements(1));
    g.bench_function("single", |b| {
        b.iter(|| {
            let spec = SocketSpec::for_reuseport(0, 0, 1);
            let builder = SocketBuilder::new(spec, features);
            let sock = builder.build().expect("build");
            let fd = sock.into_raw_fd();
            unsafe { libc::close(fd) };
            black_box(fd);
        });
    });
    g.finish();
}

criterion_group!(benches, bench_socket_create);
criterion_main!(benches);

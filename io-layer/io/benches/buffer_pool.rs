//! Benchmark: provided-buffer ring (PBUF_RING) operations.
//!
//! Measures kernel-registration cost for varying pool sizes, plus the
//! hot-path `refill_buffer` and `get_buffer_address` operations.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use io::buffer_pool::BufferPool;
use io_uring::IoUring;

fn bench_init_buffer_ring_count(c: &mut Criterion) {
    let mut g = c.benchmark_group("init_buffer_ring/varying_count");

    for &count in common::BUF_COUNTS {
        g.bench_function(format!("count={count}"), |b| {
            b.iter_batched(
                || common::fresh_ring(64),
                |ring| {
                    let pool = BufferPool::init_buffer_ring(&ring.submitter(), 1, count, 2048)
                        .expect("pool");
                    black_box(pool);
                },
                BatchSize::PerIteration,
            )
        });
    }
    g.finish();
}

fn bench_init_buffer_ring_size(c: &mut Criterion) {
    let mut g = c.benchmark_group("init_buffer_ring/varying_size");

    for &size in common::BUF_SIZES {
        g.bench_function(format!("size={size}"), |b| {
            b.iter_batched(
                || common::fresh_ring(64),
                |ring| {
                    let pool =
                        BufferPool::init_buffer_ring(&ring.submitter(), 1, 64, size).expect("pool");
                    black_box(pool);
                },
                BatchSize::PerIteration,
            )
        });
    }
    g.finish();
}

fn bench_with_defaults(c: &mut Criterion) {
    let mut g = c.benchmark_group("with_defaults");
    g.bench_function("count=256/size=2048", |b| {
        b.iter_batched(
            || common::fresh_ring(64),
            |ring| {
                let pool = BufferPool::with_defaults(&ring.submitter(), 1).expect("pool");
                black_box(pool);
            },
            BatchSize::PerIteration,
        )
    });
    g.finish();
}

fn bench_refill_buffer(c: &mut Criterion) {
    let ring = common::fresh_ring(256);
    let mut pool = common::fresh_pool(&ring, 1, 256, 2048);

    let mut g = c.benchmark_group("refill_buffer");
    g.bench_function("single", |b| {
        let mut buf_id = 0u16;
        b.iter(|| {
            pool.refill_buffer(buf_id);
            buf_id = buf_id.wrapping_add(1) & 0xFF; // cycle through 0..255
            black_box(&pool);
        })
    });
    g.finish();
}

fn bench_get_buffer_address(c: &mut Criterion) {
    let ring = common::fresh_ring(256);
    let pool = common::fresh_pool(&ring, 1, 256, 2048);

    let mut g = c.benchmark_group("get_buffer_address");
    let mut rng = common::XorShift64::new();
    g.bench_function("random_lookup", |b| {
        b.iter(|| {
            let buf_id = rng.next_bounded(256) as u16;
            let addr = pool.get_buffer_address(buf_id);
            black_box(addr);
        })
    });

    g.bench_function("sequential_lookup", |b| {
        let mut buf_id = 0u16;
        b.iter(|| {
            let addr = pool.get_buffer_address(buf_id);
            buf_id = buf_id.wrapping_add(1) & 0xFF;
            black_box(addr);
        })
    });
    g.finish();
}

// Keep ring alive so the pool's registration stays valid.
fn _keep_alive(_r: &IoUring) {}

criterion_group!(
    benches,
    bench_init_buffer_ring_count,
    bench_init_buffer_ring_size,
    bench_with_defaults,
    bench_refill_buffer,
    bench_get_buffer_address,
);
criterion_main!(benches);

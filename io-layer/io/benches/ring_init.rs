//! Benchmark: io_uring ring setup cost.
//!
//! Measures kernel-syscall overhead for `io_uring_setup(2)` and the
//! registration calls (`register_buffers`, `register_files_sparse`).

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use io::context::IoUringCtx;
use io_uring::IoUring;

fn bench_init_ring(c: &mut Criterion) {
    let mut g = c.benchmark_group("init_ring");

    for &entries in common::RING_ENTRIES {
        g.bench_function(format!("entries={entries}"), |b| {
            b.iter_batched(
                || (),
                |()| {
                    let ctx = IoUringCtx::init_ring(entries, false, None).expect("ring");
                    black_box(ctx);
                },
                BatchSize::PerIteration,
            )
        });
    }

    // SQPOLL requires CAP_SYS_ADMIN; skip gracefully if denied.
    match IoUringCtx::init_ring(64, true, None) {
        Ok(_) => {
            g.bench_function("entries=64/sqpoll=true", |b| {
                b.iter_batched(
                    || (),
                    |()| {
                        let ctx = IoUringCtx::init_ring(64, true, None).expect("ring");
                        black_box(ctx);
                    },
                    BatchSize::PerIteration,
                )
            });
        }
        Err(e) => {
            // sqpoll requires CAP_SYS_NICE or elevated privileges — skip silently.
            let _ = e;
        }
    }

    g.finish();
}

fn bench_register_fixed_buffers(c: &mut Criterion) {
    let mut g = c.benchmark_group("register_fixed_buffers");
    let counts: &[u32] = &[16, 64, 256];

    for &count in counts {
        g.bench_function(format!("iovecs={count}"), |b| {
            b.iter_batched(
                || {
                    let ring = common::fresh_ring(64);
                    // Allocate backing memory that outlives the registration.
                    let bufs: Vec<Vec<u8>> = (0..count).map(|_| vec![0u8; 2048]).collect();
                    let iovs: Vec<libc::iovec> = bufs
                        .iter()
                        .map(|b| libc::iovec {
                            iov_base: b.as_ptr() as *mut _,
                            iov_len: b.len(),
                        })
                        .collect();
                    (ring, iovs, bufs)
                },
                |(ring, iovs, _bufs)| {
                    // SAFETY: iovs point into the Vec<Vec<u8>> kept alive
                    // by the tuple for the duration of the call.
                    unsafe {
                        ring.submitter().register_buffers(&iovs).expect("register");
                    }
                },
                BatchSize::PerIteration,
            )
        });
    }
    g.finish();
}

fn bench_register_sparse_files(c: &mut Criterion) {
    let mut g = c.benchmark_group("register_sparse_files");

    for &count in &[64u32, 256, 1024] {
        g.bench_function(format!("files={count}"), |b| {
            b.iter_batched(
                || common::fresh_ring(64),
                |ring| {
                    ring.submitter()
                        .register_files_sparse(count)
                        .expect("register files");
                },
                BatchSize::PerIteration,
            )
        });
    }
    g.finish();
}

fn bench_ring_overhead(c: &mut Criterion) {
    // Baseline: how fast can we create a bare IoUring (without our wrapper)?
    let mut g = c.benchmark_group("raw_io_uring_new");
    g.bench_function("entries=64", |b| {
        b.iter_batched(
            || (),
            |()| {
                let ring = IoUring::new(64).expect("ring");
                black_box(ring);
            },
            BatchSize::PerIteration,
        )
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_init_ring,
    bench_register_fixed_buffers,
    bench_register_sparse_files,
    bench_ring_overhead
);
criterion_main!(benches);

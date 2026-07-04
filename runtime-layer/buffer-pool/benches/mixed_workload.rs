//! Mixed allocation benchmarks (Category 8).
//!
//! Real workloads don't request a single size — they mix small heartbeats,
//! medium payloads, and occasional large frames. This file measures the
//! pool's behaviour under size distributions that approximate production
//! traffic.
//!
//! ## Distributions
//!
//! - **uniform** — equal probability across 5 sizes (64 B – 256 KiB).
//! - **production** — biased toward small sizes: 50 % heartbeat, 25 % tool
//!   call, 15 % audio, 7 % image, 3 % video (models a chat / streaming server).
//! - **bimodal** — 80 % small (64 B) + 20 % large (128 KiB), stress-tests
//!   cache locality across two distant classes.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::{criterion_group, criterion_main};

use buffer_pool::BufferPool;

/// Pre-generates `count` request sizes following `weights` distribution.
///
/// `entries` is `[(size, weight)]` where weights sum to 1.0.
fn generate_distribution(entries: &[(usize, f64)], count: usize) -> Vec<usize> {
    let mut rng = common::XorShift64::new(0xAABBCCDD_EEFF0011);
    (0..count)
        .map(|_| {
            let r = (rng.next_u64() as f64) / (u64::MAX as f64);
            let mut acc = 0.0;
            let mut chosen = entries[0].0;
            for &(size, w) in entries {
                acc += w;
                if r < acc {
                    chosen = size;
                    break;
                }
            }
            chosen
        })
        .collect()
}

/// Uniform: equal probability across 5 sizes.
fn uniform_sizes() -> Vec<usize> {
    generate_distribution(
        &[
            (64, 0.20),
            (512, 0.20),
            (4096, 0.20),
            (65_536, 0.20),
            (262_144, 0.20),
        ],
        10_000,
    )
}

/// Production-like: biased toward small sizes.
fn production_sizes() -> Vec<usize> {
    generate_distribution(
        &[
            (64, 0.50),      // heartbeat / ack
            (4096, 0.25),    // tool call
            (80_000, 0.15),  // audio frame
            (256_000, 0.07), // image frame
            (320_000, 0.03), // video frame
        ],
        10_000,
    )
}

/// Bimodal: small + large only.
fn bimodal_sizes() -> Vec<usize> {
    generate_distribution(&[(64, 0.80), (131_072, 0.20)], 10_000)
}

// ============================================================================
// Mixed: uniform distribution
// ============================================================================

fn bench_mixed_uniform(c: &mut Criterion) {
    let sizes = uniform_sizes();
    let avg: usize = sizes.iter().sum::<usize>() / sizes.len();
    let total: usize = sizes.iter().sum();

    let mut group = c.benchmark_group("mixed/uniform");
    group.throughput(Throughput::Bytes(total as u64));

    group.bench_function("10k_ops", |b| {
        let pool = BufferPool::default();

        b.iter(|| {
            for &size in &sizes {
                let mut buf = pool.get(size).expect("get");
                buf[0] = 0x01;
                black_box(buf.as_ptr());
                drop(buf);
            }
        });

        let stats = pool.stats();
        assert_eq!(stats.counters.outstanding, 0);
    });

    group.finish();
    // Prevent avg from being optimised away.
    let _ = black_box(avg);
}

// ============================================================================
// Mixed: production distribution
// ============================================================================

fn bench_mixed_production(c: &mut Criterion) {
    let sizes = production_sizes();
    let total: usize = sizes.iter().sum();

    let mut group = c.benchmark_group("mixed/production");
    group.throughput(Throughput::Bytes(total as u64));

    group.bench_function("10k_ops", |b| {
        let pool = BufferPool::default();

        b.iter(|| {
            for &size in &sizes {
                let mut buf = pool.get(size).expect("get");
                buf[0] = 0x02;
                black_box(buf.as_ptr());
                drop(buf);
            }
        });

        let stats = pool.stats();
        assert_eq!(stats.counters.outstanding, 0);
        // Document reuse efficiency.
        let _ = black_box((stats.hit_rate(), stats.reuse_rate()));
    });

    group.finish();
}

// ============================================================================
// Mixed: bimodal distribution
// ============================================================================

fn bench_mixed_bimodal(c: &mut Criterion) {
    let sizes = bimodal_sizes();
    let total: usize = sizes.iter().sum();

    let mut group = c.benchmark_group("mixed/bimodal");
    group.throughput(Throughput::Bytes(total as u64));

    group.bench_function("10k_ops", |b| {
        let pool = BufferPool::default();

        b.iter(|| {
            for &size in &sizes {
                let mut buf = pool.get(size).expect("get");
                buf[0] = 0x03;
                black_box(buf.as_ptr());
                drop(buf);
            }
        });

        assert_eq!(pool.stats().counters.outstanding, 0);
    });

    group.finish();
}

// ============================================================================
// Mixed: interleaved hold (models concurrent in-flight buffers)
// ============================================================================

/// Allocates several buffers before dropping any, simulating a pipeline
/// where multiple buffers are live simultaneously.
fn bench_mixed_interleaved(c: &mut Criterion) {
    let sizes = production_sizes();
    let total: usize = sizes.iter().sum();

    let mut group = c.benchmark_group("mixed/interleaved");
    group.throughput(Throughput::Bytes(total as u64));

    group.bench_function("hold_8_deep", |b| {
        let pool = BufferPool::default();
        const WINDOW: usize = 8;

        b.iter(|| {
            let mut live: Vec<_> = Vec::with_capacity(WINDOW);
            for &size in &sizes {
                let mut buf = pool.get(size).expect("get");
                buf[0] = 0x04;
                black_box(buf.as_ptr());
                live.push(buf);
                if live.len() >= WINDOW {
                    // Drop oldest half.
                    let half = WINDOW / 2;
                    for buf in live.drain(..half) {
                        drop(buf);
                    }
                }
            }
            drop(live);
        });

        assert_eq!(pool.stats().counters.outstanding, 0);
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_mixed_uniform,
    bench_mixed_production,
    bench_mixed_bimodal,
    bench_mixed_interleaved,
);
criterion_main!(benches);

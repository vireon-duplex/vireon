//! Real-world workload benchmarks (Categories 16–19).
//!
//! Simulates four production patterns to measure end-to-end performance
//! including buffer acquisition, data manipulation, and release:
//!
//! - **Category 16 — QUIC frame**: acquire → write header → append payload →
//!   "send" → drop. MTU-aware sizes (64 B – 64 KiB).
//! - **Category 17 — Streaming**: acquire small → write chunks → grow by
//!   reallocating through the pool → drop. Models dynamic buffer expansion.
//! - **Category 18 — Event bus**: acquire → serialize framed message → drop.
//!   Measures messages/sec for a publish loop.
//! - **Category 19 — Codec**: acquire → encode frame → decode frame → drop.
//!   Measures the encode/decode round-trip on pooled memory.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::{criterion_group, criterion_main};

use buffer_pool::BufferPool;

// ============================================================================
// QUIC frame workload (Category 16)
// ============================================================================

/// QUIC long header: 1 B flags + 4 B version + 8 B dst-cid + 4 B length.
const QUIC_HEADER_LEN: usize = 17;

fn bench_quic_frame(c: &mut Criterion) {
    let mut group = c.benchmark_group("quic/frame");

    for &(size, label) in common::QUIC_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(label), &size, |b, &size| {
            let pool = BufferPool::default();

            // Pre-warm.
            let warmup = pool.get(size).expect("get");
            drop(warmup);

            b.iter(|| {
                let mut buf = pool.get(size).expect("get");

                // Encode header.
                buf[0] = 0xC0; // long header flag
                buf[1..5].copy_from_slice(&0x00000001u32.to_be_bytes()); // version
                buf[5..13].copy_from_slice(&[0xAB; 8]); // dst connection id
                let payload_len = size.saturating_sub(QUIC_HEADER_LEN + 4);
                buf[13..17].copy_from_slice(&(payload_len as u32).to_be_bytes());

                // Append payload.
                if size > QUIC_HEADER_LEN + 4 {
                    common::fill_pattern(&mut buf[QUIC_HEADER_LEN + 4..], 0x50);
                }

                // "Send".
                black_box(buf.as_ptr());
                black_box(buf.len());
                drop(buf);
            });
        });
    }

    group.finish();
}

// ============================================================================
// Streaming: dynamic buffer growth (Category 17)
// ============================================================================

/// Models a streaming write pattern: start with a small buffer, append
/// chunks, and grow by acquiring a larger buffer + copying when full.
fn bench_streaming_grow(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming/grow");

    // 10 chunks of 1 KiB each — forces 3-4 growth doublings from 1 KiB start.
    let chunk_size = 1024;
    let num_chunks = 10;
    let total_bytes = chunk_size * num_chunks;
    group.throughput(Throughput::Bytes(total_bytes as u64));

    group.bench_function("1KiB_chunks_x10", |b| {
        let pool = BufferPool::default();

        // Pre-allocate a chunk to write (avoids measuring chunk gen).
        let chunk: Vec<u8> = (0..chunk_size).map(|i| i as u8).collect();

        b.iter(|| {
            let mut buf = pool.get(1024).expect("get");
            let mut written = 0usize;

            for _ in 0..num_chunks {
                if written + chunk_size > buf.len() {
                    // Grow: double until it fits.
                    let new_size = buf.len() * 2;
                    let mut new_buf = pool.get(new_size).expect("get");
                    new_buf[..written].copy_from_slice(&buf[..written]);
                    drop(buf);
                    buf = new_buf;
                }
                buf[written..written + chunk_size].copy_from_slice(&chunk);
                written += chunk_size;
            }

            black_box(buf.as_ptr());
            black_box(written);
            drop(buf);
        });

        assert_eq!(pool.stats().counters.outstanding, 0);
    });

    group.finish();
}

// ============================================================================
// Event bus: serialize + publish (Category 18)
// ============================================================================

/// Simulates an event-bus publish loop: acquire a buffer, frame-encode a
/// message, "publish" (black_box), release.
fn bench_event_bus(c: &mut Criterion) {
    let mut group = c.benchmark_group("eventbus/publish");

    let cases: &[(usize, &str)] = &[
        (64, "tiny"),
        (256, "small"),
        (1024, "medium"),
        (4096, "large"),
    ];

    for &(size, label) in cases {
        let msg_size = size.saturating_sub(16); // leave room for framing
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new(label, size), &size, |b, &size| {
            let pool = BufferPool::default();
            let warmup = pool.get(size).expect("get");
            drop(warmup);

            let payload: Vec<u8> = (0..msg_size).map(|i| i as u8).collect();

            b.iter(|| {
                let mut buf = pool.get(size).expect("get");

                // Frame: [magic 4][msg_id 8][len 4][payload].
                buf[0..4].copy_from_slice(b"QICB");
                buf[4..12].copy_from_slice(&0xDEAD_BEEFu64.to_le_bytes());
                buf[12..16].copy_from_slice(&(msg_size as u32).to_le_bytes());
                buf[16..16 + msg_size].copy_from_slice(&payload);

                // Publish.
                black_box(buf.as_ptr());
                drop(buf);
            });
        });
    }

    group.finish();
}

// ============================================================================
// Codec: encode + decode round-trip (Category 19)
// ============================================================================

/// Acquire a buffer, encode a framed message, decode it back (validate),
/// then release. Measures the full encode/decode cycle on pooled memory.
fn bench_codec_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec/roundtrip");

    let cases: &[(usize, &str)] = &[
        (64, "tiny"),
        (1200, "mtu"),
        (4096, "medium"),
        (16_384, "large"),
    ];

    for &(size, label) in cases {
        let payload_len = size.saturating_sub(5); // 1 type + 4 length
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new(label, size), &size, |b, &size| {
            let pool = BufferPool::default();
            let warmup = pool.get(size).expect("get");
            drop(warmup);

            let payload: Vec<u8> = (0..payload_len).map(|i| (i * 7) as u8).collect();

            b.iter(|| {
                let mut buf = pool.get(size).expect("get");

                // --- Encode ---
                buf[0] = 0x01; // frame type
                buf[1..5].copy_from_slice(&(payload_len as u32).to_be_bytes());
                buf[5..5 + payload_len].copy_from_slice(&payload);

                // --- Decode ---
                let frame_type = buf[0];
                let decoded_len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                debug_assert_eq!(decoded_len, payload_len);
                let checksum = buf[5 + payload_len - 1]; // last byte

                black_box(frame_type);
                black_box(decoded_len);
                black_box(checksum);
                drop(buf);
            });
        });
    }

    group.finish();
}

// ============================================================================
// Concurrent event bus (Category 18 — was multi-thread publish, now sequential)
// ============================================================================

/// Was: multiple threads publishing events simultaneously.
/// Now: single-threaded equivalent running the same total message count.
/// BufferPool is `!Send` (uses `Rc` internally) so cross-thread sharing is
/// no longer possible.
fn bench_eventbus_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("eventbus/concurrent");
    const SIZE: usize = 1024;
    const MSGS_PER_THREAD: usize = 5_000;

    for &num_threads in &[1usize, 2, 4, 8] {
        let total = (num_threads * MSGS_PER_THREAD) as u64;
        group.throughput(Throughput::Elements(total));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            &num_threads,
            |b, &num_threads| {
                let pool = BufferPool::default();
                let total_msgs = num_threads * MSGS_PER_THREAD;

                b.iter(|| {
                    for i in 0..total_msgs {
                        let mut buf = pool.get(SIZE).expect("get");
                        // Minimal framing.
                        buf[0..4].copy_from_slice(b"QICB");
                        buf[4..12].copy_from_slice(&(i as u64).to_le_bytes());
                        buf[12] = 0;
                        black_box(buf.as_ptr());
                        drop(buf);
                    }
                });

                assert_eq!(pool.stats().counters.outstanding, 0);
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_quic_frame,
    bench_streaming_grow,
    bench_event_bus,
    bench_codec_roundtrip,
    bench_eventbus_concurrent,
);
criterion_main!(benches);

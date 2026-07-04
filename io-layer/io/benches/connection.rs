//! Benchmark: UDP connection setup, recv arming, buffer parsing, and
//! send_zc submission.
//!
//! Measures both kernel-interaction paths (socket setup, SQE submit) and
//! pure-compute paths (CQE flag decode, buffer parsing).

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

use io::connection::{Connection, TxNotification};

// ─── Synthetic buffer builder ─────────────────────────────────────

/// Size of the `io_uring_recvmsg_out` header (`{u32,u32,u32,u32}` = 16 B).
const RECVMSG_OUT_HDR: usize = 16;

/// `PEER_ADDR_LEN` used by `Connection::setup` (must match).
const NAME_FIELD_LEN: usize = 128;

/// Builds a synthetic buffer matching the layout the kernel would fill
/// for a `RecvMsgMulti` CQE: `[header 16B][name 128B][control 0B][payload]`.
fn build_recv_buffer(payload_len: usize) -> Vec<u8> {
    let total = RECVMSG_OUT_HDR + NAME_FIELD_LEN + payload_len;
    let mut buf = vec![0u8; total];

    // Header: namelen=16 (sockaddr_in), controllen=0, payloadlen, flags=0.
    buf[0..4].copy_from_slice(&16u32.to_ne_bytes()); // namelen
    buf[4..8].copy_from_slice(&0u32.to_ne_bytes()); // controllen
    buf[8..12].copy_from_slice(&(payload_len as u32).to_ne_bytes()); // payloadlen
    buf[12..16].copy_from_slice(&0u32.to_ne_bytes()); // flags

    // Name field: a sockaddr_in at offset 16.
    let sin = libc::sockaddr_in {
        sin_family: libc::AF_INET as u16,
        sin_port: 1234u16.to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from(Ipv4Addr::new(127, 0, 0, 1)).to_be(),
        },
        sin_zero: [0; 8],
    };
    unsafe {
        std::ptr::write_unaligned(
            buf.as_mut_ptr().add(RECVMSG_OUT_HDR) as *mut libc::sockaddr_in,
            sin,
        );
    }

    // Payload: first byte = opcode, next 8 = stream_id, rest = data.
    let payload_start = RECVMSG_OUT_HDR + NAME_FIELD_LEN;
    buf[payload_start] = 0x01; // TEXT_TOKEN
    // stream_id = 0 (already zeroed)
    for (i, byte) in buf[payload_start + 9..].iter_mut().enumerate() {
        *byte = (i as u8).wrapping_mul(7);
    }

    buf
}

// ─── Benches ──────────────────────────────────────────────────────

fn bench_setup_socket(c: &mut Criterion) {
    let mut g = c.benchmark_group("setup_socket");

    g.bench_function("ephemeral_port", |b| {
        b.iter_batched(
            || (),
            |()| {
                let conn = Connection::setup_socket(0).expect("setup");
                black_box(conn);
            },
            BatchSize::PerIteration,
        )
    });

    g.bench_function("with_reuseport", |b| {
        b.iter_batched(
            || (),
            |()| {
                let conn = Connection::setup_socket_with_reuseport(0, 0).expect("setup");
                black_box(conn);
            },
            BatchSize::PerIteration,
        )
    });

    g.finish();
}

fn bench_arm_multishot_recv(c: &mut Criterion) {
    let conn = common::fresh_connection(0);
    let mut g = c.benchmark_group("arm_multishot_recv");

    g.bench_function("submit", |b| {
        b.iter_batched(
            || common::fresh_ring(64),
            |mut ring| {
                conn.arm_multishot_recv(&mut ring, 1, 0xDEAD).expect("arm");
                black_box(&ring);
            },
            BatchSize::PerIteration,
        )
    });

    g.finish();
}

fn bench_parse_received_buffer(c: &mut Criterion) {
    let conn = common::fresh_connection(0);
    let mut g = c.benchmark_group("parse_received_buffer");

    // Payload lengths to sweep (excludes the 9-byte frame header).
    for &payload_len in &[55usize, 247, 1015, 2039] {
        let buf = build_recv_buffer(payload_len);
        let label = format!("total={}B", buf.len());

        g.bench_function(label, |b| {
            b.iter(|| {
                let frame = conn.parse_received_buffer(&buf).expect("parse");
                black_box(frame);
            })
        });
    }

    g.finish();
}

fn bench_handle_tx_notification(c: &mut Criterion) {
    let mut g = c.benchmark_group("handle_tx_notification");

    const NOTIF: u32 = 1 << 3; // IORING_CQE_F_NOTIF

    g.bench_function("send_completed", |b| {
        b.iter(|| {
            let r = Connection::handle_tx_notification(0, 1024, 42);
            black_box(r);
        })
    });

    g.bench_function("buffer_reclaimable", |b| {
        b.iter(|| {
            let r = Connection::handle_tx_notification(NOTIF, 0, 42);
            black_box(r);
        })
    });

    g.finish();
}

fn bench_send_packet_zc(c: &mut Criterion) {
    let conn = common::fresh_connection(0);
    let dest = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 9999));
    let mut g = c.benchmark_group("send_packet_zc");

    for &pkt_size in common::PKT_SIZES {
        g.bench_function(format!("{pkt_size}B"), |b| {
            b.iter_batched(
                || common::fresh_ring(64),
                |mut ring| {
                    // SQE build + submit path (kernel will fail with
                    // -EFAULT since no fixed buffers are registered, but
                    // we measure the submission overhead only).
                    let _ = conn.send_packet_zc(&mut ring, 0, pkt_size as u32, dest, 0xBEEF);
                    black_box(&ring);
                },
                BatchSize::PerIteration,
            )
        });
    }

    g.finish();
}

// Suppress unused-import warning for Duration (used in future timer benches).
#[allow(dead_code)]
fn _duration_marker() -> Duration {
    Duration::from_millis(0)
}

// Suppress unused import.
#[allow(dead_code)]
const _TX_NOTIF_MARKER: Option<TxNotification> = None;

criterion_group!(
    benches,
    bench_setup_socket,
    bench_arm_multishot_recv,
    bench_parse_received_buffer,
    bench_handle_tx_notification,
    bench_send_packet_zc,
);
criterion_main!(benches);

//! Benchmark: Socket setup, recv arming, buffer parsing, send_zc, and
//! completion decoding.
//!
//! Measures both kernel-interaction paths (socket setup, SQE submit) and
//! pure-compute paths (CQE decode, buffer parsing).

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

use io::completion::CompletionDecoder;
use io::ops::{OpKind, Sender, decode_id, decode_kind, encode};
use io::socket::Socket;

// ─── Synthetic buffer builder ─────────────────────────────────────

/// Size of the `io_uring_recvmsg_out` header (`{u32,u32,u32,u32}` = 16 B).
const RECVMSG_OUT_HDR: usize = 16;

/// `PEER_ADDR_LEN` used by `Receiver::new` (must match).
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
    let mut g = c.benchmark_group("socket_bind");

    g.bench_function("ephemeral_port", |b| {
        b.iter_batched(
            || (),
            |()| {
                let sock = Socket::bind(0).expect("bind");
                black_box(sock);
            },
            BatchSize::PerIteration,
        )
    });

    g.bench_function("with_reuseport", |b| {
        b.iter_batched(
            || (),
            |()| {
                let sock = Socket::bind_with_reuseport(0, 0).expect("bind");
                black_box(sock);
            },
            BatchSize::PerIteration,
        )
    });

    g.finish();
}

fn bench_arm_multishot_recv(c: &mut Criterion) {
    let socket = common::fresh_socket(0);
    let receiver = socket.receiver(1);
    let mut g = c.benchmark_group("receiver_arm");

    g.bench_function("submit", |b| {
        b.iter_batched(
            || common::fresh_ring(64),
            |mut ring| {
                receiver.arm(&mut ring, 0xDEAD).expect("arm");
                black_box(&ring);
            },
            BatchSize::PerIteration,
        )
    });

    g.finish();
}

fn bench_parse_received_buffer(c: &mut Criterion) {
    let socket = common::fresh_socket(0);
    let receiver = socket.receiver(1);
    let mut g = c.benchmark_group("receiver_parse");

    // Payload lengths to sweep (excludes the 9-byte frame header).
    for &payload_len in &[55usize, 247, 1015, 2039] {
        let buf = build_recv_buffer(payload_len);
        let label = format!("total={}B", buf.len());

        g.bench_function(label, |b| {
            b.iter(|| {
                let frame = receiver.parse(&buf).expect("parse");
                black_box(frame);
            })
        });
    }

    g.finish();
}

fn bench_completion_decode(c: &mut Criterion) {
    let decoder = CompletionDecoder;
    let mut g = c.benchmark_group("completion_decode");

    // Simulate a recv success CQE: user_data = encode(Recv, 42), res=128.
    let recv_ud = encode(OpKind::Recv, 42);

    g.bench_function("recv_success", |b| {
        b.iter(|| {
            let ud = black_box(recv_ud);
            let kind = decode_kind(ud);
            let id = decode_id(ud);
            black_box((kind, id));
        })
    });

    // Simulate a send notification CQE: user_data = encode(Send, 99).
    let send_ud = encode(OpKind::Send, 99);

    g.bench_function("send_notif", |b| {
        b.iter(|| {
            let ud = black_box(send_ud);
            let kind = decode_kind(ud);
            let id = decode_id(ud);
            black_box((kind, id));
        })
    });

    let _ = decoder; // suppress unused warning

    g.finish();
}

fn bench_send_zc(c: &mut Criterion) {
    let socket = common::fresh_socket(0);
    let sender: Sender = socket.sender();
    let dest = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 9999));
    let mut g = c.benchmark_group("send_zc");

    for &pkt_size in common::PKT_SIZES {
        g.bench_function(format!("{pkt_size}B"), |b| {
            b.iter_batched(
                || common::fresh_ring(64),
                |mut ring| {
                    // SQE build + submit path (kernel will fail with
                    // -EFAULT since no fixed buffers are registered, but
                    // we measure the submission overhead only).
                    let _ = sender.send_zc(&mut ring, 0, pkt_size as u32, dest, 0xBEEF);
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

criterion_group!(
    benches,
    bench_setup_socket,
    bench_arm_multishot_recv,
    bench_parse_received_buffer,
    bench_completion_decode,
    bench_send_zc,
);
criterion_main!(benches);

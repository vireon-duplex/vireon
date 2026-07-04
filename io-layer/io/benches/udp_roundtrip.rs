//! Benchmark: end-to-end UDP round-trip latency.
//!
//! Sets up a full io_uring RX path (Connection + BufferPool + IoUring),
//! arms multishot recv, then measures the per-packet cycle:
//!
//! ```text
//! sender.send_to(pkt) → ring.submit_and_wait(1) → drain CQE → parse → refill
//! ```
//!
//! Reports per-packet latency at varying payload sizes.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::hint::black_box;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use io_uring::cqueue;

use io::memory::BufferPool;
use io::ops::Receiver;
use io::socket::Socket;

/// Frame header: 1-byte opcode + 8-byte stream_id.
const FRAME_HDR: usize = 9;

fn bench_udp_roundtrip(c: &mut Criterion) {
    let mut g = c.benchmark_group("udp_roundtrip");
    g.sample_size(50); // each sample includes a kernel round-trip

    for &payload_len in common::PKT_SIZES {
        let pkt_size = payload_len + FRAME_HDR;
        let label = format!("{pkt_size}B");

        g.bench_function(label, |b| {
            b.iter_batched(
                || setup_roundtrip(pkt_size),
                |mut ctx| {
                    // ── Routine: send one packet and reap one CQE ──
                    let pkt = build_packet(pkt_size);
                    ctx.sender.send_to(&pkt, ctx.dest).expect("send_to");

                    // Block until the kernel posts at least one CQE.
                    ctx.ring.submit_and_wait(1).expect("submit_and_wait");

                    // Drain the CQE.
                    let entries: Vec<cqueue::Entry> = {
                        let mut cq = ctx.ring.completion();
                        cq.by_ref().collect()
                    };

                    for entry in entries {
                        let res = entry.result();
                        if res < 0 {
                            continue;
                        }
                        let flags = entry.flags();

                        // Decode buf_id and refill the pool.
                        if let Some(buf_id) = cqueue::buffer_select(flags) {
                            // Read the payload from the buffer for parse bench.
                            if let Some(addr) = ctx.pool.get_buffer_address(buf_id) {
                                let raw = unsafe {
                                    std::slice::from_raw_parts(addr.as_ptr(), res as usize)
                                };
                                // Parse the io_uring_recvmsg_out layout.
                                let _ = ctx.receiver.parse(raw);
                            }
                            ctx.pool.refill_buffer(buf_id);
                        }

                        // Re-arm if multishot terminated.
                        if !cqueue::more(flags) {
                            let _ = ctx.receiver.arm(&mut ctx.ring, 0);
                        }
                    }

                    black_box(&ctx);
                },
                BatchSize::PerIteration,
            )
        });
    }

    g.finish();
}

// ─── Setup ────────────────────────────────────────────────────────

#[allow(dead_code)]
struct RoundtripCtx {
    ring: io_uring::IoUring,
    socket: Socket,
    receiver: Receiver,
    pool: BufferPool,
    sender: UdpSocket,
    dest: std::net::SocketAddr,
}

fn setup_roundtrip(pkt_size: usize) -> RoundtripCtx {
    let _ = pkt_size; // only affects the packet built per-iteration

    let mut ring = common::fresh_ring(256);
    let pool = common::fresh_pool(&ring, 1, 256, 2048);
    let socket = common::fresh_socket(0);
    let receiver = socket.receiver(pool.bgid());

    // Discover the bound port.
    let dest = socket.local_addr().expect("local_addr");

    // Arm multishot recv.
    receiver.arm(&mut ring, 0).expect("arm");

    // Sender socket.
    let sender = UdpSocket::bind("0.0.0.0:0").expect("sender bind");
    let sender_port = sender.local_addr().expect("addr").port();

    // If dest is 0.0.0.0, use 127.0.0.1 instead for loopback.
    let dest = match dest {
        std::net::SocketAddr::V4(v4) if v4.ip() == &Ipv4Addr::UNSPECIFIED => {
            std::net::SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, v4.port()))
        }
        other => other,
    };

    // Suppress unused warning.
    let _ = sender_port;

    RoundtripCtx {
        ring,
        socket,
        receiver,
        pool,
        sender,
        dest,
    }
}

fn build_packet(pkt_size: usize) -> Vec<u8> {
    let mut pkt = vec![0u8; pkt_size];
    pkt[0] = 0x01; // TEXT_TOKEN
    // stream_id = 0 (already zeroed)
    for (i, byte) in pkt[FRAME_HDR..].iter_mut().enumerate() {
        *byte = (i as u8).wrapping_mul(3);
    }
    pkt
}

criterion_group!(benches, bench_udp_roundtrip);
criterion_main!(benches);

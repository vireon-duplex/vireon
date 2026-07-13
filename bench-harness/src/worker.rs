//! Per-connection load-test driver.
//!
//! Supports four benchmark modes, selected via [`BenchMode`]:
//!
//! | Mode | Description |
//! |---|---|
//! | `Rr` | **Request-Response** — 1 QUIC stream, send batch -> wait echo -> repeat. |
//! | `MultiRr` | **Multi-stream R-R** — 15 streams fired concurrently, wait all echoes. |
//! | `Stream` | **Full-duplex streaming** — fire continuously without per-batch wait. |
//! | `Pipeline` | **Pipelined R-R** — keep N requests in-flight before waiting. |
//!
//! All send/recv goes through the [`ClientStack`] layer chain
//! (application -> session -> stream -> message -> encode/decode).

use std::collections::VecDeque;
use std::io::ErrorKind;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::config::build_client_config;
use crate::gso::send_with_gso;
use crate::layers::ClientStack;
use crate::stats::WorkerResult;
use crate::workload::WorkloadEntry;

/// Selects the measurement strategy for each worker.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BenchMode {
    /// Classic request-response on a **single** stream (stream 0).
    Rr,
    /// All 15 frames sent on separate streams simultaneously, then wait for
    /// all echoes.
    MultiRr,
    /// Fire batches continuously without waiting for each echo.
    Stream,
    /// Keep `depth` requests in-flight at once before blocking on the oldest.
    Pipeline,
}

/// Read timeout on the blocking socket. Short enough that flow-control
/// spin loops don't waste wall-clock time, long enough that we don't
/// burn 100% CPU in a pure busy loop.
const READ_TIMEOUT: Duration = Duration::from_millis(1);
/// Give up on the handshake after this many seconds.
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);
/// Echo deadline per batch.
const ECHO_DEADLINE: Duration = Duration::from_secs(5);
/// Client-initiated bidirectional QUIC stream IDs for the 15 logical streams.
/// Formula: id = i * 4 (client bidi streams use ids 0, 4, 8, ...).
const STREAM_IDS: [u64; 15] = [0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56];
/// UDP datagram scratch buffer size.
const DGRAM_BUF: usize = 65_535;

/// Monotonic counter backing unique 8-byte SCIDs per worker.
static NEXT_SCID: AtomicU64 = AtomicU64::new(1);

/// Generates a fresh 8-byte source connection id (unique per worker).
fn gen_scid() -> Vec<u8> {
    let n = NEXT_SCID.fetch_add(1, Ordering::Relaxed);
    let mut bytes = vec![0u8; 8];
    bytes.copy_from_slice(&n.to_be_bytes());
    bytes
}

/// Computes the max coalesce buffer capacity needed (largest single frame × coalesce).
fn max_coalesce_capacity(entries: &[WorkloadEntry], coalesce: usize) -> usize {
    entries
        .iter()
        .map(|e| (frame::codec::HEADER_SIZE + e.payload_len) * coalesce)
        .max()
        .unwrap_or(64 * 1024)
}

/// Runs one connection's lifecycle. Returns a [`WorkerResult`] — never panics.
pub fn worker(
    idx: usize,
    target: SocketAddr,
    sni: &str,
    entries: &[WorkloadEntry],
    duration: Duration,
    mode: BenchMode,
    pipeline_depth: usize,
    coalesce: usize,
    shutdown: &AtomicBool,
    barrier: &Barrier,
) -> WorkerResult {
    let run_start = Instant::now();

    let socket = match bind_and_connect(target) {
        Ok(s) => s,
        Err(e) => {
            return WorkerResult::failed(format!("socket: {e}"), run_start.elapsed());
        }
    };

    let local = socket.local_addr().unwrap_or(target);
    let mut config = match build_client_config() {
        Ok(c) => c,
        Err(e) => {
            return WorkerResult::failed(format!("config: {e}"), run_start.elapsed());
        }
    };

    let scid = quiche::ConnectionId::from_vec(gen_scid());
    let mut conn = match quiche::connect(Some(sni), &scid, local, target, &mut config) {
        Ok(c) => c,
        Err(e) => {
            return WorkerResult::failed(format!("connect: {e}"), run_start.elapsed());
        }
    };

    let mut recv_buf = vec![0u8; DGRAM_BUF];
    let mut send_buf = vec![0u8; DGRAM_BUF];

    // --- Handshake -------------------------------------------------------
    let handshake_start = Instant::now();
    if let Err(e) = flush_outbound(&mut conn, &socket, &mut send_buf) {
        return WorkerResult::failed(format!("handshake send: {e}"), handshake_start.elapsed());
    }
    let handshake_ok = loop {
        if conn.is_established() {
            break true;
        }
        if conn.is_closed() {
            let reason = conn
                .peer_error()
                .map(|e| format!("{e:?}"))
                .or_else(|| conn.local_error().map(|e| format!("{e:?}")))
                .unwrap_or_else(|| "unknown".into());
            tracing::warn!(worker = idx, %reason, "connection closed during handshake");
            break false;
        }
        if handshake_start.elapsed() > HANDSHAKE_DEADLINE {
            break false;
        }
        if let Err(e) = recv_inbound(&mut conn, &socket, &mut recv_buf, target, local) {
            return WorkerResult::failed(format!("handshake recv: {e}"), handshake_start.elapsed());
        }
        if let Err(e) = flush_outbound(&mut conn, &socket, &mut send_buf) {
            return WorkerResult::failed(format!("handshake send: {e}"), handshake_start.elapsed());
        }
    };
    let handshake_time = handshake_start.elapsed();

    barrier.wait();

    if !handshake_ok {
        let reason = if conn.is_closed() {
            conn.peer_error()
                .map(|e| format!("peer closed: {e:?}"))
                .or_else(|| conn.local_error().map(|e| format!("local closed: {e:?}")))
                .unwrap_or_else(|| "connection closed".into())
        } else {
            format!(
                "handshake timed out after {}ms (server may not echo for this mode)",
                handshake_time.as_millis()
            )
        };
        return WorkerResult::failed(reason, handshake_time);
    }

    tracing::debug!(
        worker = idx,
        hs_us = handshake_time.as_micros(),
        "handshake ok"
    );

    // --- Create client layer stack --------------------------------------
    let mut stack = ClientStack::new(max_coalesce_capacity(entries, coalesce));
    if stack.on_connect().is_none() {
        return WorkerResult::failed("session table full".into(), handshake_time);
    }

    // --- Measurement (mode-dispatched) ----------------------------------
    let result = match mode {
        BenchMode::Rr => measure_rr(
            &mut stack,
            &mut conn,
            entries,
            duration,
            coalesce,
            shutdown,
            handshake_time,
            target,
            local,
            &socket,
            &mut recv_buf,
            &mut send_buf,
        ),
        BenchMode::MultiRr => measure_multi_rr(
            &mut stack,
            &mut conn,
            entries,
            duration,
            coalesce,
            shutdown,
            handshake_time,
            target,
            local,
            &socket,
            &mut recv_buf,
            &mut send_buf,
        ),
        BenchMode::Stream => measure_stream(
            &mut stack,
            &mut conn,
            entries,
            duration,
            coalesce,
            shutdown,
            handshake_time,
            target,
            local,
            &socket,
            &mut recv_buf,
            &mut send_buf,
        ),
        BenchMode::Pipeline => measure_pipeline(
            &mut stack,
            &mut conn,
            entries,
            duration,
            coalesce,
            shutdown,
            handshake_time,
            pipeline_depth,
            target,
            local,
            &socket,
            &mut recv_buf,
            &mut send_buf,
        ),
    };

    // --- Graceful close --------------------------------------------------
    let _ = stack.on_close();
    let _ = conn.close(true, 0x100, b"loadtest-done");
    let _ = flush_outbound(&mut conn, &socket, &mut send_buf);

    result
}

// ---- socket helpers -------------------------------------------------------

/// Binds a UDP socket to a wildcard address, connects it to `target`, and sets
/// the 25 ms read timeout.
fn bind_and_connect(target: SocketAddr) -> Result<UdpSocket> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect(target)?;
    socket.set_read_timeout(Some(READ_TIMEOUT))?;
    Ok(socket)
}

/// Drains all currently-available inbound datagrams into the connection.
fn recv_inbound(
    conn: &mut quiche::Connection,
    socket: &UdpSocket,
    buf: &mut [u8],
    peer: SocketAddr,
    local: SocketAddr,
) -> Result<()> {
    loop {
        match socket.recv(buf) {
            Ok(n) => {
                let info = quiche::RecvInfo {
                    from: peer,
                    to: local,
                };
                match conn.recv(&mut buf[..n], info) {
                    Ok(_) | Err(quiche::Error::Done) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(()),
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// Drains all outbound datagrams produced by quiche onto the socket.
fn flush_outbound(
    conn: &mut quiche::Connection,
    socket: &UdpSocket,
    buf: &mut [u8],
) -> Result<u64> {
    let mut syscalls = 0;
    loop {
        match conn.send(buf) {
            Ok((n, info)) => {
                send_with_gso(socket, &buf[..n], info.to, None)?;
                syscalls += 1;
            }
            Err(quiche::Error::Done) => return Ok(syscalls),
            Err(e) => return Err(e.into()),
        }
    }
}

// ---- shared send helper ---------------------------------------------------

/// Sends all entries through the layer stack — each on its own QUIC stream.
///
/// Each entry is encoded coalesced (COALESCE copies with incrementing seq)
/// via `stack.encode_frame_coalesced`, then sent on `STREAM_IDS[entry.logical_stream]`.
///
/// When `stream_send` returns `Error::Done` (flow-control blocked), this
/// function drains the receive side (including `stream_recv`) to free up
/// flow-control credit. Without draining receives, quiche's internal recv
/// buffer fills, it can't emit MAX_DATA/MAX_STREAM_DATA frames, and the
/// server's send side also blocks → circular deadlock.
///
/// Returns the total wire bytes sent. On deadline expiry, returns the
/// partial count (callers track milestones by cumulative byte count).
#[allow(clippy::too_many_arguments)]
fn send_through_stack(
    stack: &mut ClientStack,
    conn: &mut quiche::Connection,
    entries: &[WorkloadEntry],
    coalesce: usize,
    socket: &UdpSocket,
    recv_buf: &mut [u8],
    send_buf: &mut [u8],
    stream_buf: &mut [u8],
    encode_buf: &mut Vec<u8>,
    target: SocketAddr,
    local: SocketAddr,
    handshake_time: Duration,
    deadline: Instant,
) -> Result<u64, WorkerResult> {
    let mut bytes_sent = 0u64;
    for entry in entries {
        if Instant::now() >= deadline {
            return Ok(bytes_sent);
        }
        let quic_sid = STREAM_IDS[entry.logical_stream as usize];
        // Encode into caller-owned buffer — no heap allocation, no memset.
        // The borrow on `stack` is released immediately after this call.
        stack.encode_frame_coalesced_into(
            encode_buf,
            entry.logical_stream,
            entry.msg_type,
            entry.flags,
            &entry.payload,
            coalesce,
        );
        let mut off = 0usize;
        while off < encode_buf.len() {
            if Instant::now() >= deadline {
                bytes_sent += off as u64;
                return Ok(bytes_sent);
            }
            match conn.stream_send(quic_sid, &encode_buf[off..], false) {
                Ok(n) => {
                    if n == 0 {
                        let _ = flush_outbound(conn, socket, send_buf);
                        let _ = recv_inbound(conn, socket, recv_buf, target, local);
                        let _ = drain_recv_through_stack(stack, conn, stream_buf, STREAM_IDS.len());
                    }
                    off += n;
                }
                Err(quiche::Error::Done) => {
                    // Flow-control blocked — flush + recv + drain echo data
                    // through the stack (freeing flow-control credit AND
                    // counting received frames).
                    let _ = flush_outbound(conn, socket, send_buf);
                    let _ = recv_inbound(conn, socket, recv_buf, target, local);
                    let _ = drain_recv_through_stack(stack, conn, stream_buf, STREAM_IDS.len());
                }
                Err(e) => {
                    return Err(WorkerResult::failed(
                        format!("stream_send(sid={quic_sid}): {e}"),
                        handshake_time,
                    ));
                }
            }
        }
        bytes_sent += encode_buf.len() as u64;
    }
    Ok(bytes_sent)
}

/// Drains all QUIC streams through the layer stack's decode path.
///
/// Returns the total raw bytes received across all streams.
#[allow(clippy::too_many_arguments)]
fn drain_recv_through_stack(
    stack: &mut ClientStack,
    conn: &mut quiche::Connection,
    stream_buf: &mut [u8],
    num_streams: usize,
) -> u64 {
    let mut total_bytes = 0u64;
    for &sid in STREAM_IDS.iter().take(num_streams) {
        loop {
            match conn.stream_recv(sid, stream_buf) {
                Ok((0, _)) => break,
                Ok((n, _)) => {
                    total_bytes += n as u64;
                    stack.on_stream_data(sid, &stream_buf[..n]);
                }
                Err(quiche::Error::Done) => break,
                Err(_) => break,
            }
        }
    }
    total_bytes
}

// ---- measurement strategies -----------------------------------------------

/// **Mode 1 — Request-Response**: single stream 0, sequential send -> echo.
#[allow(clippy::too_many_arguments)]
fn measure_rr(
    stack: &mut ClientStack,
    conn: &mut quiche::Connection,
    entries: &[WorkloadEntry],
    duration: Duration,
    coalesce: usize,
    shutdown: &AtomicBool,
    handshake_time: Duration,
    target: SocketAddr,
    local: SocketAddr,
    socket: &UdpSocket,
    recv_buf: &mut [u8],
    send_buf: &mut [u8],
) -> WorkerResult {
    let batch_coalesced_bytes: u64 = entries
        .iter()
        .map(|e| (frame::codec::HEADER_SIZE + e.payload_len) as u64 * coalesce as u64)
        .sum();
    let mut stream_buf = vec![0u8; DGRAM_BUF];
    let mut encode_buf: Vec<u8> = Vec::with_capacity(DGRAM_BUF);
    let mut rtts = Vec::new();
    let mut bytes_sent = 0u64;
    let mut blocked_writes = 0u64;
    let mut flow_control_waits = 0u64;
    let mut write_syscalls = 0u64;
    let mut read_syscalls = 0u64;
    let start = Instant::now();

    while !shutdown.load(Ordering::Relaxed) && start.elapsed() < duration {
        if conn.is_closed() {
            break;
        }
        let t0 = Instant::now();

        // Send all entries on QUIC stream 0 (frame headers carry logical_stream 0..14).
        for entry in entries {
            stack.encode_frame_coalesced_into(
                &mut encode_buf,
                entry.logical_stream,
                entry.msg_type,
                entry.flags,
                &entry.payload,
                coalesce,
            );
            let mut off = 0usize;
            while off < encode_buf.len() {
                match conn.stream_send(STREAM_IDS[0], &encode_buf[off..], false) {
                    Ok(n) => {
                        write_syscalls += 1;
                        off += n;
                    }
                    Err(quiche::Error::Done) => {
                        blocked_writes += 1;
                        flow_control_waits += 1;
                        let _ = flush_outbound(conn, socket, send_buf);
                        read_syscalls += 1;
                        let _ = recv_inbound(conn, socket, recv_buf, target, local);
                        let _ = drain_recv_through_stack(stack, conn, &mut stream_buf, 1);
                    }
                    Err(e) => {
                        return WorkerResult::failed(
                            format!("rr stream_send: {e}"),
                            handshake_time,
                        );
                    }
                }
            }
            bytes_sent += encode_buf.len() as u64;
        }
        write_syscalls += 1;
        if let Err(e) = flush_outbound(conn, socket, send_buf) {
            return WorkerResult::failed(format!("rr flush: {e}"), handshake_time);
        }

        // Wait for echo.
        let mut recv_bytes = 0u64;
        let deadline = t0 + ECHO_DEADLINE;
        while recv_bytes < batch_coalesced_bytes {
            if Instant::now() > deadline {
                break;
            }
            read_syscalls += 1;
            let _ = recv_inbound(conn, socket, recv_buf, target, local);
            if let Ok(syscalls) = flush_outbound(conn, socket, send_buf) {
                write_syscalls += syscalls;
            }
            loop {
                match conn.stream_recv(STREAM_IDS[0], &mut stream_buf) {
                    Ok((0, _)) => break,
                    Ok((n, _)) => {
                        recv_bytes += n as u64;
                        stack.on_stream_data(STREAM_IDS[0], &stream_buf[..n]);
                    }
                    Err(quiche::Error::Done) => break,
                    Err(e) => {
                        return WorkerResult::failed(
                            format!("rr stream_recv: {e}"),
                            handshake_time,
                        );
                    }
                }
            }
        }

        if recv_bytes >= batch_coalesced_bytes {
            rtts.push(t0.elapsed());
        }
    }

    let rtt_count = rtts.len() as u64;
    WorkerResult::ok(
        rtts,
        stack.frames_tx(),
        bytes_sent,
        stack.frames_rx(),
        stack.bytes_rx(),
        1,
        rtt_count,
        rtt_count,
        blocked_writes,
        flow_control_waits,
        write_syscalls,
        read_syscalls,
        handshake_time,
    )
}

/// **Mode 2 — Multi-stream R-R**: 15 streams fired together, wait all echoes.
#[allow(clippy::too_many_arguments)]
fn measure_multi_rr(
    stack: &mut ClientStack,
    conn: &mut quiche::Connection,
    entries: &[WorkloadEntry],
    duration: Duration,
    coalesce: usize,
    shutdown: &AtomicBool,
    handshake_time: Duration,
    target: SocketAddr,
    local: SocketAddr,
    socket: &UdpSocket,
    recv_buf: &mut [u8],
    send_buf: &mut [u8],
) -> WorkerResult {
    let total_bytes: u64 = entries
        .iter()
        .map(|e| (frame::codec::HEADER_SIZE + e.payload_len) as u64 * coalesce as u64)
        .sum();
    let frames_per_batch = (entries.len() * coalesce) as u64;
    let mut stream_buf = vec![0u8; DGRAM_BUF];
    let mut encode_buf: Vec<u8> = Vec::with_capacity(DGRAM_BUF);
    let mut rtts = Vec::new();
    let mut bytes_sent = 0u64;
    let mut blocked_writes = 0u64;
    let mut flow_control_waits = 0u64;
    let mut write_syscalls = 0u64;
    let mut read_syscalls = 0u64;
    let start = Instant::now();
    let deadline = start + duration;

    while !shutdown.load(Ordering::Relaxed) && start.elapsed() < duration {
        if conn.is_closed() {
            break;
        }
        let t0 = Instant::now();

        // Send all entries through the stack (each on its own QUIC stream).
        match send_through_stack(
            stack,
            conn,
            entries,
            coalesce,
            socket,
            recv_buf,
            send_buf,
            &mut stream_buf,
            &mut encode_buf,
            target,
            local,
            handshake_time,
            deadline,
        ) {
            Ok(bs) => bytes_sent += bs,
            Err(wr) => return wr,
        }
        write_syscalls += entries.len() as u64 + 1;
        if let Err(e) = flush_outbound(conn, socket, send_buf) {
            return WorkerResult::failed(format!("multi-rr flush: {e}"), handshake_time);
        }

        // Wait for echo on all streams.
        let mut recv_bytes = 0u64;
        let echo_deadline = t0 + ECHO_DEADLINE;
        while recv_bytes < total_bytes {
            if Instant::now() > echo_deadline {
                blocked_writes += 1;
                flow_control_waits += 1;
                break;
            }
            read_syscalls += 1;
            let _ = recv_inbound(conn, socket, recv_buf, target, local);
            let _ = flush_outbound(conn, socket, send_buf);
            recv_bytes += drain_recv_through_stack(stack, conn, &mut stream_buf, entries.len());
        }

        if recv_bytes >= total_bytes {
            rtts.push(t0.elapsed());
        }
    }

    let rtt_count = rtts.len() as u64;
    WorkerResult::ok(
        rtts,
        stack.frames_tx(),
        bytes_sent,
        stack.frames_rx(),
        stack.bytes_rx(),
        frames_per_batch,
        frames_per_batch * rtt_count,
        rtt_count,
        blocked_writes,
        flow_control_waits,
        write_syscalls,
        read_syscalls,
        handshake_time,
    )
}

/// **Mode 3 — Full-duplex streaming**: fire continuously, drain echoes in same
/// poll loop.
#[allow(clippy::too_many_arguments)]
fn measure_stream(
    stack: &mut ClientStack,
    conn: &mut quiche::Connection,
    entries: &[WorkloadEntry],
    duration: Duration,
    coalesce: usize,
    shutdown: &AtomicBool,
    handshake_time: Duration,
    target: SocketAddr,
    local: SocketAddr,
    socket: &UdpSocket,
    recv_buf: &mut [u8],
    send_buf: &mut [u8],
) -> WorkerResult {
    let frames_per_batch = (entries.len() * coalesce) as u64;
    let mut stream_buf = vec![0u8; DGRAM_BUF];
    let mut encode_buf: Vec<u8> = Vec::with_capacity(DGRAM_BUF);
    let mut bytes_sent = 0u64;
    let mut blocked_writes = 0u64;
    let mut flow_control_waits = 0u64;
    let mut write_syscalls = 0u64;
    let mut read_syscalls = 0u64;
    let mut frames_in_flight: u64 = 0;
    let mut frames_in_flight_peak: u64 = 0;
    let mut queue_depth_sum = 0u64;
    let mut queue_depth_samples = 0u64;
    let start = Instant::now();

    while !shutdown.load(Ordering::Relaxed) && start.elapsed() < duration {
        if conn.is_closed() {
            break;
        }

        for entry in entries {
            let quic_sid = STREAM_IDS[entry.logical_stream as usize];
            stack.encode_frame_coalesced_into(
                &mut encode_buf,
                entry.logical_stream,
                entry.msg_type,
                entry.flags,
                &entry.payload,
                coalesce,
            );
            let mut off = 0usize;
            while off < encode_buf.len() {
                if shutdown.load(Ordering::Relaxed) || start.elapsed() >= duration {
                    break;
                }
                match conn.stream_send(quic_sid, &encode_buf[off..], false) {
                    Ok(n) => off += n,
                    Err(quiche::Error::Done) => {
                        blocked_writes += 1;
                        flow_control_waits += 1;
                        if let Ok(syscalls) = flush_outbound(conn, socket, send_buf) {
                            write_syscalls += syscalls;
                        }
                        read_syscalls += 1;
                        let _ = recv_inbound(conn, socket, recv_buf, target, local);
                        let _ =
                            drain_recv_through_stack(stack, conn, &mut stream_buf, entries.len());
                    }
                    Err(e) => {
                        return WorkerResult::failed(
                            format!("stream stream_send(sid={quic_sid}): {e}"),
                            handshake_time,
                        );
                    }
                }
            }
            bytes_sent += encode_buf.len() as u64;
        }
        if let Ok(syscalls) = flush_outbound(conn, socket, send_buf) {
            write_syscalls += syscalls;
        }
        frames_in_flight += frames_per_batch;
        if frames_in_flight > frames_in_flight_peak {
            frames_in_flight_peak = frames_in_flight;
        }
        queue_depth_sum += frames_in_flight;
        queue_depth_samples += 1;

        read_syscalls += 1;
        let _ = recv_inbound(conn, socket, recv_buf, target, local);
        let raw_recv = drain_recv_through_stack(stack, conn, &mut stream_buf, entries.len());
        if raw_recv > 0 {
            // Estimate frames recv'd from raw bytes (avg frame size).
            let avg_frame_size = entries
                .iter()
                .map(|e| frame::codec::HEADER_SIZE + e.payload_len)
                .sum::<usize>()
                / entries.len();
            if avg_frame_size > 0 {
                let est_frames = raw_recv / avg_frame_size as u64;
                frames_in_flight = frames_in_flight.saturating_sub(est_frames);
            }
        }
    }

    WorkerResult::ok(
        Vec::new(),
        stack.frames_tx(),
        bytes_sent,
        stack.frames_rx(),
        stack.bytes_rx(),
        frames_in_flight_peak,
        queue_depth_sum,
        queue_depth_samples,
        blocked_writes,
        flow_control_waits,
        write_syscalls,
        read_syscalls,
        handshake_time,
    )
}

/// **Mode 4 — Pipelined R-R**: keep `depth` batches in-flight at once.
#[allow(clippy::too_many_arguments)]
fn measure_pipeline(
    stack: &mut ClientStack,
    conn: &mut quiche::Connection,
    entries: &[WorkloadEntry],
    duration: Duration,
    coalesce: usize,
    shutdown: &AtomicBool,
    handshake_time: Duration,
    pipeline_depth: usize,
    target: SocketAddr,
    local: SocketAddr,
    socket: &UdpSocket,
    recv_buf: &mut [u8],
    send_buf: &mut [u8],
) -> WorkerResult {
    let frames_per_batch = (entries.len() * coalesce) as u64;
    // Milestone must match what stack.bytes_rx() counts — which only
    // includes data-plane frames (is_data() == true), not control frames
    // like Fragment, Ack, StreamOpen, etc.
    let batch_data_payload_bytes: u64 = entries
        .iter()
        .filter(|e| e.msg_type.is_data())
        .map(|e| e.payload_len as u64 * coalesce as u64)
        .sum();
    let depth = pipeline_depth.max(1);
    let mut in_flight: VecDeque<(Instant, u64)> = VecDeque::with_capacity(depth);
    let mut stream_buf = vec![0u8; DGRAM_BUF];
    let mut encode_buf: Vec<u8> = Vec::with_capacity(DGRAM_BUF);
    let mut rtts = Vec::new();
    let mut bytes_sent_total = 0u64;
    let mut payload_bytes_sent_total = 0u64;
    let mut bytes_recv_total = 0u64;
    let mut write_syscalls = 0u64;
    let mut read_syscalls = 0u64;
    let mut queue_depth_sum = 0u64;
    let mut queue_depth_samples = 0u64;
    let mut frames_in_flight_peak: u64 = 0;
    let start = Instant::now();
    let deadline = start + duration;

    while !shutdown.load(Ordering::Relaxed) && start.elapsed() < duration {
        if conn.is_closed() {
            break;
        }

        // Send ONE batch if the pipeline has room, then immediately drain.
        // This interleaves sends with receives so flow-control credit
        // from the server's echo is consumed promptly. (Previously a
        // `while` loop tried to fill all `depth` slots before draining,
        // which starved the receive side and stalled the connection.)
        if in_flight.len() < depth {
            match send_through_stack(
                stack,
                conn,
                entries,
                coalesce,
                socket,
                recv_buf,
                send_buf,
                &mut stream_buf,
                &mut encode_buf,
                target,
                local,
                handshake_time,
                deadline,
            ) {
                Ok(bs) => {
                    bytes_sent_total += bs;
                }
                Err(wr) => return wr,
            }
            write_syscalls += entries.len() as u64 + 1;
            if let Err(e) = flush_outbound(conn, socket, send_buf) {
                return WorkerResult::failed(format!("pipeline flush: {e}"), handshake_time);
            }
            payload_bytes_sent_total += batch_data_payload_bytes;
            in_flight.push_back((Instant::now(), payload_bytes_sent_total));
            let cur_in_flight = in_flight.len() as u64 * frames_per_batch;
            if cur_in_flight > frames_in_flight_peak {
                frames_in_flight_peak = cur_in_flight;
            }
        }

        // Drain phase — runs EVERY iteration (not just when pipeline is full).
        read_syscalls += 1;
        let _ = recv_inbound(conn, socket, recv_buf, target, local);
        write_syscalls += 1;
        let _ = flush_outbound(conn, socket, send_buf);
        // Use stack.bytes_rx() as the cumulative counter so that echo
        // bytes consumed during the send phase (inside send_through_stack's
        // flow-control path) are also counted toward bytes_recv_total.
        let _ = drain_recv_through_stack(stack, conn, &mut stream_buf, entries.len());
        bytes_recv_total = stack.bytes_rx();

        queue_depth_sum += in_flight.len() as u64;
        queue_depth_samples += 1;

        while let Some(&(t_send, milestone)) = in_flight.front() {
            if bytes_recv_total >= milestone {
                rtts.push(t_send.elapsed());
                in_flight.pop_front();
            } else {
                break;
            }
        }
    }

    WorkerResult::ok(
        rtts,
        stack.frames_tx(),
        bytes_sent_total,
        stack.frames_rx(),
        stack.bytes_rx(),
        frames_in_flight_peak,
        queue_depth_sum,
        queue_depth_samples,
        0,
        0,
        write_syscalls,
        read_syscalls,
        handshake_time,
    )
}

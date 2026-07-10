//! Per-connection load-test driver.
//!
//! Supports four benchmark modes, selected via [`BenchMode`]:
//!
//! | Mode | Description |
//! |---|---|
//! | `Rr` | **Request-Response** — 1 stream, send batch -> wait echo -> repeat. |
//! | `MultiRr` | **Multi-stream R-R** — 15 streams fired concurrently, wait all echoes. |
//! | `Stream` | **Full-duplex streaming** — fire continuously without per-batch wait. |
//! | `Pipeline` | **Pipelined R-R** — keep N requests in-flight before waiting. |

use std::collections::VecDeque;
use std::io::ErrorKind;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::config::build_client_config;
use crate::gso::send_with_gso;
use crate::stats::WorkerResult;
use crate::workload::COALESCE;

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

/// Read timeout on the blocking socket.
const READ_TIMEOUT: Duration = Duration::from_millis(25);
/// Give up on the handshake after this many seconds.
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);
/// Echo deadline per batch.
const ECHO_DEADLINE: Duration = Duration::from_secs(5);
/// Client-initiated bidirectional QUIC stream IDs for the 15 logical streams.
/// Formula: id = i * 4 (client bidi streams use ids 0, 4, 8, ...).
const STREAM_IDS: [u64; 15] = [0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56];
/// UDP datagram scratch buffer size.
const DGRAM_BUF: usize = 65535;

/// Monotonic counter backing unique 8-byte SCIDs per worker.
static NEXT_SCID: AtomicU64 = AtomicU64::new(1);

/// Generates a fresh 8-byte source connection id (unique per worker).
fn gen_scid() -> Vec<u8> {
    let n = NEXT_SCID.fetch_add(1, Ordering::Relaxed);
    let mut bytes = vec![0u8; 8];
    bytes.copy_from_slice(&n.to_be_bytes());
    bytes
}

/// Runs one connection's lifecycle. Returns a [`WorkerResult`] — never panics.
#[allow(clippy::too_many_arguments)]
pub fn worker(
    idx: usize,
    target: SocketAddr,
    sni: &str,
    frames: &[Vec<u8>],
    thick_frames: &[Vec<u8>],
    thick_batch: &[u8],
    duration: Duration,
    mode: BenchMode,
    pipeline_depth: usize,
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

    // --- Measurement (mode-dispatched) ----------------------------------
    let result = match mode {
        BenchMode::Rr => measure_rr(
            &mut conn,
            thick_batch,
            frames.len() as u64,
            duration,
            shutdown,
            handshake_time,
            target,
            local,
            &socket,
            &mut recv_buf,
            &mut send_buf,
        ),
        BenchMode::MultiRr => measure_multi_rr(
            idx,
            &mut conn,
            frames,
            thick_frames,
            duration,
            shutdown,
            handshake_time,
            target,
            local,
            &socket,
            &mut recv_buf,
            &mut send_buf,
        ),
        BenchMode::Stream => measure_stream(
            &mut conn,
            frames,
            thick_frames,
            duration,
            shutdown,
            handshake_time,
            target,
            local,
            &socket,
            &mut recv_buf,
            &mut send_buf,
        ),
        BenchMode::Pipeline => measure_pipeline(
            &mut conn,
            frames,
            thick_frames,
            duration,
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

/// Sends all frames — one `stream_send` per stream using the pre-coalesced
/// `thick_frames` buffer.
#[allow(clippy::too_many_arguments)]
fn send_frames_multi(
    conn: &mut quiche::Connection,
    thick_frames: &[Vec<u8>],
    socket: &UdpSocket,
    recv_buf: &mut [u8],
    send_buf: &mut [u8],
    target: SocketAddr,
    local: SocketAddr,
    handshake_time: Duration,
    deadline: Instant,
) -> Result<(), WorkerResult> {
    for (i, frame) in thick_frames.iter().enumerate() {
        let sid = STREAM_IDS[i];
        let mut off = 0usize;
        while off < frame.len() {
            if Instant::now() >= deadline {
                return Ok(());
            }
            match conn.stream_send(sid, &frame[off..], false) {
                Ok(n) => off += n,
                Err(quiche::Error::Done) => {
                    let _ = flush_outbound(conn, socket, send_buf);
                    let _ = recv_inbound(conn, socket, recv_buf, target, local);
                }
                Err(e) => {
                    return Err(WorkerResult::failed(
                        format!("stream_send(sid={sid}): {e}"),
                        handshake_time,
                    ));
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn wait_echo_multi(
    idx: usize,
    conn: &mut quiche::Connection,
    thick_frames: &[Vec<u8>],
    socket: &UdpSocket,
    recv_buf: &mut [u8],
    send_buf: &mut [u8],
    target: SocketAddr,
    local: SocketAddr,
    handshake_time: Duration,
) -> Result<bool, WorkerResult> {
    let mut stream_buf = vec![0u8; DGRAM_BUF];
    let mut received = vec![0usize; thick_frames.len()];
    let deadline = Instant::now() + ECHO_DEADLINE;

    while received
        .iter()
        .zip(thick_frames.iter())
        .any(|(r, f)| *r < f.len())
    {
        if Instant::now() > deadline {
            let pending = received
                .iter()
                .zip(thick_frames.iter())
                .filter(|(r, f)| **r < f.len())
                .count();
            tracing::warn!(worker = idx, pending_streams = pending, "echo timeout");
            return Ok(false);
        }
        let _ = recv_inbound(conn, socket, recv_buf, target, local);
        let _ = flush_outbound(conn, socket, send_buf);

        for (i, r) in received.iter_mut().enumerate() {
            if *r >= thick_frames[i].len() {
                continue;
            }
            let sid = STREAM_IDS[i];
            loop {
                match conn.stream_recv(sid, &mut stream_buf) {
                    Ok((n, _fin)) => {
                        *r += n;
                        if n == 0 {
                            break;
                        }
                    }
                    Err(quiche::Error::Done) => break,
                    Err(e) => {
                        return Err(WorkerResult::failed(
                            format!("stream_recv(sid={sid}): {e}"),
                            handshake_time,
                        ));
                    }
                }
            }
        }
    }
    Ok(true)
}

// ---- measurement strategies -----------------------------------------------

/// **Mode 1 — Request-Response**: single stream 0, sequential send -> echo.
#[allow(clippy::too_many_arguments)]
fn measure_rr(
    conn: &mut quiche::Connection,
    thick_batch: &[u8],
    frames_per_coalesced_batch: u64,
    duration: Duration,
    shutdown: &AtomicBool,
    handshake_time: Duration,
    target: SocketAddr,
    local: SocketAddr,
    socket: &UdpSocket,
    recv_buf: &mut [u8],
    send_buf: &mut [u8],
) -> WorkerResult {
    let batch_len = thick_batch.len();
    let frames_per_batch = frames_per_coalesced_batch * COALESCE as u64;
    let mut stream_buf = vec![0u8; DGRAM_BUF];
    let mut rtts = Vec::new();
    let mut frames_sent = 0u64;
    let mut bytes_sent = 0u64;
    let mut frames_recv = 0u64;
    let mut bytes_recv = 0u64;
    let mut blocked_writes = 0u64;
    let mut flow_control_waits = 0u64;
    let mut write_syscalls = 0u64;
    let mut read_syscalls = 0u64;
    let mut queue_depth_sum = 0u64;
    let mut queue_depth_samples = 0u64;
    let start = Instant::now();

    while !shutdown.load(Ordering::Relaxed) && start.elapsed() < duration {
        if conn.is_closed() {
            break;
        }
        let t0 = Instant::now();

        let mut off = 0;
        while off < batch_len {
            match conn.stream_send(STREAM_IDS[0], &thick_batch[off..], false) {
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
                }
                Err(e) => {
                    return WorkerResult::failed(format!("rr stream_send: {e}"), handshake_time);
                }
            }
        }
        write_syscalls += 1;
        if let Err(e) = flush_outbound(conn, socket, send_buf) {
            return WorkerResult::failed(format!("rr flush: {e}"), handshake_time);
        }

        let mut received = 0usize;
        let deadline = t0 + ECHO_DEADLINE;
        while received < batch_len {
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
                    Ok((n, _fin)) => {
                        received += n;
                        bytes_recv += n as u64;
                        if n == 0 {
                            break;
                        }
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

        if received >= batch_len {
            rtts.push(t0.elapsed());
            frames_sent += frames_per_batch;
            bytes_sent += batch_len as u64;
            frames_recv += frames_per_batch;
            queue_depth_sum += 1;
            queue_depth_samples += 1;
        }
    }
    WorkerResult::ok(
        rtts,
        frames_sent,
        bytes_sent,
        frames_recv,
        bytes_recv,
        1,
        queue_depth_sum,
        queue_depth_samples,
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
    idx: usize,
    conn: &mut quiche::Connection,
    frames: &[Vec<u8>],
    thick_frames: &[Vec<u8>],
    duration: Duration,
    shutdown: &AtomicBool,
    handshake_time: Duration,
    target: SocketAddr,
    local: SocketAddr,
    socket: &UdpSocket,
    recv_buf: &mut [u8],
    send_buf: &mut [u8],
) -> WorkerResult {
    let total_bytes: usize = thick_frames.iter().map(|f| f.len()).sum();
    let frames_per_batch = (frames.len() * COALESCE) as u64;
    let mut rtts = Vec::new();
    let mut frames_sent = 0u64;
    let mut bytes_sent = 0u64;
    let mut frames_recv = 0u64;
    let mut bytes_recv = 0u64;
    let mut blocked_writes = 0u64;
    let mut flow_control_waits = 0u64;
    let mut write_syscalls = 0u64;
    let mut read_syscalls = 0u64;
    let mut queue_depth_sum = 0u64;
    let mut queue_depth_samples = 0u64;
    let start = Instant::now();
    let deadline = start + duration;

    while !shutdown.load(Ordering::Relaxed) && start.elapsed() < duration {
        if conn.is_closed() {
            break;
        }
        let t0 = Instant::now();

        if let Err(wr) = send_frames_multi(
            conn,
            thick_frames,
            socket,
            recv_buf,
            send_buf,
            target,
            local,
            handshake_time,
            deadline,
        ) {
            return wr;
        }
        write_syscalls += frames.len() as u64 + 1;
        if let Err(e) = flush_outbound(conn, socket, send_buf) {
            return WorkerResult::failed(format!("multi-rr flush: {e}"), handshake_time);
        }

        match wait_echo_multi(
            idx,
            conn,
            thick_frames,
            socket,
            recv_buf,
            send_buf,
            target,
            local,
            handshake_time,
        ) {
            Ok(true) => {
                rtts.push(t0.elapsed());
                frames_sent += frames_per_batch;
                bytes_sent += total_bytes as u64;
                frames_recv += frames_per_batch;
                bytes_recv += total_bytes as u64;
                read_syscalls += 1;
                queue_depth_sum += frames_per_batch;
                queue_depth_samples += 1;
            }
            Ok(false) => {
                blocked_writes += 1;
                flow_control_waits += 1;
            }
            Err(wr) => return wr,
        }
    }
    WorkerResult::ok(
        rtts,
        frames_sent,
        bytes_sent,
        frames_recv,
        bytes_recv,
        frames_per_batch,
        queue_depth_sum,
        queue_depth_samples,
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
    conn: &mut quiche::Connection,
    frames: &[Vec<u8>],
    thick_frames: &[Vec<u8>],
    duration: Duration,
    shutdown: &AtomicBool,
    handshake_time: Duration,
    target: SocketAddr,
    local: SocketAddr,
    socket: &UdpSocket,
    recv_buf: &mut [u8],
    send_buf: &mut [u8],
) -> WorkerResult {
    let total_bytes: usize = thick_frames.iter().map(|f| f.len()).sum();
    let frames_per_batch = (frames.len() * COALESCE) as u64;
    let mut stream_buf = vec![0u8; DGRAM_BUF];
    let mut frames_sent = 0u64;
    let mut bytes_sent = 0u64;
    let mut frames_recv = 0u64;
    let mut bytes_recv = 0u64;
    let mut blocked_writes = 0u64;
    let mut flow_control_waits = 0u64;
    let mut write_syscalls = 0u64;
    let mut read_syscalls = 0u64;
    let mut queue_depth_sum = 0u64;
    let mut queue_depth_samples = 0u64;
    let mut frames_in_flight: u64 = 0;
    let mut frames_in_flight_peak: u64 = 0;
    let start = Instant::now();

    while !shutdown.load(Ordering::Relaxed) && start.elapsed() < duration {
        if conn.is_closed() {
            break;
        }

        for (i, frame) in thick_frames.iter().enumerate() {
            let sid = STREAM_IDS[i];
            let mut off = 0usize;
            while off < frame.len() {
                if shutdown.load(Ordering::Relaxed) || start.elapsed() >= duration {
                    break;
                }
                match conn.stream_send(sid, &frame[off..], false) {
                    Ok(n) => off += n,
                    Err(quiche::Error::Done) => {
                        blocked_writes += 1;
                        flow_control_waits += 1;
                        if let Ok(syscalls) = flush_outbound(conn, socket, send_buf) {
                            write_syscalls += syscalls;
                        }
                        read_syscalls += 1;
                        let _ = recv_inbound(conn, socket, recv_buf, target, local);
                    }
                    Err(e) => {
                        return WorkerResult::failed(
                            format!("stream stream_send(sid={sid}): {e}"),
                            handshake_time,
                        );
                    }
                }
            }
        }
        if let Ok(syscalls) = flush_outbound(conn, socket, send_buf) {
            write_syscalls += syscalls;
        }
        frames_sent += frames_per_batch;
        bytes_sent += total_bytes as u64;
        frames_in_flight += frames_per_batch;
        if frames_in_flight > frames_in_flight_peak {
            frames_in_flight_peak = frames_in_flight;
        }
        queue_depth_sum += frames_in_flight;
        queue_depth_samples += 1;

        read_syscalls += 1;
        let _ = recv_inbound(conn, socket, recv_buf, target, local);
        for &sid in STREAM_IDS.iter().take(frames.len()) {
            while let Ok((n, _)) = conn.stream_recv(sid, &mut stream_buf) {
                bytes_recv += n as u64;
                if n > 0 {
                    frames_recv += 1;
                    frames_in_flight = frames_in_flight.saturating_sub(1);
                }
            }
        }
    }
    WorkerResult::ok(
        Vec::new(),
        frames_sent,
        bytes_sent,
        frames_recv,
        bytes_recv,
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
    conn: &mut quiche::Connection,
    frames: &[Vec<u8>],
    thick_frames: &[Vec<u8>],
    duration: Duration,
    shutdown: &AtomicBool,
    handshake_time: Duration,
    pipeline_depth: usize,
    target: SocketAddr,
    local: SocketAddr,
    socket: &UdpSocket,
    recv_buf: &mut [u8],
    send_buf: &mut [u8],
) -> WorkerResult {
    let total_bytes: usize = thick_frames.iter().map(|f| f.len()).sum();
    let frames_per_batch = (frames.len() * COALESCE) as u64;
    let depth = pipeline_depth.max(1);
    let mut in_flight: VecDeque<(Instant, u64)> = VecDeque::with_capacity(depth);
    let mut stream_buf = vec![0u8; DGRAM_BUF];
    let mut rtts = Vec::new();
    let mut frames_sent = 0u64;
    let mut bytes_sent_total = 0u64;
    let mut bytes_recv_total = 0u64;
    let mut frames_recv = 0u64;
    let blocked_writes = 0u64;
    let flow_control_waits = 0u64;
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

        while in_flight.len() < depth {
            if let Err(wr) = send_frames_multi(
                conn,
                thick_frames,
                socket,
                recv_buf,
                send_buf,
                target,
                local,
                handshake_time,
                deadline,
            ) {
                return wr;
            }
            write_syscalls += frames.len() as u64 + 1;
            if let Err(e) = flush_outbound(conn, socket, send_buf) {
                return WorkerResult::failed(format!("pipeline flush: {e}"), handshake_time);
            }
            bytes_sent_total += total_bytes as u64;
            frames_sent += frames_per_batch;
            in_flight.push_back((Instant::now(), bytes_sent_total));
            let cur_in_flight = in_flight.len() as u64 * frames_per_batch;
            if cur_in_flight > frames_in_flight_peak {
                frames_in_flight_peak = cur_in_flight;
            }
            queue_depth_sum += in_flight.len() as u64;
            queue_depth_samples += 1;
        }

        read_syscalls += 1;
        let _ = recv_inbound(conn, socket, recv_buf, target, local);
        write_syscalls += 1;
        let _ = flush_outbound(conn, socket, send_buf);
        for &sid in STREAM_IDS.iter().take(frames.len()) {
            while let Ok((n, _)) = conn.stream_recv(sid, &mut stream_buf) {
                bytes_recv_total += n as u64;
                if n > 0 {
                    frames_recv += 1;
                }
            }
        }

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
        frames_sent,
        bytes_sent_total,
        frames_recv,
        bytes_recv_total,
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

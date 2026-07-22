//! Per-connection state for the mio event-driven reactor.
//!
//! Each [`ConnState`] owns one QUIC connection (quiche), its UDP socket
//! (registered with the reactor's mio::Poll), the client layer stack, and
//! per-connection buffers. The reactor calls three methods per iteration:
//!
//! 1. [`ConnState::recv_inbound`] — drain inbound datagrams into quiche
//! 2. [`ConnState::drain_and_process`] — drain stream data + update mode state
//! 3. [`ConnState::tick`] — mode-dispatched send logic
//! 4. [`ConnState::flush_outbound`] — push quiche's send queue onto the socket

use std::collections::VecDeque;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::time::{Duration, Instant};

use anyhow::Result;
use mio::net::UdpSocket;

use crate::gso::send_with_gso;
use crate::layers::ClientStack;
use crate::stats::WorkerResult;
use crate::workload::WorkloadEntry;

/// Client-initiated bidirectional QUIC stream IDs.
pub const STREAM_IDS: [u64; 15] = [0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56];

/// Number of distinct QUIC streams — exposed for workload mapping.
pub(crate) const STREAM_ID_COUNT: usize = STREAM_IDS.len();

/// UDP datagram scratch buffer size.
const DGRAM_BUF: usize = 65_535;
/// Echo deadline per batch.
const ECHO_DEADLINE: Duration = Duration::from_secs(5);

/// Monotonic counter backing unique 8-byte SCIDs per connection.
static NEXT_SCID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn gen_scid() -> quiche::ConnectionId<'static> {
    let n = NEXT_SCID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    quiche::ConnectionId::from_vec(n.to_be_bytes().to_vec())
}

// ── Enums ──────────────────────────────────────────────────────────

/// Selects the measurement strategy.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BenchMode {
    Rr,
    MultiRr,
    Stream,
    Pipeline,
}

/// Client-side send policy.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EgressPolicy {
    None,
    ReliableOrdered,
    ReliableUnordered,
    RealtimeDropOld,
    LatestOnly,
}

impl EgressPolicy {
    #[must_use]
    pub const fn to_delivery_policy(self) -> Option<send_policy::DeliveryPolicy> {
        match self {
            Self::None => None,
            Self::ReliableOrdered => Some(send_policy::DeliveryPolicy::ReliableOrdered),
            Self::ReliableUnordered => Some(send_policy::DeliveryPolicy::ReliableUnordered),
            Self::RealtimeDropOld => Some(send_policy::DeliveryPolicy::RealtimeDropOld),
            Self::LatestOnly => Some(send_policy::DeliveryPolicy::LatestOnly),
        }
    }
}

/// Connection lifecycle phase.
#[derive(Clone, Copy)]
pub(crate) enum Phase {
    Handshake,
    Ready,
    Measuring { deadline: Instant },
    Done,
}

/// Send cursor — tracks which entry is being sent and how far into its
/// encoded buffer we've pushed via `stream_send`.
#[derive(Default)]
pub(crate) struct SendCursor {
    entry_idx: usize,
    send_off: usize,
    encoded: bool,
}

/// Free-function flush — callable while holding `&mut self.ms` because it
/// only borrows the individual fields it needs (disjoint from `self.ms`).
fn flush_conn(conn: &mut quiche::Connection, send_buf: &mut [u8], fd: RawFd) -> u64 {
    let mut syscalls = 0u64;
    loop {
        match conn.send(send_buf) {
            Ok((n, info)) => {
                let _ = send_with_gso(fd, &send_buf[..n], info.to, None);
                syscalls += 1;
            }
            Err(quiche::Error::Done) => break,
            Err(_) => break,
        }
    }
    syscalls
}

/// Mode-specific mutable state.
pub(crate) enum ModeState {
    Rr {
        cursor: SendCursor,
        waiting: bool,
        recv_bytes: u64,
        batch_target: u64,
        batch_start: Option<Instant>,
    },
    MultiRr {
        cursor: SendCursor,
        waiting: bool,
        recv_bytes: u64,
        batch_target: u64,
        batch_start: Option<Instant>,
    },
    Stream {
        cursor: SendCursor,
        frames_in_flight: u64,
    },
    Pipeline {
        cursor: SendCursor,
        sending: bool,
        in_flight: VecDeque<(Instant, u64)>,
        payload_sent: u64,
    },
}

// ── ConnState ──────────────────────────────────────────────────────

/// Per-connection state — owned by the reactor, indexed by mio Token.
pub(crate) struct ConnState {
    socket: UdpSocket,
    pub conn: quiche::Connection,
    local: SocketAddr,
    target: SocketAddr,

    stack: ClientStack,

    recv_buf: Vec<u8>,
    send_buf: Vec<u8>,
    stream_buf: Vec<u8>,
    encode_buf: Vec<u8>,

    pub phase: Phase,
    handshake_start: Instant,
    handshake_time: Duration,

    // Precomputed workload constants
    frames_per_batch: u64,
    batch_data_payload_bytes: u64,
    avg_frame_size: u64,

    // Stats
    rtts: Vec<Duration>,
    bytes_sent: u64,
    blocked_writes: u64,
    flow_control_waits: u64,
    write_syscalls: u64,
    read_syscalls: u64,
    frames_in_flight_peak: u64,
    queue_depth_sum: u64,
    queue_depth_samples: u64,

    error: Option<String>,

    mode: BenchMode,
    pipeline_depth: usize,
    ms: ModeState,
}

impl ConnState {
    /// Creates a new connection: binds a non-blocking UDP socket, creates
    /// the quiche connection, registers with mio, and initialises the
    /// layer stack + mode state.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        target: SocketAddr,
        sni: &str,
        config: &mut quiche::Config,
        mode: BenchMode,
        pipeline_depth: usize,
        egress_policy: EgressPolicy,
        entries: &[WorkloadEntry],
        coalesce: usize,
        poll: &mio::Poll,
        token: mio::Token,
    ) -> Result<Self> {
        let std_sock = std::net::UdpSocket::bind("0.0.0.0:0")?;
        std_sock.set_nonblocking(true)?;
        std_sock.connect(target)?;
        let local = std_sock.local_addr()?;
        let mut socket = UdpSocket::from_std(std_sock);

        poll.registry()
            .register(&mut socket, token, mio::Interest::READABLE)?;

        let scid = gen_scid();
        let conn = quiche::connect(Some(sni), &scid, local, target, config)?;

        let coalesce_cap = entries
            .iter()
            .map(|e| (frame::codec::HEADER_SIZE + e.payload_len) * coalesce)
            .max()
            .unwrap_or(DGRAM_BUF);
        let mut stack = ClientStack::new(coalesce_cap);
        anyhow::ensure!(stack.on_connect().is_some(), "session table full");

        if let Some(policy) = egress_policy.to_delivery_policy() {
            for (i, _) in entries.iter().enumerate() {
                stack.bind_send_policy(i as u64, policy);
            }
        }

        let frames_per_batch = (entries.len() * coalesce) as u64;
        let batch_data_payload_bytes = entries
            .iter()
            .filter(|e| e.msg_type.is_data())
            .map(|e| e.payload_len as u64 * coalesce as u64)
            .sum();
        let total_wire: usize = entries
            .iter()
            .map(|e| (frame::codec::HEADER_SIZE + e.payload_len) * coalesce)
            .sum();
        let avg_frame_size = if entries.is_empty() {
            1
        } else {
            (total_wire / entries.len()) as u64
        };

        let batch_target: u64 = entries
            .iter()
            .map(|e| (frame::codec::HEADER_SIZE + e.payload_len) as u64 * coalesce as u64)
            .sum();

        let ms = match mode {
            BenchMode::Rr => ModeState::Rr {
                cursor: SendCursor::default(),
                waiting: false,
                recv_bytes: 0,
                batch_target,
                batch_start: None,
            },
            BenchMode::MultiRr => ModeState::MultiRr {
                cursor: SendCursor::default(),
                waiting: false,
                recv_bytes: 0,
                batch_target,
                batch_start: None,
            },
            BenchMode::Stream => ModeState::Stream {
                cursor: SendCursor::default(),
                frames_in_flight: 0,
            },
            BenchMode::Pipeline => ModeState::Pipeline {
                cursor: SendCursor::default(),
                sending: false,
                in_flight: VecDeque::with_capacity(pipeline_depth.max(1)),
                payload_sent: 0,
            },
        };

        Ok(Self {
            socket,
            conn,
            local,
            target,
            stack,
            recv_buf: vec![0u8; DGRAM_BUF],
            send_buf: vec![0u8; DGRAM_BUF],
            stream_buf: vec![0u8; DGRAM_BUF],
            encode_buf: Vec::with_capacity(DGRAM_BUF),
            phase: Phase::Handshake,
            handshake_start: Instant::now(),
            handshake_time: Duration::ZERO,
            frames_per_batch,
            batch_data_payload_bytes,
            avg_frame_size,
            rtts: Vec::new(),
            bytes_sent: 0,
            blocked_writes: 0,
            flow_control_waits: 0,
            write_syscalls: 0,
            read_syscalls: 0,
            frames_in_flight_peak: 0,
            queue_depth_sum: 0,
            queue_depth_samples: 0,
            error: None,
            mode,
            pipeline_depth,
            ms,
        })
    }

    pub fn is_established(&self) -> bool {
        self.conn.is_established()
    }

    pub fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }

    pub fn handshake_done(&mut self) {
        self.handshake_time = self.handshake_start.elapsed();
    }

    pub fn handshake_timeout(&mut self) {
        self.handshake_time = self.handshake_start.elapsed();
    }

    // ── I/O ─────────────────────────────────────────────────────────

    /// Drains readable datagrams from the socket into quiche.
    pub fn recv_inbound(&mut self) {
        self.read_syscalls += 1;
        loop {
            match self.socket.recv(&mut self.recv_buf) {
                Ok(n) => {
                    let info = quiche::RecvInfo {
                        from: self.target,
                        to: self.local,
                    };
                    let _ = self.conn.recv(&mut self.recv_buf[..n], info);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => return,
            }
        }
    }

    /// Drains quiche's outbound queue onto the socket.
    pub fn flush_outbound(&mut self) -> u64 {
        let syscalls = flush_conn(
            &mut self.conn,
            &mut self.send_buf,
            self.socket.as_raw_fd(),
        );
        self.write_syscalls += syscalls;
        syscalls
    }

    /// Drains all QUIC streams through the layer stack's decode path,
    /// then updates mode-specific recv state.
    pub fn drain_and_process(&mut self, entries_len: usize) {
        let num_streams = match self.mode {
            BenchMode::Rr => 1,
            _ => entries_len.min(STREAM_IDS.len()),
        };
        let mut raw_recv = 0u64;
        for &sid in STREAM_IDS.iter().take(num_streams) {
            loop {
                match self.conn.stream_recv(sid, &mut self.stream_buf) {
                    Ok((0, _)) => break,
                    Ok((n, _)) => {
                        raw_recv += n as u64;
                        self.stack.on_stream_data(sid, &self.stream_buf[..n]);
                    }
                    Err(quiche::Error::Done) => break,
                    Err(_) => break,
                }
            }
        }

        // Mode-specific recv processing
        match &mut self.ms {
            ModeState::Rr {
                waiting,
                recv_bytes,
                batch_target,
                batch_start,
                ..
            } if *waiting => {
                *recv_bytes += raw_recv;
                if *recv_bytes >= *batch_target {
                    if let Some(t0) = batch_start.take() {
                        self.rtts.push(t0.elapsed());
                    }
                    *waiting = false;
                } else if let Some(t0) = *batch_start {
                    if t0.elapsed() > ECHO_DEADLINE {
                        *waiting = false;
                    }
                }
            }
            ModeState::MultiRr {
                waiting,
                recv_bytes,
                batch_target,
                batch_start,
                ..
            } if *waiting => {
                *recv_bytes += raw_recv;
                if *recv_bytes >= *batch_target {
                    if let Some(t0) = batch_start.take() {
                        self.rtts.push(t0.elapsed());
                    }
                    *waiting = false;
                } else if let Some(t0) = *batch_start {
                    if t0.elapsed() > ECHO_DEADLINE {
                        *waiting = false;
                    }
                }
            }
            ModeState::Stream { frames_in_flight, .. } => {
                if raw_recv > 0 && self.avg_frame_size > 0 {
                    let est = raw_recv / self.avg_frame_size;
                    *frames_in_flight = frames_in_flight.saturating_sub(est);
                }
            }
            ModeState::Pipeline { in_flight, .. } => {
                let bytes_recv_total = self.stack.bytes_rx();
                while let Some(&(t_send, milestone)) = in_flight.front() {
                    if bytes_recv_total >= milestone {
                        self.rtts.push(t_send.elapsed());
                        in_flight.pop_front();
                    } else {
                        break;
                    }
                }
            }
            _ => {}
        }
    }

    // ── Tick ────────────────────────────────────────────────────────

    /// Mode-dispatched send logic.
    pub fn tick(&mut self, entries: &[WorkloadEntry], coalesce: usize, deadline: Instant) {
        match self.mode {
            BenchMode::Rr => self.tick_rr(entries, coalesce, deadline),
            BenchMode::MultiRr => self.tick_multi_rr(entries, coalesce, deadline),
            BenchMode::Stream => self.tick_stream(entries, coalesce, deadline),
            BenchMode::Pipeline => self.tick_pipeline(entries, coalesce, deadline),
        }
    }

    fn tick_rr(&mut self, entries: &[WorkloadEntry], coalesce: usize, deadline: Instant) {
        let ModeState::Rr {
            cursor,
            waiting,
            recv_bytes,
            batch_target: _,
            batch_start,
        } = &mut self.ms
        else {
            return;
        };
        if *waiting {
            return;
        }
        while cursor.entry_idx < entries.len() {
            if Instant::now() >= deadline {
                return;
            }
            let entry = &entries[cursor.entry_idx];
            if !cursor.encoded {
                self.stack.encode_frame_coalesced_into(
                    &mut self.encode_buf,
                    entry.logical_stream,
                    entry.msg_type,
                    entry.flags,
                    &entry.payload,
                    coalesce,
                );
                cursor.send_off = 0;
                cursor.encoded = true;
            }
            let sid = STREAM_IDS[0];
            let mut blocked = false;
            while cursor.send_off < self.encode_buf.len() {
                match self
                    .conn
                    .stream_send(sid, &self.encode_buf[cursor.send_off..], false)
                {
                    Ok(0) => {
                        blocked = true;
                        break;
                    }
                    Ok(n) => cursor.send_off += n,
                    Err(quiche::Error::Done) => {
                        self.blocked_writes += 1;
                        self.flow_control_waits += 1;
                        blocked = true;
                        break;
                    }
                    Err(e) => {
                        self.error = Some(format!("rr stream_send(sid={sid}): {e}"));
                        self.phase = Phase::Done;
                        return;
                    }
                }
            }
            if blocked {
                return;
            }
            self.bytes_sent += self.encode_buf.len() as u64;
            cursor.encoded = false;
            cursor.entry_idx += 1;
        }
        self.write_syscalls += flush_conn(
            &mut self.conn,
            &mut self.send_buf,
            self.socket.as_raw_fd(),
        );
        *waiting = true;
        *recv_bytes = 0;
        *batch_start = Some(Instant::now());
    }

    fn tick_multi_rr(&mut self, entries: &[WorkloadEntry], coalesce: usize, deadline: Instant) {
        let ModeState::MultiRr {
            cursor,
            waiting,
            recv_bytes,
            batch_target: _,
            batch_start,
        } = &mut self.ms
        else {
            return;
        };
        if *waiting {
            return;
        }
        while cursor.entry_idx < entries.len() {
            if Instant::now() >= deadline {
                return;
            }
            let entry = &entries[cursor.entry_idx];
            if !cursor.encoded {
                self.stack.encode_frame_coalesced_into(
                    &mut self.encode_buf,
                    entry.logical_stream,
                    entry.msg_type,
                    entry.flags,
                    &entry.payload,
                    coalesce,
                );
                cursor.send_off = 0;
                cursor.encoded = true;
            }
            let sid = STREAM_IDS[entry.logical_stream as usize];
            let mut blocked = false;
            while cursor.send_off < self.encode_buf.len() {
                match self
                    .conn
                    .stream_send(sid, &self.encode_buf[cursor.send_off..], false)
                {
                    Ok(0) => {
                        blocked = true;
                        break;
                    }
                    Ok(n) => cursor.send_off += n,
                    Err(quiche::Error::Done) => {
                        self.blocked_writes += 1;
                        self.flow_control_waits += 1;
                        blocked = true;
                        break;
                    }
                    Err(e) => {
                        self.error =
                            Some(format!("multi-rr stream_send(sid={sid}): {e}"));
                        self.phase = Phase::Done;
                        return;
                    }
                }
            }
            if blocked {
                return;
            }
            self.bytes_sent += self.encode_buf.len() as u64;
            cursor.encoded = false;
            cursor.entry_idx += 1;
        }
        self.write_syscalls += flush_conn(
            &mut self.conn,
            &mut self.send_buf,
            self.socket.as_raw_fd(),
        );
        *waiting = true;
        *recv_bytes = 0;
        *batch_start = Some(Instant::now());
    }

    fn tick_stream(&mut self, entries: &[WorkloadEntry], coalesce: usize, deadline: Instant) {
        let ModeState::Stream {
            cursor,
            frames_in_flight,
        } = &mut self.ms
        else {
            return;
        };

        loop {
            if Instant::now() >= deadline {
                self.frames_in_flight_peak = self.frames_in_flight_peak.max(*frames_in_flight);
                return;
            }
            if cursor.entry_idx >= entries.len() {
                self.frames_in_flight_peak = self.frames_in_flight_peak.max(self.frames_per_batch);
                self.queue_depth_sum += self.frames_per_batch;
                self.queue_depth_samples += 1;
                cursor.entry_idx = 0;
                cursor.encoded = false;
            }
            let entry = &entries[cursor.entry_idx];
            if !cursor.encoded {
                self.stack.encode_frame_coalesced_into(
                    &mut self.encode_buf,
                    entry.logical_stream,
                    entry.msg_type,
                    entry.flags,
                    &entry.payload,
                    coalesce,
                );
                cursor.send_off = 0;
                cursor.encoded = true;
            }
            let sid = STREAM_IDS[entry.logical_stream as usize];
            let mut blocked = false;
            while cursor.send_off < self.encode_buf.len() {
                match self
                    .conn
                    .stream_send(sid, &self.encode_buf[cursor.send_off..], false)
                {
                    Ok(0) => {
                        blocked = true;
                        break;
                    }
                    Ok(n) => cursor.send_off += n,
                    Err(quiche::Error::Done) => {
                        self.blocked_writes += 1;
                        self.flow_control_waits += 1;
                        self.write_syscalls += flush_conn(
                            &mut self.conn,
                            &mut self.send_buf,
                            self.socket.as_raw_fd(),
                        );
                        // Drain recv to free flow-control credit
                        let num_streams = entries.len().min(STREAM_IDS.len());
                        let mut raw = 0u64;
                        for &s2 in STREAM_IDS.iter().take(num_streams) {
                            loop {
                                match self.conn.stream_recv(s2, &mut self.stream_buf) {
                                    Ok((0, _)) => break,
                                    Ok((m, _)) => {
                                        raw += m as u64;
                                        self.stack.on_stream_data(s2, &self.stream_buf[..m]);
                                    }
                                    Err(quiche::Error::Done) => break,
                                    Err(_) => break,
                                }
                            }
                        }
                        if raw > 0 && self.avg_frame_size > 0 {
                            let est = raw / self.avg_frame_size;
                            *frames_in_flight =
                                frames_in_flight.saturating_sub(est);
                        }
                        blocked = true;
                        break;
                    }
                    Err(e) => {
                        self.error =
                            Some(format!("stream stream_send(sid={sid}): {e}"));
                        self.phase = Phase::Done;
                        return;
                    }
                }
            }
            if blocked {
                *frames_in_flight =
                    frames_in_flight.saturating_add(self.frames_per_batch);
                self.frames_in_flight_peak = self.frames_in_flight_peak.max(*frames_in_flight);
                self.queue_depth_sum += *frames_in_flight;
                self.queue_depth_samples += 1;
                return;
            }
            self.bytes_sent += self.encode_buf.len() as u64;
            cursor.encoded = false;
            cursor.entry_idx += 1;
        }
    }

    fn tick_pipeline(&mut self, entries: &[WorkloadEntry], coalesce: usize, deadline: Instant) {
        let ModeState::Pipeline {
            cursor,
            sending,
            in_flight,
            payload_sent,
        } = &mut self.ms
        else {
            return;
        };
        let depth = self.pipeline_depth.max(1);

        if !*sending && in_flight.len() < depth && Instant::now() < deadline {
            *sending = true;
            cursor.entry_idx = 0;
            cursor.encoded = false;
            cursor.send_off = 0;
        }

        if *sending {
            while cursor.entry_idx < entries.len() {
                if Instant::now() >= deadline {
                    return;
                }
                let entry = &entries[cursor.entry_idx];
                if !cursor.encoded {
                    self.stack.encode_frame_coalesced_into(
                        &mut self.encode_buf,
                        entry.logical_stream,
                        entry.msg_type,
                        entry.flags,
                        &entry.payload,
                        coalesce,
                    );
                    cursor.send_off = 0;
                    cursor.encoded = true;
                }
                let sid = STREAM_IDS[entry.logical_stream as usize];
                let mut blocked = false;
                while cursor.send_off < self.encode_buf.len() {
                    match self
                        .conn
                        .stream_send(sid, &self.encode_buf[cursor.send_off..], false)
                    {
                        Ok(0) => {
                            blocked = true;
                            break;
                        }
                        Ok(n) => cursor.send_off += n,
                        Err(quiche::Error::Done) => {
                            self.blocked_writes += 1;
                            self.flow_control_waits += 1;
                            blocked = true;
                            break;
                        }
                        Err(e) => {
                            self.error =
                                Some(format!("pipeline stream_send(sid={sid}): {e}"));
                            self.phase = Phase::Done;
                            return;
                        }
                    }
                }
                if blocked {
                    return;
                }
                self.bytes_sent += self.encode_buf.len() as u64;
                cursor.encoded = false;
                cursor.entry_idx += 1;
            }
            self.write_syscalls += flush_conn(
                &mut self.conn,
                &mut self.send_buf,
                self.socket.as_raw_fd(),
            );
            *payload_sent += self.batch_data_payload_bytes;
            in_flight.push_back((Instant::now(), *payload_sent));
            let cur = in_flight.len() as u64 * self.frames_per_batch;
            if cur > self.frames_in_flight_peak {
                self.frames_in_flight_peak = cur;
            }
            *sending = false;
        }

        self.queue_depth_sum += in_flight.len() as u64;
        self.queue_depth_samples += 1;
    }

    // ── Shutdown ────────────────────────────────────────────────────

    pub fn close(&mut self) {
        let _ = self.stack.on_close();
        let _ = self.conn.close(true, 0x100, b"loadtest-done");
        let _ = self.flush_outbound();
    }

    pub fn deregister(&mut self, poll: &mio::Poll) {
        let _ = poll.registry().deregister(&mut self.socket);
    }

    /// Consumes the state and produces a [`WorkerResult`].
    pub fn into_result(mut self) -> WorkerResult {
        let handshake_time = self.handshake_time;
        if let Some(msg) = self.error.take() {
            return WorkerResult::failed(msg, handshake_time);
        }
        WorkerResult::ok(
            std::mem::take(&mut self.rtts),
            self.stack.frames_tx(),
            self.bytes_sent,
            self.stack.frames_rx(),
            self.stack.bytes_rx(),
            self.frames_in_flight_peak,
            self.queue_depth_sum,
            self.queue_depth_samples,
            self.blocked_writes,
            self.flow_control_waits,
            self.write_syscalls,
            self.read_syscalls,
            handshake_time,
        )
    }
}

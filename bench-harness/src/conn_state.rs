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
use crate::workload::{GROUP_NAME, GROUP_PARTITIONS, GROUP_TOPIC, WorkloadEntry};
use frame::header::{FrameFlags, MessageType};

/// Client-initiated bidirectional QUIC stream IDs.
pub const STREAM_IDS: [u64; 15] = [0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56];

/// Number of distinct QUIC streams — exposed for workload mapping.
pub(crate) const STREAM_ID_COUNT: usize = STREAM_IDS.len();

/// UDP datagram scratch buffer size.
const DGRAM_BUF: usize = 65_535;

/// Member re-sends its ConsumerGroupJoin at this cadence to refresh the
/// server-side heartbeat before `DEFAULT_MAX_IDLE_NS` (3s) evicts it.
const GROUP_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
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
    /// P5 consumer-group scenario: connection 0 is the [`Role::Publisher`],
    /// all others are [`Role::Member`]s that join one group and count
    /// round-robin deliveries. See `tick_group`.
    Group,
}

/// Role a connection plays in `BenchMode::Group`. Legacy modes default to
/// `Publisher` (the field is unused there). Reported back via
/// [`WorkerResult`] so `stats` can split publisher tx from member rx.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Publisher,
    Member,
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

/// Builds the group-mode publisher's Publish payload:
/// `topic_len:u16 BE | topic | 4 KiB data`. Mirrors the wire format the
/// server's `handle_publish` parses (`application.rs` Publish arm).
fn build_group_publish_payload() -> Vec<u8> {
    let topic = GROUP_TOPIC.as_bytes();
    let data = vec![0xE8u8; 4096];
    let mut v = Vec::with_capacity(2 + topic.len() + data.len());
    v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    v.extend_from_slice(topic);
    v.extend_from_slice(&data);
    v
}

/// Builds a group-mode member's ConsumerGroupJoin payload with a
/// per-member consumer id `c<idx>`: `topic_len:u16 | topic | group_len:u16
/// | group | consumer_len:u16 | consumer | partitions:u32 BE`. Mirrors
/// the wire format the server's `handle_group_join` parses.
fn build_group_join_payload(global_idx: usize) -> Vec<u8> {
    let topic = GROUP_TOPIC.as_bytes();
    let group = GROUP_NAME.as_bytes();
    let consumer = format!("c{global_idx}");
    let consumer = consumer.as_bytes();
    let mut v = Vec::with_capacity(6 + topic.len() + group.len() + consumer.len() + 4);
    v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    v.extend_from_slice(topic);
    v.extend_from_slice(&(group.len() as u16).to_be_bytes());
    v.extend_from_slice(group);
    v.extend_from_slice(&(consumer.len() as u16).to_be_bytes());
    v.extend_from_slice(consumer);
    v.extend_from_slice(&GROUP_PARTITIONS.to_be_bytes());
    v
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
    /// P5 consumer-group mode. The publisher streams Publish frames
    /// continuously (reusing the Stream flow-control pattern); each
    /// member periodically re-sends its ConsumerGroupJoin (heartbeat
    /// refresh — the server evicts idle members after 3s) and counts
    /// incoming Publish deliveries via `stack.frames_rx()`.
    Group {
        cursor: SendCursor,
        /// Member: last time the ConsumerGroupJoin was sent (`None` =
        /// never sent). Publisher ignores this field.
        last_join: Option<Instant>,
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

    /// Role in `BenchMode::Group`. Unused (always `Publisher`) for the
    /// legacy four modes; reported through `into_result` so `stats` can
    /// break out group throughput by role.
    role: Role,
    /// Group-mode publisher: the pre-built Publish payload
    /// (`topic_len:u16 | topic | data`). Built once in `new`.
    group_publish_payload: Vec<u8>,
    /// Group-mode member: the pre-built ConsumerGroupJoin payload
    /// (`topic_len | topic | group_len | group | consumer_len |
    ///  consumer | partitions:u32`). Built once in `new` with a
    /// per-member consumer id derived from the global conn idx.
    group_join_payload: Vec<u8>,
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
        global_idx: usize,
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

        // P5 group mode: derive role from global connection index and
        // pre-build the role-specific payload. Connection 0 publishes;
        // every other connection joins the group as a member.
        let role = if mode == BenchMode::Group && global_idx > 0 {
            Role::Member
        } else {
            Role::Publisher
        };
        let (group_publish_payload, group_join_payload) = if mode == BenchMode::Group {
            (build_group_publish_payload(), build_group_join_payload(global_idx))
        } else {
            (Vec::new(), Vec::new())
        };

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
            // Both publisher and member start with last_join = None.
            // The publisher never calls tick_group_member (it dispatches
            // to tick_group_publisher instead), so the field is unused
            // for that role.
            BenchMode::Group => ModeState::Group {
                cursor: SendCursor::default(),
                last_join: None,
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
            role,
            group_publish_payload,
            group_join_payload,
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
            BenchMode::Rr | BenchMode::Group => 1,
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
            BenchMode::Group => self.tick_group(coalesce, deadline),
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

    // ── Group (P5 consumer-group scenario) ───────────────────────────

    /// Dispatches by [`Role`]: the publisher streams Publish frames; a
    /// member sends one ConsumerGroupJoin then idles (the reactor's recv
    /// loop counts deliveries via `stack.frames_rx()`).
    fn tick_group(&mut self, coalesce: usize, deadline: Instant) {
        match self.role {
            Role::Publisher => self.tick_group_publisher(coalesce, deadline),
            Role::Member => self.tick_group_member(),
        }
    }

    /// Publisher: continuously stream coalesced Publish frames to
    /// `bench.group` on QUIC stream 0. Mirrors `tick_stream`'s send loop
    /// and flow-control recovery, but operates on a single self-built
    /// Publish payload instead of the shared `entries` blend.
    fn tick_group_publisher(&mut self, coalesce: usize, deadline: Instant) {
        let ModeState::Group { cursor, .. } = &mut self.ms else {
            return;
        };
        loop {
            if Instant::now() >= deadline {
                return;
            }
            if !cursor.encoded {
                self.stack.encode_frame_coalesced_into(
                    &mut self.encode_buf,
                    0,
                    MessageType::Publish,
                    FrameFlags::NONE,
                    &self.group_publish_payload,
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
                        self.write_syscalls += flush_conn(
                            &mut self.conn,
                            &mut self.send_buf,
                            self.socket.as_raw_fd(),
                        );
                        // Drain stream 0 to free flow-control credit so
                        // quiche accepts the next stream_send. The
                        // publisher rarely receives, but members' acks
                        // and any server frames land here too.
                        loop {
                            match self.conn.stream_recv(STREAM_IDS[0], &mut self.stream_buf) {
                                Ok((0, _)) => break,
                                Ok((m, _)) => {
                                    self.stack
                                        .on_stream_data(STREAM_IDS[0], &self.stream_buf[..m]);
                                }
                                Err(quiche::Error::Done) => break,
                                Err(_) => break,
                            }
                        }
                        blocked = true;
                        break;
                    }
                    Err(e) => {
                        self.error = Some(format!("group pub stream_send(sid={sid}): {e}"));
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
        }
    }

    /// Member: send the ConsumerGroupJoin exactly once on QUIC stream 0,
    /// then idle. Delivery counting happens in the reactor's recv path
    /// (`stack.on_stream_data` → `frames_rx`); this method only ensures
    /// the join reaches the server so it registers the member in
    /// `group_locals` for round-robin delivery.
    fn tick_group_member(&mut self) {
        let ModeState::Group { last_join, .. } = &mut self.ms else {
            return;
        };
        let now = Instant::now();
        let due = match last_join {
            None => true, // first join
            Some(t) => now.duration_since(*t) >= GROUP_HEARTBEAT_INTERVAL,
        };
        if !due {
            return;
        }
        self.stack.encode_frame_coalesced_into(
            &mut self.encode_buf,
            0,
            MessageType::ConsumerGroupJoin,
            FrameFlags::NONE,
            &self.group_join_payload,
            1, // join is a single control frame — no coalescing
        );
        let sid = STREAM_IDS[0];
        let mut off = 0;
        while off < self.encode_buf.len() {
            match self.conn.stream_send(sid, &self.encode_buf[off..], false) {
                Ok(0) => return, // blocked; retry next tick (last_join unchanged)
                Ok(n) => off += n,
                Err(quiche::Error::Done) => {
                    self.blocked_writes += 1;
                    self.flow_control_waits += 1;
                    self.write_syscalls += flush_conn(
                        &mut self.conn,
                        &mut self.send_buf,
                        self.socket.as_raw_fd(),
                    );
                    return; // retry next tick
                }
                Err(e) => {
                    self.error = Some(format!("group join stream_send(sid={sid}): {e}"));
                    self.phase = Phase::Done;
                    return;
                }
            }
        }
        self.bytes_sent += self.encode_buf.len() as u64;
        self.write_syscalls += flush_conn(
            &mut self.conn,
            &mut self.send_buf,
            self.socket.as_raw_fd(),
        );
        *last_join = Some(now);
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
        let role = if self.mode == BenchMode::Group {
            Some(self.role)
        } else {
            None
        };
        if let Some(msg) = self.error.take() {
            let mut r = WorkerResult::failed(msg, handshake_time);
            r.role = role;
            return r;
        }
        let mut r = WorkerResult::ok(
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
        );
        r.role = role;
        r
    }
}

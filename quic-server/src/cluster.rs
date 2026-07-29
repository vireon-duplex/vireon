//! Inter-node cluster transport — UDP datagram relay for
//! [`InterNodeMessage`] traffic.
//!
//! See P6 (Multi-node Cluster Wiring) in the project roadmap.
//!
//! # Architecture
//!
//! ```text
//! Main thread
//!  ├─ ClusterTransport thread (1 per node)
//!  │    ├─ UDP socket (non-blocking, dedicated port)
//!  │    ├─ outgoing_rx ← all workers (fan-in, try_recv)
//!  │    └─ incoming_txs → all workers (broadcast)
//!  └─ Workers (existing, per-core)
//!       └─ VireonDispatcher
//!            ├─ coordinator: Option<ClusterCoordinator>
//!            ├─ cluster_outgoing_tx: Option<Sender<OutgoingMsg>>
//!            └─ cluster_incoming_rx: Option<Receiver<InterNodeMessage>>
//! ```
//!
//! Each worker has its own [`ClusterCoordinator`] copy (same config →
//! same routing decisions via consistent hash). Incoming
//! [`InterNodeMessage`]s are broadcast to ALL workers so each can update
//! its own `RemoteSubscriberRegistry`.
//!
//! # Why UDP, not QUIC, for inter-node traffic?
//!
//! * [`InterNodeMessage`] has its own encode/decode (CRC lives in the
//!   outer frame codec when we choose to wrap it; UDP itself is optional
//!   for internal traffic over a trusted network).
//! * Each message is small enough for a single UDP datagram (the wire
//!   format uses u16 topic_len + u32 payload_len, so max practical size
//!   is well under 64 KiB).
//! * Avoids quiche client complexity + TLS overhead for internal
//!   traffic that doesn't need authentication.

use std::net::{SocketAddr, UdpSocket};
use std::sync::mpsc;
use std::thread;

use cluster_coordinator::{ClusterConfig, ClusterCoordinator, ClusterError, InterNodeMessage};
use rustc_hash::FxHashMap;
use session_registry::NodeId;
use session_registry::node::{NodeAddress, NodeInfo, NodeState, Region, Zone};

// ── log throttles ─────────────────────────────────────────────────
//
// The transport thread sits in a tight `loop { recv_from(); ... }` and
// a misconfigured peer, exhausted socket buffer, or attacker can fire
// these `warn!` calls on every iteration — hundreds of identical lines
// per second. Each counter yields one log line per N occurrences;
// `Relaxed` ordering is sufficient (best-effort noise control, not
// synchronization).
//
// 64 chosen so that at the worst observed rate (~10k err/s on a
// misconfigured peer) we still surface ~150 lines/s — enough to see
// the problem clearly without flooding the log sink.
static DECODE_DROP: crate::log_throttle::AtomicRateCounter =
    crate::log_throttle::AtomicRateCounter::new(64);
static RECV_DROP: crate::log_throttle::AtomicRateCounter =
    crate::log_throttle::AtomicRateCounter::new(64);
static NO_PEER_DROP: crate::log_throttle::AtomicRateCounter =
    crate::log_throttle::AtomicRateCounter::new(64);
static SEND_DROP: crate::log_throttle::AtomicRateCounter =
    crate::log_throttle::AtomicRateCounter::new(64);
static ENCODE_DROP: crate::log_throttle::AtomicRateCounter =
    crate::log_throttle::AtomicRateCounter::new(64);

/// A message destined for another node in the cluster.
///
/// Workers produce these (e.g. `InterNodeMessage::Publish` to forward
/// to a topic owner) and hand them to the transport thread via the
/// outgoing channel.
#[derive(Debug)]
pub struct OutgoingMsg {
    /// The wire-encoded-ready inter-node message.
    pub msg: InterNodeMessage,
    /// Which peer node to send it to.
    pub target: NodeId,
}

/// Per-worker handle into the cluster transport.
///
/// Each worker gets one of these at construction time:
/// * `outgoing_tx` — clone of the shared sender into the transport
///   thread's fan-in inbox. Workers push [`OutgoingMsg`]s here.
/// * `incoming_rx` — dedicated receiver for this worker. The transport
///   thread broadcasts every decoded incoming [`InterNodeMessage`] to
///   every worker's incoming channel.
///
/// When `None` (single-node mode), the worker has no cluster
/// coordination — all topics are owned locally.
pub struct ClusterWorkerHandle {
    /// Sender into the transport thread's outgoing queue.
    pub outgoing_tx: mpsc::Sender<OutgoingMsg>,
    /// Per-worker incoming queue (one of the transport's broadcast fan-out).
    pub incoming_rx: mpsc::Receiver<InterNodeMessage>,
}

/// Configuration for the cluster transport, resolved from CLI.
///
/// Built once in `ServerConfig::from_cli`; passed to
/// [`launch_cluster`] at server startup.
#[derive(Clone, Debug)]
pub struct ClusterTransportConfig {
    /// The cluster coordinator config (self id, peers, vnodes).
    pub cluster_cfg: ClusterConfig,
    /// UDP address this node should bind its inter-node socket to.
    /// Resolved from the entry in `cluster_cfg.peers` whose id ==
    /// `self_id`.
    pub bind_addr: SocketAddr,
    /// Peer id → resolved UDP address. Used by the transport thread to
    /// look up where to send each outgoing datagram.
    pub peer_addrs: FxHashMap<NodeId, SocketAddr>,
}

/// Parse a `--cluster-peers "id=host:port,id=host:port,..."` CLI string
/// into a list of `(NodeId, SocketAddr)` pairs.
///
/// Returns `Err` with a human-readable message on any parse failure so
/// `ServerConfig::from_cli` can surface a readable error.
pub fn parse_cluster_peers(s: &str) -> Result<Vec<(NodeId, SocketAddr)>, String> {
    let mut out = Vec::new();
    for entry in s.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (id_str, addr_str) = entry
            .split_once('=')
            .ok_or_else(|| format!("cluster peer `{entry}` missing `=` separator"))?;
        let id: u32 = id_str
            .trim()
            .parse()
            .map_err(|e| format!("cluster peer id `{id_str}` not a u32: {e}"))?;
        if id == 0 {
            return Err("cluster peer id 0 is reserved (nil)".into());
        }
        let addr: SocketAddr = addr_str
            .trim()
            .parse()
            .map_err(|e| format!("cluster peer addr `{addr_str}` invalid: {e}"))?;
        out.push((NodeId::new(id), addr));
    }
    if out.is_empty() {
        return Err("cluster peers list is empty".into());
    }
    Ok(out)
}

/// Build a [`ClusterTransportConfig`] from a self id and a peer list.
///
/// Constructs a [`ClusterConfig`] suitable for [`ClusterCoordinator`],
/// resolving the bind address from the peer list (the entry whose id
/// matches `self_id`). All peers are marked [`NodeState::Alive`] and
/// placed in the default region / zone — topology-aware placement is a
/// later concern.
///
/// # Errors
/// Optional cluster overrides applied on top of defaults during
/// [`build_cluster_transport_config`]. All fields optional — `Default`
/// gives the standard behavior (replication factor 1, default vnode
/// count from `ClusterConfig::new`).
#[derive(Default, Clone, Debug)]
pub struct ClusterOpts {
    /// Number of nodes (including owner) that should hold a replica of
    /// each publish. `None` = `1` (no replication).
    pub replication_factor: Option<u8>,
    /// Virtual nodes per physical node on the consistent-hash ring.
    /// `None` = `ClusterConfig::DEFAULT_VNODES` (150). Clamped to
    /// `[1, 1024]` by `ClusterMembership::new`.
    pub vnodes: Option<u32>,
}

/// Returns an error if `self_id` is nil, the peer list is empty, the
/// peer list does not contain `self_id`, or there are duplicate ids.
pub fn build_cluster_transport_config(
    self_id: NodeId,
    peers: &[(NodeId, SocketAddr)],
    opts: ClusterOpts,
) -> Result<ClusterTransportConfig, String> {
    if self_id.is_nil() {
        return Err("cluster self_id is nil".into());
    }
    if peers.is_empty() {
        return Err("cluster peers list is empty".into());
    }
    let mut peer_addrs: FxHashMap<NodeId, SocketAddr> =
        FxHashMap::with_capacity_and_hasher(peers.len(), Default::default());
    let mut peer_infos: Vec<NodeInfo> = Vec::with_capacity(peers.len());
    let mut bind_addr: Option<SocketAddr> = None;
    for &(id, addr) in peers {
        if id.is_nil() {
            return Err("cluster peer id 0 is reserved (nil)".into());
        }
        if peer_addrs.insert(id, addr).is_some() {
            return Err(format!("duplicate cluster peer id {}", id.raw()));
        }
        let host_ipv6 = match addr.ip() {
            std::net::IpAddr::V4(v4) => v4.to_ipv6_mapped(),
            std::net::IpAddr::V6(v6) => v6,
        };
        let node_addr = NodeAddress::new(host_ipv6.octets(), addr.port());
        let mut info = NodeInfo::new(id, Region::GLOBAL, Zone::DEFAULT, node_addr);
        info.state = NodeState::Alive;
        peer_infos.push(info);
        if id == self_id {
            bind_addr = Some(addr);
        }
    }
    let bind_addr = bind_addr.ok_or_else(|| {
        format!(
            "cluster self_id {} not present in peers list",
            self_id.raw()
        )
    })?;
    let mut cluster_cfg = ClusterConfig::new(self_id, peer_infos);
    cluster_cfg.replication_factor = opts.replication_factor.unwrap_or(1).max(1);
    if let Some(v) = opts.vnodes {
        cluster_cfg = cluster_cfg.with_vnodes(v);
    }
    Ok(ClusterTransportConfig {
        cluster_cfg,
        bind_addr,
        peer_addrs,
    })
}

/// Builds a fresh [`ClusterCoordinator`] from the config. Each worker
/// calls this to get its own copy (the coordinator is `!Sync`).
///
/// # Errors
/// Propagates [`ClusterError`] from coordinator construction (e.g.
/// invalid config).
#[allow(dead_code)]
pub fn worker_coordinator(cfg: &ClusterConfig) -> Result<ClusterCoordinator, ClusterError> {
    ClusterCoordinator::new(cfg.clone())
}

/// Launches the cluster transport thread and returns one
/// [`ClusterWorkerHandle`] per worker.
///
/// The transport thread:
/// 1. Binds a non-blocking UDP socket to `cfg.bind_addr`.
/// 2. Loops: try_recv from the outgoing fan-in queue → encode → UDP
///    send to the target peer; non-blocking UDP recv → decode →
///    broadcast to every worker's incoming channel.
/// 3. Exits when the outgoing queue's senders all drop (all workers
///    gone) — the thread's JoinHandle is returned for the caller to
///    join at shutdown.
///
/// # Errors
/// Returns a `String` error if the UDP socket cannot be bound or set
/// to non-blocking.
pub fn launch_cluster(
    cfg: &ClusterTransportConfig,
    num_workers: usize,
) -> Result<(Vec<ClusterWorkerHandle>, thread::JoinHandle<()>), String> {
    let socket = UdpSocket::bind(cfg.bind_addr)
        .map_err(|e| format!("cluster UDP bind {}: {e}", cfg.bind_addr))?;
    socket
        .set_nonblocking(true)
        .map_err(|e| format!("cluster UDP non-blocking: {e}"))?;

    // Match the main QUIC socket buffer sizes so burst-heavy cluster
    // forwarding (e.g. ClientPool 20K-frame bursts) doesn't overflow
    // the kernel default (~208 KiB) and silently drop datagrams.
    use std::os::unix::io::AsRawFd;
    const CLUSTER_BUF_SIZE: libc::c_int = 16 * 1024 * 1024;
    let fd = socket.as_raw_fd();
    let optlen = std::mem::size_of_val(&CLUSTER_BUF_SIZE) as libc::socklen_t;
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &CLUSTER_BUF_SIZE as *const _ as *const libc::c_void,
            optlen,
        )
    };
    if rc != 0 {
        tracing::warn!(errno = %rc, "cluster UDP SO_RCVBUF failed (using kernel default)");
    }
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &CLUSTER_BUF_SIZE as *const _ as *const libc::c_void,
            optlen,
        )
    };
    if rc != 0 {
        tracing::warn!(errno = %rc, "cluster UDP SO_SNDBUF failed (using kernel default)");
    }
    tracing::info!(
        bind_addr = %cfg.bind_addr,
        peers = cfg.peer_addrs.len(),
        "cluster transport bound"
    );

    let (outgoing_tx, outgoing_rx) = mpsc::channel::<OutgoingMsg>();
    let mut incoming_txs = Vec::with_capacity(num_workers);
    let mut handles = Vec::with_capacity(num_workers);
    for _ in 0..num_workers {
        let (itx, irx) = mpsc::channel::<InterNodeMessage>();
        incoming_txs.push(itx);
        handles.push(ClusterWorkerHandle {
            outgoing_tx: outgoing_tx.clone(),
            incoming_rx: irx,
        });
    }
    // Drop the original sender so `outgoing_rx` exhausts when all
    // workers drop their clones (the transport loop exits then).
    drop(outgoing_tx);

    let peers = cfg.peer_addrs.clone();
    let self_id = cfg.cluster_cfg.self_id;
    let join = thread::Builder::new()
        .name("vireon-cluster-transport".into())
        .spawn(move || run_transport(socket, peers, outgoing_rx, incoming_txs, self_id))
        .map_err(|e| format!("cluster transport thread spawn: {e}"))?;

    Ok((handles, join))
}

/// Maximum UDP recv buffer size. Matches `MAX_TOPIC_LEN + fixed overhead`
/// with plenty of slack for the InterNodeMessage wire format.
const RECV_BUF_LEN: usize = 70_000;

fn run_transport(
    socket: UdpSocket,
    peers: FxHashMap<NodeId, SocketAddr>,
    outgoing_rx: mpsc::Receiver<OutgoingMsg>,
    incoming_txs: Vec<mpsc::Sender<InterNodeMessage>>,
    self_id: NodeId,
) {
    let mut recv_buf = vec![0u8; RECV_BUF_LEN];
    // Coalesce sleep duration — the transport thread polls both the
    // mpsc inbox and the UDP socket; when both are idle it yields.
    let idle = std::time::Duration::from_micros(200);

    loop {
        let mut did_work = false;

        // ── Drain outgoing fan-in ──────────────────────────────────
        while let Ok(out) = outgoing_rx.try_recv() {
            did_work = true;
            process_outgoing(&out, self_id, &peers, &socket, &incoming_txs);
        }

        // ── Drain incoming UDP datagrams ──────────────────────────
        loop {
            match socket.recv_from(&mut recv_buf) {
                Ok((n, peer_addr)) => {
                    did_work = true;
                    match InterNodeMessage::decode(&recv_buf[..n]) {
                        Ok(msg) => {
                            // Broadcast to every worker's incoming queue.
                            // A full worker queue silently drops — the
                            // worker will catch up on the next message.
                            for tx in &incoming_txs {
                                let _ = tx.send(msg.clone());
                            }
                        }
                        Err(e) => {
                            if DECODE_DROP.tick() {
                                tracing::warn!(
                                    %peer_addr,
                                    len = n,
                                    total = DECODE_DROP.count(),
                                    ?e,
                                    "[cluster] decode failed — dropping (logging every 64th)"
                                );
                            }
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break; // No more datagrams ready.
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {
                    continue; // Retry on EINTR.
                }
                Err(e) => {
                    if RECV_DROP.tick() {
                        tracing::warn!(
                            error = %e,
                            total = RECV_DROP.count(),
                            "[cluster] UDP recv failed — backing off (logging every 64th)"
                        );
                    }
                    break;
                }
            }
        }

        if !did_work {
            // Both queues idle — block briefly on the outgoing queue so
            // we wake up as soon as a worker sends, or exit if all
            // worker senders have dropped.
            match outgoing_rx.recv_timeout(idle) {
                Ok(out) => {
                    process_outgoing(&out, self_id, &peers, &socket, &incoming_txs);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Loop and poll again.
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    tracing::info!("[cluster] transport thread exiting (no more workers)");
                    return;
                }
            }
        }
    }
}

/// Sends one outgoing message, routing self-targets back through the
/// incoming queue.
fn process_outgoing(
    out: &OutgoingMsg,
    self_id: NodeId,
    peers: &FxHashMap<NodeId, SocketAddr>,
    socket: &UdpSocket,
    incoming_txs: &[mpsc::Sender<InterNodeMessage>],
) {
    if out.target == self_id {
        // Shouldn't happen — workers only forward to non-self owners —
        // but if it does, loop back through the incoming queue so the
        // message is handled locally.
        for tx in incoming_txs {
            let _ = tx.send(out.msg.clone());
        }
        return;
    }
    let Some(addr) = peers.get(&out.target) else {
        if NO_PEER_DROP.tick() {
            tracing::warn!(
                target = %out.target,
                total = NO_PEER_DROP.count(),
                "[cluster] no peer address for target — dropping (logging every 64th)"
            );
        }
        return;
    };
    match out.msg.encode() {
        Ok(bytes) => {
            if let Err(e) = socket.send_to(&bytes, addr) {
                if SEND_DROP.tick() {
                    tracing::warn!(
                        target = %out.target,
                        %addr,
                        error = %e,
                        total = SEND_DROP.count(),
                        "[cluster] UDP send failed (logging every 64th)"
                    );
                }
            }
        }
        Err(e) => {
            if ENCODE_DROP.tick() {
                tracing::warn!(
                    target = %out.target,
                    ?e,
                    total = ENCODE_DROP.count(),
                    "[cluster] encode failed — dropping (logging every 64th)"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn parse_cluster_peers_basic() {
        let peers =
            parse_cluster_peers("1=127.0.0.1:4001,2=127.0.0.1:4002,3=127.0.0.1:4003").unwrap();
        assert_eq!(peers.len(), 3);
        assert_eq!(peers[0].0, NodeId::new(1));
        assert_eq!(peers[1].0, NodeId::new(2));
        assert_eq!(peers[2].0, NodeId::new(3));
    }

    #[test]
    fn parse_cluster_peers_handles_spaces() {
        let peers = parse_cluster_peers(" 1 = 127.0.0.1:4001 , 2 = 127.0.0.1:4002 ").unwrap();
        assert_eq!(peers.len(), 2);
    }

    #[test]
    fn parse_cluster_peers_rejects_empty() {
        assert!(parse_cluster_peers("").is_err());
        assert!(parse_cluster_peers("   ").is_err());
    }

    #[test]
    fn parse_cluster_peers_rejects_nil_id() {
        assert!(parse_cluster_peers("0=127.0.0.1:4000").is_err());
    }

    #[test]
    fn parse_cluster_peers_rejects_bad_id() {
        assert!(parse_cluster_peers("abc=127.0.0.1:4000").is_err());
    }

    #[test]
    fn parse_cluster_peers_rejects_missing_equals() {
        assert!(parse_cluster_peers("1 127.0.0.1:4000").is_err());
    }

    #[test]
    fn parse_cluster_peers_rejects_bad_addr() {
        assert!(parse_cluster_peers("1=not-an-addr").is_err());
    }

    #[test]
    fn build_config_resolves_bind_addr_from_self_entry() {
        let peers = vec![
            (NodeId::new(1), "127.0.0.1:4001".parse().unwrap()),
            (NodeId::new(2), "127.0.0.1:4002".parse().unwrap()),
        ];
        let cfg = build_cluster_transport_config(NodeId::new(2), &peers, ClusterOpts::default()).unwrap();
        assert_eq!(cfg.bind_addr, peers[1].1);
        assert_eq!(cfg.cluster_cfg.self_id, NodeId::new(2));
        assert_eq!(cfg.cluster_cfg.peers.len(), 2);
        assert_eq!(cfg.peer_addrs.len(), 2);
    }

    #[test]
    fn build_config_marks_all_peers_alive() {
        let peers = vec![
            (NodeId::new(1), "127.0.0.1:4001".parse().unwrap()),
            (NodeId::new(2), "127.0.0.1:4002".parse().unwrap()),
        ];
        let cfg = build_cluster_transport_config(NodeId::new(1), &peers, ClusterOpts::default()).unwrap();
        for p in &cfg.cluster_cfg.peers {
            assert_eq!(p.state, NodeState::Alive);
        }
    }

    #[test]
    fn build_config_rejects_nil_self() {
        let peers = vec![(NodeId::new(1), "127.0.0.1:4001".parse().unwrap())];
        assert!(build_cluster_transport_config(NodeId::NIL, &peers, ClusterOpts::default()).is_err());
    }

    #[test]
    fn build_config_rejects_self_missing_from_peers() {
        let peers = vec![
            (NodeId::new(1), "127.0.0.1:4001".parse().unwrap()),
            (NodeId::new(2), "127.0.0.1:4002".parse().unwrap()),
        ];
        assert!(build_cluster_transport_config(NodeId::new(99), &peers, ClusterOpts::default()).is_err());
    }

    #[test]
    fn build_config_rejects_duplicate_ids() {
        let peers = vec![
            (NodeId::new(1), "127.0.0.1:4001".parse().unwrap()),
            (NodeId::new(1), "127.0.0.1:4099".parse().unwrap()),
        ];
        assert!(build_cluster_transport_config(NodeId::new(1), &peers, ClusterOpts::default()).is_err());
    }

    #[test]
    fn build_config_rejects_empty_peers() {
        let peers = vec![];
        assert!(build_cluster_transport_config(NodeId::new(1), &peers, ClusterOpts::default()).is_err());
    }

    #[test]
    fn worker_coordinator_clones_from_cfg() {
        let peers = vec![
            (NodeId::new(1), "127.0.0.1:4001".parse().unwrap()),
            (NodeId::new(2), "127.0.0.1:4002".parse().unwrap()),
        ];
        let cfg = build_cluster_transport_config(NodeId::new(1), &peers, ClusterOpts::default()).unwrap();
        let c1 = worker_coordinator(&cfg.cluster_cfg).unwrap();
        let c2 = worker_coordinator(&cfg.cluster_cfg).unwrap();
        // Two independent copies — both see the same self id.
        assert_eq!(c1.self_id(), c2.self_id());
    }

    #[test]
    fn launch_cluster_returns_one_handle_per_worker() {
        let peers = vec![
            (NodeId::new(1), "127.0.0.1:4101".parse().unwrap()),
            (NodeId::new(2), "127.0.0.1:4102".parse().unwrap()),
        ];
        let cfg = build_cluster_transport_config(NodeId::new(1), &peers, ClusterOpts::default()).unwrap();
        let (handles, join) = launch_cluster(&cfg, 3).unwrap();
        assert_eq!(handles.len(), 3);
        // Drop all handles → outgoing_tx senders all drop → transport
        // thread sees Disconnected and exits.
        drop(handles);
        // Join with a generous timeout — the transport thread polls
        // with a 200µs idle so should exit within ~1s.
        let _ = join.join();
    }

    #[test]
    fn launch_cluster_end_to_end_udp_roundtrip() {
        // Two transport instances on different ports — node 1 and
        // node 2. Push a message from node 1's worker into node 2's
        // socket; node 2's worker should receive it.
        let peers = vec![
            (NodeId::new(1), "127.0.0.1:4201".parse().unwrap()),
            (NodeId::new(2), "127.0.0.1:4202".parse().unwrap()),
        ];
        let cfg1 = build_cluster_transport_config(NodeId::new(1), &peers, ClusterOpts::default()).unwrap();
        let cfg2 = build_cluster_transport_config(NodeId::new(2), &peers, ClusterOpts::default()).unwrap();
        let (mut h1, _j1) = launch_cluster(&cfg1, 1).unwrap();
        let (mut h2, _j2) = launch_cluster(&cfg2, 1).unwrap();
        let h1rx = h1.remove(0);
        let h2rx = h2.remove(0);

        // Node 1 sends a Subscribe to node 2.
        let msg = InterNodeMessage::subscribe(NodeId::new(1), 42, 100, "sensor.*".into());
        h1rx.outgoing_tx
            .send(OutgoingMsg {
                msg: msg.clone(),
                target: NodeId::new(2),
            })
            .unwrap();

        // Node 2's worker should receive it.
        let got = h2rx
            .incoming_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        assert_eq!(got, msg);

        // Node 1 should NOT receive its own message (transport doesn't
        // broadcast back to self).
        assert!(
            h1rx.incoming_rx
                .recv_timeout(std::time::Duration::from_millis(100))
                .is_err()
        );
    }
}

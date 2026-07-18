//! Inter-worker packet forwarding mesh.
//!
//! When eBPF `SO_REUSEPORT` steering is unavailable (no `CAP_BPF`, or
//! AppArmor restricts unprivileged io_uring), the kernel distributes
//! incoming UDP datagrams by hashing the 4-tuple
//! `(src_ip, src_port, dst_ip, dst_port)`. This is uniform for distinct
//! clients but has one critical weakness: **connection migration**.
//!
//! If a client's source address changes (NAT rebinding, Wi-Fi → LTE
//! handoff), the kernel hash routes subsequent packets to a *different*
//! worker than the one that owns the QUIC connection. Without a
//! forwarding mesh, those packets are silently dropped and the
//! connection dies.
//!
//! ## How it works
//!
//! The server's Source Connection ID (SCID) encodes `reactor_id` in
//! bytes 0-3 (see [`QuicEngine::generate_scid`](crate::engine::QuicEngine)).
//! After the handshake, the client uses the server's SCID as the
//! Destination Connection ID (DCID) in 1-RTT packets. So any worker
//! can read `DCID[0..4]` to determine which worker owns the connection.
//!
//! When a worker receives a 1-RTT packet for a DCID it doesn't own:
//! 1. Parse the DCID from the packet header.
//! 2. Read `DCID[0..4]` as `reactor_id` (little-endian `u32`).
//! 3. If `reactor_id != self && reactor_id < num_workers`, forward the
//!    packet to that worker's inbox.
//! 4. The target worker drains its inbox on the next `on_tick` and
//!    processes the packet normally.
//!
//! ## Performance
//!
//! In steady state, the kernel hash is stable for a given client
//! 4-tuple, so packets always arrive at the correct worker and
//! forwarding is never triggered. Forwarding only activates during
//! migration events — which are rare in benchmark workloads and
//! occasional in production. The per-packet overhead on the fast path
//! is a single DCID hash-table miss + a 4-byte read, both already
//! performed by the existing routing logic.
//!
//! ## Channel topology
//!
//! Each worker has one inbox (`mpsc::Receiver`) and N outbox senders
//! (`mpsc::SyncSender`, one per worker). `SyncSender` is `Clone`, so
//! any worker can send to any other worker's inbox. Channels are
//! bounded (capacity 256); on overflow the packet is dropped (QUIC
//! retransmits on PTO, so a dropped forward is self-healing).

use std::net::SocketAddr;
use std::sync::mpsc;

/// Channel capacity for each worker's inbox.
///
/// 256 entries × ~1.4 KiB avg packet = ~350 KiB worst-case backlog
/// per worker. Sufficient for bursty migration events; overflow drops
/// are self-healing (QUIC PTO retransmit).
const INBOX_CAPACITY: usize = 256;

/// A packet forwarded from one worker to another.
///
/// Owns a heap-allocated copy of the datagram (the io_uring provided
/// buffer is recycled by the originating worker and cannot be sent
/// across threads). The `Box<[u8]>` is sized to the actual packet
/// length — no wasted capacity.
#[derive(Debug)]
pub struct ForwardedPacket {
    /// Source address of the original datagram.
    pub peer: SocketAddr,
    /// Packet payload (exact length, no padding).
    pub data: Box<[u8]>,
}

/// Per-worker forwarding mesh.
///
/// One instance lives inside each `QuicEngine`. The worker reads from
/// its inbox (`inbox_rx`) and writes to other workers' inboxes via
/// cloned `SyncSender` handles in `outboxes`.
pub struct ForwardMesh {
    /// This worker's reactor identifier.
    reactor_id: u32,
    /// Total number of workers (for bounds-checking target reactor_id).
    num_workers: u32,
    /// Inbox — packets forwarded TO this worker by others.
    inbox_rx: mpsc::Receiver<ForwardedPacket>,
    /// Outboxes — `outboxes[i]` sends to worker `i`'s inbox.
    /// `outboxes[self.reactor_id]` is a dummy (never used).
    outboxes: Vec<mpsc::SyncSender<ForwardedPacket>>,

    // ── Metrics (read by observability layer) ────────────────────
    /// Packets this worker forwarded to OTHER workers.
    pub forwarded_out: u64,
    /// Packets this worker received FROM other workers.
    pub forwarded_in: u64,
    /// Packets dropped because the target inbox was full.
    pub forward_drops: u64,
}

impl ForwardMesh {
    /// Creates the N-channel mesh for `num_workers` workers.
    ///
    /// Returns a `Vec` of `(ForwardMesh_build_data)` — one entry per
    /// worker — where each entry contains that worker's inbox receiver
    /// and the shared outbox sender vector.
    ///
    /// Call this ONCE in the server bootstrap, before spawning workers.
    #[must_use]
    pub fn build_channels(
        num_workers: u32,
    ) -> Vec<(
        mpsc::Receiver<ForwardedPacket>,
        Vec<mpsc::SyncSender<ForwardedPacket>>,
    )> {
        let n = num_workers as usize;
        // Create one bounded channel per worker (inbox).
        let channels: Vec<_> = (0..n)
            .map(|_| mpsc::sync_channel::<ForwardedPacket>(INBOX_CAPACITY))
            .collect();
        // Each worker gets a Vec of cloned senders — one per inbox.
        let senders: Vec<mpsc::SyncSender<ForwardedPacket>> =
            channels.iter().map(|(s, _)| s.clone()).collect();
        // Pair each receiver with the shared sender vector.
        channels
            .into_iter()
            .map(|(_, rx)| (rx, senders.clone()))
            .collect()
    }

    /// Constructs a `ForwardMesh` for worker `reactor_id`.
    ///
    /// `inbox_rx` is this worker's receiver. `outboxes` is the shared
    /// sender vector (one sender per worker, index by reactor_id).
    #[must_use]
    pub fn new(
        reactor_id: u32,
        num_workers: u32,
        inbox_rx: mpsc::Receiver<ForwardedPacket>,
        outboxes: Vec<mpsc::SyncSender<ForwardedPacket>>,
    ) -> Self {
        Self {
            reactor_id,
            num_workers,
            inbox_rx,
            outboxes,
            forwarded_out: 0,
            forwarded_in: 0,
            forward_drops: 0,
        }
    }

    /// Attempts to forward a packet to worker `target`.
    ///
    /// Copies the packet data into a heap-allocated `Box<[u8]>` and
    /// pushes it to the target worker's inbox. If the inbox is full,
    /// the packet is dropped (QUIC PTO will retransmit).
    ///
    /// Returns `true` if the packet was queued, `false` if dropped.
    pub fn try_forward(&mut self, target: u32, data: &[u8], peer: SocketAddr) -> bool {
        if target >= self.num_workers || target == self.reactor_id {
            return false; // Invalid target or forwarding to self.
        }
        let pkt = ForwardedPacket {
            peer,
            data: data.to_vec().into_boxed_slice(),
        };
        match self.outboxes[target as usize].try_send(pkt) {
            Ok(()) => {
                self.forwarded_out += 1;
                true
            }
            Err(mpsc::TrySendError::Full(_)) => {
                self.forward_drops += 1;
                false
            }
            // Disconnected means the target worker exited — treat as drop.
            Err(mpsc::TrySendError::Disconnected(_)) => {
                self.forward_drops += 1;
                false
            }
        }
    }

    /// Drains all pending forwarded packets into `out`.
    ///
    /// Called once per `on_tick` before `flush_sends`. The caller
    /// processes each packet via `process_datagram` after the drain
    /// completes, avoiding `&mut self` borrow conflicts.
    pub fn drain_into(&mut self, out: &mut Vec<ForwardedPacket>) {
        while let Ok(pkt) = self.inbox_rx.try_recv() {
            self.forwarded_in += 1;
            out.push(pkt);
        }
    }
}

impl std::fmt::Debug for ForwardMesh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForwardMesh")
            .field("reactor_id", &self.reactor_id)
            .field("num_workers", &self.num_workers)
            .field("forwarded_out", &self.forwarded_out)
            .field("forwarded_in", &self.forwarded_in)
            .field("forward_drops", &self.forward_drops)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn build_channels_returns_one_per_worker() {
        let mesh_data = ForwardMesh::build_channels(4);
        assert_eq!(mesh_data.len(), 4);
        // Each entry has 4 senders (one per worker).
        for (_, senders) in &mesh_data {
            assert_eq!(senders.len(), 4);
        }
    }

    #[test]
    fn forward_to_other_worker_succeeds() {
        let mesh_data = ForwardMesh::build_channels(2);
        let mut iter = mesh_data.into_iter();
        let (rx0, senders0) = iter.next().unwrap(); // worker 0
        let (rx1, _senders1) = iter.next().unwrap(); // worker 1 — keep rx1 alive

        let mut mesh0 = ForwardMesh::new(0, 2, rx0, senders0);
        let peer: SocketAddr = "127.0.0.1:9999".parse().unwrap();

        assert!(mesh0.try_forward(1, b"hello", peer));
        assert_eq!(mesh0.forwarded_out, 1);

        // Worker 1 should have received it.
        let pkt = rx1
            .try_recv()
            .expect("packet should be in worker 1's inbox");
        assert_eq!(&*pkt.data, b"hello");
        assert_eq!(pkt.peer, peer);
    }

    #[test]
    fn forward_to_self_is_rejected() {
        let mesh_data = ForwardMesh::build_channels(2);
        let (rx, senders) = mesh_data.into_iter().next().unwrap();
        let mut mesh = ForwardMesh::new(0, 2, rx, senders);

        let peer: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        assert!(!mesh.try_forward(0, b"data", peer)); // self-forward
        assert_eq!(mesh.forwarded_out, 0);
    }

    #[test]
    fn forward_to_invalid_target_is_rejected() {
        let mesh_data = ForwardMesh::build_channels(2);
        let (rx, senders) = mesh_data.into_iter().next().unwrap();
        let mut mesh = ForwardMesh::new(0, 2, rx, senders);

        let peer: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        assert!(!mesh.try_forward(99, b"data", peer)); // out of range
        assert_eq!(mesh.forwarded_out, 0);
    }

    #[test]
    fn drain_collects_forwarded_packets() {
        let mesh_data = ForwardMesh::build_channels(2);
        let mut iter = mesh_data.into_iter();
        let (_rx0, senders0) = iter.next().unwrap();
        let (rx1, senders1) = iter.next().unwrap();

        // Worker 0 sends two packets to worker 1.
        let peer: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let _ = senders0[1].try_send(ForwardedPacket {
            peer,
            data: b"pkt1".to_vec().into_boxed_slice(),
        });
        let _ = senders0[1].try_send(ForwardedPacket {
            peer,
            data: b"pkt2".to_vec().into_boxed_slice(),
        });

        // Worker 1 drains its inbox.
        let mut mesh1 = ForwardMesh::new(1, 2, rx1, senders1);
        let mut out = Vec::new();
        mesh1.drain_into(&mut out);
        assert_eq!(out.len(), 2);
        assert_eq!(mesh1.forwarded_in, 2);
        assert_eq!(&*out[0].data, b"pkt1");
        assert_eq!(&*out[1].data, b"pkt2");
    }
}

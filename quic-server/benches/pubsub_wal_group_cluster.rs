//! Bench: Phase 4 / 5 / 6 hot paths through `VireonDispatcher`.
//!
//! Complements `pubsub_dispatch.rs` (which covers P3 send-policy + P7
//! ACL). Each phase that lands a feature must also land the measurement
//! that protects its hot path from regression — this file closes the
//! gap for the three phases that shipped wiring + unit tests but had
//! no bench coverage:
//!
//! * **P4 — `bench_wal_replay`** — WAL append + `Fetch(offset)` replay.
//!   Surfaces per-entry FetchReply encode cost and guards against an
//!   accidental O(N²) scan in the replay loop.
//! * **P5 — `bench_group_fanout`** — consumer-group round-robin
//!   delivery. Per-publish cost should be flat in group size; a growing
//!   slope signals member-list iteration creep.
//! * **P6 — `bench_cluster_routing`** — consistent-hash-ring lookup
//!   (`route_publish`) + the full forward-to-owner integration path.
//!
//! Run with:
//! ```text
//! cargo bench -p quic-server --bench pubsub_wal_group_cluster
//! ```

#![allow(missing_docs, clippy::unwrap_used)]

use std::sync::mpsc;

use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use frame::codec::FrameRef;
use frame::header::{FrameFlags, MessageType, Seq, StreamId};
use frame::payload::{FetchPayload, ParsedPayload, encode_payload};
use quic_server::cluster::{OutgoingMsg, build_cluster_transport_config};
use quic_server::layers::{AppDispatcher, FrameContext, VireonDispatcher};
use session_manager::SessionId;
use session_registry::NodeId;
use tempfile::TempDir;

// ── shared helpers ───────────────────────────────────────────────────

/// FrameContext for benches — default session id, caller-supplied
/// connection + QUIC stream ids. Mirrors `pubsub_dispatch.rs::ctx`.
fn ctx(conn_idx: usize, quic_stream_id: u64) -> FrameContext {
    FrameContext {
        session_id: SessionId::new(),
        conn_idx,
        quic_stream_id,
    }
}

/// Builds a zero-copy `FrameRef` borrowing from `payload_buf`, typed
/// as a `Fetch` request. Same construction the dispatcher's own tests
/// use (application.rs `fetch_fref`).
fn fetch_fref(payload_buf: &[u8]) -> FrameRef<'_> {
    FrameRef {
        stream_id: StreamId::new(0),
        seq: Seq::ZERO,
        msg_type: MessageType::Fetch,
        flags: FrameFlags::NONE,
        payload: payload_buf,
    }
}

/// Encodes a `FetchPayload` to bytes, ready to be wrapped in a `FrameRef`.
fn encode_fetch(topic: &str, from_offset: u64, max_count: u32) -> Vec<u8> {
    encode_payload(&ParsedPayload::Fetch(FetchPayload {
        topic,
        from_offset,
        max_count,
    }))
}

/// Opens a fresh `FileStore` rooted at a tempdir and attaches it to a
/// fresh dispatcher. The `TempDir` is returned so the caller can keep
/// it alive for the bench's duration (drop = delete).
fn dispatcher_with_wal() -> (VireonDispatcher, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = wal_store::FileStore::open(dir.path()).unwrap();
    let d = VireonDispatcher::new().with_wal(store);
    (d, dir)
}

// ── Group A: P4 — WAL replay / catch-up ──────────────────────────────

/// Pre-populates the WAL with `n` single-byte entries on `topic` and
/// drains the (empty) fan-out outbox so only WAL state remains.
fn populate_wal(d: &mut VireonDispatcher, topic: &pubsub_engine::Topic, n: u64) {
    let payload = [0xABu8; 32];
    for i in 0..n {
        d.handle_publish(&ctx(1, 200), StreamId::new(2), topic, &payload);
        // Vary the byte so each entry is distinct (not that the WAL cares).
        let _ = i;
    }
    let _ = d.drain_outbox();
}

fn bench_wal_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_replay");
    let topic = pubsub_engine::Topic::new("sensor.temp").unwrap();

    // ── replay_full ───────────────────────────────────────────────
    //
    // Fetch the entire WAL (from=0, max=N). Per-entry cost is read
    // from the slope across N. A super-linear slope means the replay
    // loop is scanning from the head each time instead of seeking.
    for &n in &[10u64, 100, 1000, 10_000] {
        let (mut d, _keepalive) = dispatcher_with_wal();
        populate_wal(&mut d, &topic, n);
        let payload = encode_fetch("sensor.temp", 0, n as u32);
        let fref = fetch_fref(&payload);
        group.bench_with_input(BenchmarkId::new("replay_full", n), &n, |b, &_| {
            b.iter(|| {
                d.handle_fetch(&ctx(2, 300), &fref);
                // Clear the FetchReply frames so the next iteration
                // starts from an empty outbox.
                let _ = d.drain_outbox();
            });
        });
    }

    // ── replay_tail_catchup ───────────────────────────────────────
    //
    // Subscriber reconnecting after downtime: WAL head is large (N)
    // but only the last 10 entries are requested. Cost must be ~flat
    // in N — a growing slope signals a scan-from-zero bug in the
    // offset→location resolution.
    for &n in &[100u64, 1000, 10_000, 100_000] {
        let (mut d, _keepalive) = dispatcher_with_wal();
        populate_wal(&mut d, &topic, n);
        let from = n.saturating_sub(10);
        let payload = encode_fetch("sensor.temp", from, 10);
        let fref = fetch_fref(&payload);
        group.bench_with_input(
            BenchmarkId::new("replay_tail_catchup", n),
            &n,
            |b, &_| {
                b.iter(|| {
                    d.handle_fetch(&ctx(2, 300), &fref);
                    let _ = d.drain_outbox();
                });
            },
        );
    }

    // ── publish_append_cost ───────────────────────────────────────
    //
    // Micro: per-publish WAL append cost, isolated from fan-out (no
    // subscribers). Each iteration gets a fresh WAL so the file does
    // not grow unboundedly across criterion's iteration loop; setup
    // (FileStore::open) is not measured.
    let payload = [0xABu8; 32];
    group.bench_function("publish_append", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();
                let store = wal_store::FileStore::open(dir.path()).unwrap();
                (VireonDispatcher::new().with_wal(store), dir)
            },
            |(mut d, _keepalive)| {
                for _ in 0..100u64 {
                    d.handle_publish(&ctx(1, 200), StreamId::new(2), &topic, &payload);
                }
                black_box(d.drain_outbox());
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ── Group B: P5 — consumer-group round-robin ─────────────────────────

/// Encodes a `ConsumerGroupJoin` payload in the wire format the
/// dispatcher's `parse_group_prefix` expects (application.rs:1374):
/// `topic_len:u16 BE | topic | group_len:u16 BE | group |
///  consumer_len:u16 BE | consumer | partitions:u32 BE`. Built with
/// plain Vec byte ops — `frame::ByteWriter` is slice-backed with no
/// owned variant, and there is no `ParsedPayload::GroupJoin` to lean on.
fn encode_group_join(topic: &str, group: &str, consumer: &str, partitions: u32) -> Vec<u8> {
    fn put_u16(buf: &mut Vec<u8>, v: usize) {
        buf.extend_from_slice(&(u16::try_from(v).unwrap_or(u16::MAX)).to_be_bytes());
    }
    let mut buf = Vec::with_capacity(6 + topic.len() + group.len() + consumer.len() + 4);
    put_u16(&mut buf, topic.len());
    buf.extend_from_slice(topic.as_bytes());
    put_u16(&mut buf, group.len());
    buf.extend_from_slice(group.as_bytes());
    put_u16(&mut buf, consumer.len());
    buf.extend_from_slice(consumer.as_bytes());
    buf.extend_from_slice(&partitions.to_be_bytes());
    buf
}

/// Builds a `FrameRef` for a group-join request.
fn group_join_fref(payload_buf: &[u8]) -> FrameRef<'_> {
    FrameRef {
        stream_id: StreamId::new(0),
        seq: Seq::ZERO,
        msg_type: MessageType::ConsumerGroupJoin,
        flags: FrameFlags::NONE,
        payload: payload_buf,
    }
}

/// Joins one consumer to `group` on `topic` via the dispatcher, then
/// drains the ack.
fn join_consumer(
    d: &mut VireonDispatcher,
    conn_idx: usize,
    quic_stream_id: u64,
    topic: &str,
    group: &str,
    consumer: &str,
    partitions: u32,
) {
    let payload = encode_group_join(topic, group, consumer, partitions);
    let fref = group_join_fref(&payload);
    d.handle_group_join(&ctx(conn_idx, quic_stream_id), &fref);
    let _ = d.drain_outbox();
}

fn bench_group_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_fanout");
    let topic = pubsub_engine::Topic::new("jobs.queue").unwrap();
    let topic_str = "jobs.queue";
    let group_name = "workers";
    let payload = [0xCDu8; 16];

    // ── round_robin_per_publish ───────────────────────────────────
    //
    // One publisher publishes a fixed batch; each publish routes
    // through `group_locals` round-robin to exactly one of M members.
    // Cost should be flat in M (one HashMap lookup + index mod).
    let batch = 1000u64;
    for &m in &[1usize, 2, 4, 8, 16, 32] {
        // Setup: join M consumers (one per connection), keep dispatcher
        // alive across iterations since group membership is the sweep
        // dimension and publishes do not mutate it.
        let mut d = VireonDispatcher::new();
        for i in 0..m {
            join_consumer(
                &mut d,
                i + 1,
                100 + i as u64,
                topic_str,
                group_name,
                &format!("c{i}"),
                4,
            );
        }
        group.bench_with_input(
            BenchmarkId::new("round_robin_per_publish", m),
            &m,
            |b, &_| {
                b.iter(|| {
                    for _ in 0..batch {
                        d.handle_publish(&ctx(0, 999), StreamId::new(999), &topic, &payload);
                    }
                    black_box(d.drain_outbox());
                });
            },
        );
    }

    // ── join_cost ─────────────────────────────────────────────────
    //
    // Per-join cost vs current membership size M. Setup pre-joins M
    // consumers (not measured); the measured closure joins one more.
    for &m in &[0usize, 8, 32, 128] {
        group.bench_with_input(
            BenchmarkId::new("join_cost", m),
            &m,
            |b, &m| {
                b.iter_batched(
                    || {
                        let mut d = VireonDispatcher::new();
                        for i in 0..m {
                            join_consumer(
                                &mut d,
                                i + 1,
                                100 + i as u64,
                                topic_str,
                                group_name,
                                &format!("c{i}"),
                                4,
                            );
                        }
                        d
                    },
                    |mut d| {
                        join_consumer(
                            &mut d,
                            m + 1,
                            100 + m as u64,
                            topic_str,
                            group_name,
                            "probe",
                            4,
                        );
                        black_box(d.drain_outbox());
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ── Group C: P6 — cross-node cluster routing ─────────────────────────

/// Builds a `ClusterConfig`-shaped coordinator config for a ring of
/// `k` nodes (ids 1..=k) with self = node 1, using dummy loopback
/// addrs (the address is irrelevant to routing — only the ring
/// placement matters).
fn dummy_cluster_cfg(k: u32) -> cluster_coordinator::ClusterConfig {
    let peers: Vec<(NodeId, std::net::SocketAddr)> = (1..=k)
        .map(|id| {
            (
                NodeId::new(id),
                format!("127.0.0.1:{}", 4000 + id).parse().unwrap(),
            )
        })
        .collect();
    build_cluster_transport_config(NodeId::new(1), &peers)
        .unwrap()
        .cluster_cfg
}

/// Probes candidate topic names until `coord.route_publish` returns
/// the requested decision (Own vs Forward). Used to pick a stable
/// representative topic for each bench path; the ring is deterministic
/// so the first hit is stable across runs.
fn find_topic(
    coord: &cluster_coordinator::ClusterCoordinator,
    want_forward: bool,
) -> String {
    for i in 0..2000u32 {
        let t = format!("topic.{i}");
        let forward = match coord.route_publish(&t) {
            cluster_coordinator::PublishDecision::Forward { .. } => true,
            cluster_coordinator::PublishDecision::Own { .. } => false,
        };
        if forward == want_forward {
            return t;
        }
    }
    // Extremely unlikely (would require a pathologically skewed ring);
    // fall back to a name that definitely hashes to self is impossible
    // to guarantee, so just return something to keep the bench running.
    "topic.fallback".to_string()
}

fn bench_cluster_routing(c: &mut Criterion) {
    let mut group = c.benchmark_group("cluster_routing");

    // ── route_publish_lookup (micro) ──────────────────────────────
    //
    // Pure consistent-hash-ring lookup cost. Construct the coordinator
    // directly (no dispatcher) and bench `route_publish(topic)`. Sweep
    // cluster size K — cost should be weakly logarithmic in ring
    // cardinality (vnodes × K) since `owner_of` is a sorted-vec bsearch.
    for &k in &[2u32, 4, 8, 16] {
        let cfg = dummy_cluster_cfg(k);
        let coord = cluster_coordinator::ClusterCoordinator::new(cfg).unwrap();
        let own_topic = find_topic(&coord, false);
        let fwd_topic = find_topic(&coord, true);

        group.bench_with_input(
            BenchmarkId::new("route_publish_own", k),
            &k,
            |b, &_| {
                b.iter(|| black_box(coord.route_publish(&own_topic)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("route_publish_forward", k),
            &k,
            |b, &_| {
                b.iter(|| black_box(coord.route_publish(&fwd_topic)));
            },
        );
    }

    // ── forward_path_integration ─────────────────────────────────
    //
    // Full handle_publish forward path through a clustered dispatcher:
    // ACL check → route_publish → PublishDecision::Forward → encode
    // InterNodeMessage::Publish → cluster_outgoing_tx.send(). The
    // outgoing receiver is drained each iteration so the unbounded
    // channel doesn't grow across criterion's iteration loop. Compared
    // against the own-path on a fixed K=4.
    let k = 4u32;
    let cfg = dummy_cluster_cfg(k);
    let coord_probe = cluster_coordinator::ClusterCoordinator::new(cfg.clone()).unwrap();
    let own_topic_str = find_topic(&coord_probe, false);
    let fwd_topic_str = find_topic(&coord_probe, true);
    let own_topic = pubsub_engine::Topic::new(&own_topic_str).unwrap();
    let fwd_topic = pubsub_engine::Topic::new(&fwd_topic_str).unwrap();
    let payload = [0x77u8; 32];

    // Forward path: dispatcher wired with cluster channels.
    let mut d_fwd = VireonDispatcher::new();
    let (out_tx, out_rx) = mpsc::channel::<OutgoingMsg>();
    let (inc_tx, inc_rx) = mpsc::channel::<cluster_coordinator::InterNodeMessage>();
    d_fwd.set_cluster(&cfg, out_tx, inc_rx);
    // Keep the incoming sender alive so the channel doesn't disconnect
    // mid-bench (the forward path never reads it, but a disconnected
    // rx would make any future drain_cluster_inbox call a no-op).
    let _keep_inc_tx = inc_tx;
    group.bench_function("forward_path_K4", |b| {
        b.iter(|| {
            d_fwd.handle_publish(&ctx(0, 999), StreamId::new(999), &fwd_topic, &payload);
            // Drain forwarded inter-node messages.
            while out_rx.try_recv().is_ok() {}
            let _ = d_fwd.drain_outbox();
        });
    });

    // Own path: same dispatcher config, topic hashes to self → local
    // delivery + remote-fanout check (empty remote subs → no sends).
    let mut d_own = VireonDispatcher::new();
    let (out_tx2, out_rx2) = mpsc::channel::<OutgoingMsg>();
    let (inc_tx2, inc_rx2) = mpsc::channel::<cluster_coordinator::InterNodeMessage>();
    d_own.set_cluster(&cfg, out_tx2, inc_rx2);
    let _keep_inc_tx2 = inc_tx2;
    group.bench_function("own_path_K4", |b| {
        b.iter(|| {
            d_own.handle_publish(&ctx(0, 999), StreamId::new(999), &own_topic, &payload);
            while out_rx2.try_recv().is_ok() {}
            let _ = d_own.drain_outbox();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_wal_replay,
    bench_group_fanout,
    bench_cluster_routing,
);
criterion_main!(benches);

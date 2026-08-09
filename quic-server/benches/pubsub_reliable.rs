//! Bench: reliable pub/sub dispatch (ACK + Sequence + Resume).
//!
//! Mirrors `pubsub_dispatch.rs` but enables application-level reliability:
//! publishes carry `ACK_REQ`, the dispatcher retains a copy of every
//! fan-out delivery in the in-memory replay buffer, and after every batch
//! the subscriber side drains + acks (releasing the retained entries).
//!
//! Compares:
//!   * `ephemeral` — same dispatch path with `reliable=false` (baseline).
//!   * `reliable`  — `reliable=true`, no acks (retained entries accumulate).
//!   * `reliable_ackd` — `reliable=true` + per-frame ack release.
//!
//! Run with:
//! ```text
//! cargo bench -p quic-server --bench pubsub_reliable
//! ```

#![allow(missing_docs, clippy::unwrap_used)]

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use frame::header::StreamId;
use quic_server::layers::{AppDispatcher, FrameContext, VireonDispatcher};
use send_policy::DeliveryPolicy;
use session_manager::SessionId;

// ── helpers (mirror pubsub_dispatch.rs) ──────────────────────────────

fn ctx(conn_idx: usize, quic_stream_id: u64) -> FrameContext {
    FrameContext {
        session_id: SessionId::new(),
        conn_idx,
        quic_stream_id,
    }
}

/// Build a dispatcher with N subscribers on `sensor.*`, each bound to a
/// ReliableUnordered send policy. Each subscriber's conn is also bound to
/// a stable logical session id so that reliable fan-out retains entries.
fn build_dispatcher(num_subs: usize) -> VireonDispatcher {
    let mut d = VireonDispatcher::new();
    for i in 1..=num_subs {
        let conn_idx = i;
        let quic_stream_id = (i as u64) * 100;
        let logical_stream = (i as u64) * 10;
        d.bind_send_policy(
            conn_idx,
            quic_stream_id,
            StreamId::new(logical_stream),
            DeliveryPolicy::ReliableUnordered,
        );
        d.handle_subscribe(
            &ctx(conn_idx, quic_stream_id),
            StreamId::new(logical_stream),
            "sensor.*",
        );
        // Bind a logical session so handle_publish's reliable-fanout
        // branch retains entries for this subscriber. Use the conn_idx
        // itself as the logical session id (deterministic, isolated).
        d.register_logical_session_for_bench(conn_idx, conn_idx as u64);
    }
    let _ = d.drain_outbox();
    d
}

// ── bench groups ─────────────────────────────────────────────────────

const BATCH_SIZES: &[usize] = &[1, 10, 100, 1000];
const NUM_SUBS: usize = 3;

fn reliable_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("pubsub_reliable");
    for &batch in BATCH_SIZES {
        group.bench_with_input(
            BenchmarkId::new("ephemeral", batch),
            &batch,
            |b, &batch| {
                let mut d = build_dispatcher(NUM_SUBS);
                let topic = pubsub_engine::Topic::new("sensor.temp").unwrap();
                let payload = [0u8; 64];
                b.iter(|| {
                    for _ in 0..batch {
                        d.handle_publish(
                            &ctx(0, 999),
                            StreamId::new(999),
                            topic.as_str(),
                            &payload,
                            false,
                        );
                    }
                    let out = d.drain_outbox();
                    black_box(out);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("reliable", batch),
            &batch,
            |b, &batch| {
                let mut d = build_dispatcher(NUM_SUBS);
                let topic = pubsub_engine::Topic::new("sensor.temp").unwrap();
                let payload = [0u8; 64];
                b.iter(|| {
                    for _ in 0..batch {
                        d.handle_publish(
                            &ctx(0, 999),
                            StreamId::new(999),
                            topic.as_str(),
                            &payload,
                            true,
                        );
                    }
                    let out = d.drain_outbox();
                    black_box(out);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("reliable_ackd", batch),
            &batch,
            |b, &batch| {
                let mut d = build_dispatcher(NUM_SUBS);
                let topic = pubsub_engine::Topic::new("sensor.temp").unwrap();
                let payload = [0u8; 64];
                b.iter(|| {
                    for _ in 0..batch {
                        d.handle_publish(
                            &ctx(0, 999),
                            StreamId::new(999),
                            topic.as_str(),
                            &payload,
                            true,
                        );
                    }
                    let out = d.drain_outbox();
                    // Ack every (logical, stream) at the highest seq so
                    // the replay buffer releases retained entries.
                    d.ack_all_logical_for_bench();
                    black_box(out);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, reliable_bench);
criterion_main!(benches);

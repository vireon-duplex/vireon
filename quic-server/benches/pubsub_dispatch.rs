//! Bench: pub/sub dispatch integration.
//!
//! Drives the same `VireonDispatcher` hot path the production server
//! uses, isolated from the QUIC transport so we can measure just the
//! Phase 3 (send-policy routing) and Phase 7 (ACL check) overhead.
//!
//! Two groups:
//!
//! * `policy_fanout` — N subscribers on the same topic, each bound to
//!   a different `DeliveryPolicy`. Measures publish + fan-out + policy
//!   routing + drain. The per-policy delivered counts (validated in
//!   unit tests) are not re-asserted here — Criterion measures time,
//!   not correctness.
//!
//! * `acl_overhead` — `check_acl` and the four action handlers under
//!   three configurations: permissive default (`allow("*.*", None, ALL)`),
//!   explicit allow rule, explicit deny rule. Surfaces the cost of
//!   adding an ACL check at the top of every handler (Phase 7).
//!
//! Run with:
//! ```text
//! cargo bench -p quic-server
//! ```

#![allow(missing_docs, clippy::unwrap_used)]

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use frame::codec::FrameHeader;
use frame::header::{FrameFlags, MessageType, Seq, StreamId};
use quic_server::layers::{AppDispatcher, FrameContext, OutgoingFrame, VireonDispatcher};
use send_policy::DeliveryPolicy;
use session_manager::SessionId;
use topic_acl::{AclTableBuilder, Action, ActionSet};

// ── helpers ──────────────────────────────────────────────────────────

/// FrameContext for tests/benches — default session id, caller-supplied
/// connection + QUIC stream ids.
fn ctx(conn_idx: usize, quic_stream_id: u64) -> FrameContext {
    FrameContext {
        session_id: SessionId::new(),
        conn_idx,
        quic_stream_id,
    }
}

/// Builds an `OutgoingFrame` mimicking what `handle_publish` would
/// enqueue for a single subscriber (used by the policy-routing microbench).
fn outgoing(
    conn_idx: usize,
    quic_stream_id: u64,
    stream: u64,
    seq: u64,
    payload: &[u8],
) -> OutgoingFrame {
    OutgoingFrame {
        conn_idx,
        quic_stream_id,
        header: FrameHeader::new(StreamId::new(stream), MessageType::Publish)
            .with_seq(Seq::new(seq))
            .with_flags(FrameFlags::NONE),
        payload: Bytes::copy_from_slice(payload),
    }
}

// ── Group 1: policy-differentiated fan-out ───────────────────────────
//
// Mirrors the production fan-out path: a Publish on a topic fans out
// to N subscribers; each subscriber's copy is routed through its own
// bound `DeliveryPolicy` and emerges from `drain_ready_egress`. The
// bench fixes N=3 (one per policy variant) and sweeps the batch size
// so we can read per-message cost from the slope.

/// One publisher (conn 0) + three subscribers (conn 1/2/3) on `sensor.*`,
/// each bound to a different policy.
fn build_fanout_dispatcher() -> VireonDispatcher {
    let mut d = VireonDispatcher::new();

    // Three subscribers, each on its own QUIC stream, each declaring a
    // different send policy at StreamOpen time.
    // (conn_idx, quic_stream_id, logical_stream, policy)
    let subs: &[(usize, u64, u64, DeliveryPolicy)] = &[
        (1, 100, 10, DeliveryPolicy::ReliableOrdered),
        (2, 200, 20, DeliveryPolicy::RealtimeDropOld),
        (3, 300, 30, DeliveryPolicy::LatestOnly),
    ];
    for &(conn_idx, quic_stream_id, logical_stream, policy) in subs {
        d.bind_send_policy(
            conn_idx,
            quic_stream_id,
            StreamId::new(logical_stream),
            policy,
        );
        // Register the subscription so `handle_publish` finds them.
        d.handle_subscribe(
            &ctx(conn_idx, quic_stream_id),
            StreamId::new(logical_stream),
            "sensor.*",
        );
    }

    // Discard subscribe acks so they don't pollute the first measured drain.
    let _ = d.drain_outbox();
    d
}

/// Runs the full publish→fan-out→policy-route→drain cycle for one
/// message. Mirrors what `VireonStack::process_events` does per recv
/// batch, minus the QUIC encode+send (which is transport cost, not
/// dispatcher cost, and is measured by `bench-harness`).
fn publish_and_drain(d: &mut VireonDispatcher, topic: &pubsub_engine::Topic, payload: &[u8]) {
    d.handle_publish(&ctx(0, 999), StreamId::new(999), topic, payload);

    // Route the fan-out outbox through send-policy — same two-pass
    // drain as `stack.rs::process_events`.
    let outbox = d.drain_outbox();
    for out in outbox {
        match d.enqueue_egress(out) {
            None => {} // consumed by policy
            Some(_direct) => {
                // Subscriber stream is bound in this bench, so this
                // branch shouldn't fire. If it ever does, the frame
                // would go straight to encode_and_send in production.
            }
        }
    }
    let mut ready: Vec<OutgoingFrame> = Vec::new();
    d.drain_ready_egress(&mut ready);
    black_box(ready);
}

fn bench_policy_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("policy_fanout");
    let topic = pubsub_engine::Topic::new("sensor.temp").unwrap();
    let payload = [0xABu8; 32];

    // Batch sizes: 1 (single-message latency), 10, 100, 1000 (steady-state).
    for &batch in &[1usize, 10, 100, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            b.iter(|| {
                let mut d = build_fanout_dispatcher();
                for _ in 0..batch {
                    publish_and_drain(&mut d, &topic, &payload);
                }
            });
        });
    }
    group.finish();
}

// ── Group 2: ACL overhead ────────────────────────────────────────────
//
// Three configurations sweep the ACL cost added in Phase 7:
// * `permissive_default` — single broad allow rule (the shipped default).
// * `explicit_allow`     — rule list narrows to a specific namespace.
// * `explicit_deny`      — rule list denies the action (early return).
//
// Each configuration is measured twice: once for the pure `check_acl`
// call (microbench), once for the full `handle_*` path (integration).

fn build_permissive() -> VireonDispatcher {
    VireonDispatcher::new()
}

fn build_explicit_allow() -> VireonDispatcher {
    VireonDispatcher::new().with_acl(
        AclTableBuilder::new()
            .allow("sensor.*", None, ActionSet::ALL)
            .build()
            .unwrap_or_default(),
    )
}

fn build_explicit_deny() -> VireonDispatcher {
    VireonDispatcher::new().with_acl(
        AclTableBuilder::new()
            .deny("system.*", None, Action::Subscribe)
            .allow("*.*", None, ActionSet::ALL)
            .build()
            .unwrap_or_default(),
    )
}

fn bench_acl_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("acl_check");

    let permissive = build_permissive();
    group.bench_function("permissive_default", |b| {
        b.iter(|| black_box(permissive.check_acl(Action::Subscribe, "sensor.temp")))
    });

    let allow = build_explicit_allow();
    group.bench_function("explicit_allow_rule", |b| {
        b.iter(|| black_box(allow.check_acl(Action::Subscribe, "sensor.temp")))
    });

    let deny = build_explicit_deny();
    group.bench_function("explicit_deny_rule", |b| {
        b.iter(|| black_box(deny.check_acl(Action::Subscribe, "system.logs")))
    });

    group.finish();
}

fn bench_acl_handle(c: &mut Criterion) {
    // Full-handler cost including ACL check + state mutation + ack
    // formatting. The deny path is the cheapest (early return before
    // any state mutation); the allow path measures the full happy path.
    let mut group = c.benchmark_group("acl_handle_subscribe");

    let topic_pattern = "sensor.temp";
    let denied_pattern = "system.logs";

    let mut permissive = build_permissive();
    group.bench_function("permissive_default_allow", |b| {
        b.iter(|| {
            permissive.handle_subscribe(&ctx(0, 100), StreamId::new(1), topic_pattern);
            // Drain ack so the outbox doesn't grow unboundedly.
            let _ = permissive.drain_outbox();
        })
    });

    let mut allow = build_explicit_allow();
    group.bench_function("explicit_allow_rule", |b| {
        b.iter(|| {
            allow.handle_subscribe(&ctx(0, 100), StreamId::new(1), topic_pattern);
            let _ = allow.drain_outbox();
        })
    });

    let mut deny = build_explicit_deny();
    group.bench_function("explicit_deny_rule", |b| {
        b.iter(|| {
            deny.handle_subscribe(&ctx(0, 100), StreamId::new(1), denied_pattern);
            let _ = deny.drain_outbox();
        })
    });

    group.finish();
}

// ── Group 3: send-policy enqueue/dequeue microbench ─────────────────
//
// Isolates the per-frame policy cost from the publish fan-out cost.
// Three bound streams, each receiving M frames in sequence, then one
// drain. Useful for spotting regressions in SendPolicyTable internals
// independent of the pub/sub registry lookup.

fn bench_policy_enqueue(c: &mut Criterion) {
    let mut group = c.benchmark_group("policy_enqueue_per_frame");
    let policies: &[(&str, DeliveryPolicy)] = &[
        ("reliable_ordered", DeliveryPolicy::ReliableOrdered),
        ("realtime_drop_old", DeliveryPolicy::RealtimeDropOld),
        ("latest_only", DeliveryPolicy::LatestOnly),
    ];
    for &(name, policy) in policies {
        group.bench_function(name, |b| {
            b.iter_with_setup(
                || {
                    // Setup: fresh dispatcher with one bound stream.
                    let mut d = VireonDispatcher::new();
                    d.bind_send_policy(0, 42, StreamId::new(7), policy);
                    d
                },
                |mut d| {
                    // Measured: enqueue one frame + drain one frame.
                    let out = outgoing(0, 42, 7, 0, b"x");
                    let _ = d.enqueue_egress(out);
                    let mut ready = Vec::new();
                    d.drain_ready_egress(&mut ready);
                    black_box(ready);
                },
            )
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_policy_fanout,
    bench_acl_check,
    bench_acl_handle,
    bench_policy_enqueue,
);
criterion_main!(benches);

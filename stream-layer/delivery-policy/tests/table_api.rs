//! Integration tests for `DeliveryPolicyTable` (cross-policy behaviour).

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use delivery_policy::{
    Decision, DeliveryPolicy, DeliveryPolicyError, DeliveryPolicyTable, FramePayload,
};
use frame::header::{FrameFlags, MessageType, Seq, StreamId};

fn p(b: u8) -> FramePayload {
    FramePayload::new(
        MessageType::TextToken,
        FrameFlags::NONE,
        Bytes::from(vec![b]),
    )
}

#[test]
fn table_full_returns_error() {
    let mut t = DeliveryPolicyTable::new();
    let cap = delivery_policy::table::MAX_TRACKED_STREAMS;

    // Fill the table to the cap.
    for i in 0..cap {
        t.bind(0, StreamId::new(i as u64), DeliveryPolicy::ReliableOrdered)
            .unwrap();
    }
    assert_eq!(t.len(), cap);

    // One more should error.
    let err = t
        .bind(
            0,
            StreamId::new(cap as u64),
            DeliveryPolicy::ReliableOrdered,
        )
        .unwrap_err();
    assert!(matches!(err, DeliveryPolicyError::TableFull { cap: _ }));
}

#[test]
fn separate_connections_with_same_stream_id_are_independent() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(0, StreamId::new(1), DeliveryPolicy::RealtimeDropOld)
        .unwrap();
    t.bind(1, StreamId::new(1), DeliveryPolicy::LatestOnly)
        .unwrap();

    // Conn 0 — DropOld: seq=5 delivers, seq=3 drops.
    let d0a = t.apply(0, StreamId::new(1), Seq::new(5), p(5));
    let d0b = t.apply(0, StreamId::new(1), Seq::new(3), p(3));
    assert!(matches!(d0a, Decision::Deliver(_)));
    assert!(matches!(d0b, Decision::Drop));

    // Conn 1 — LatestOnly: both buffer.
    let d1a = t.apply(1, StreamId::new(1), Seq::new(5), p(5));
    let d1b = t.apply(1, StreamId::new(1), Seq::new(6), p(6));
    assert!(matches!(d1a, Decision::Buffer));
    assert!(matches!(d1b, Decision::Buffer));

    // Drain only emits for conn 1.
    let mut out0 = Vec::new();
    t.drain_ready_for_connection(0, &mut out0);
    assert!(out0.is_empty());

    let mut out1 = Vec::new();
    t.drain_ready_for_connection(1, &mut out1);
    assert_eq!(out1.len(), 1);
    assert_eq!(out1[0].1, Seq::new(6));
}

#[test]
fn bind_twice_with_same_policy_is_noop_drained() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(0, StreamId::new(2), DeliveryPolicy::ReliableOrdered)
        .unwrap();
    // Apply a frame so the state has a cursor.
    t.apply(0, StreamId::new(2), Seq::new(0), p(0));

    let drained = t
        .bind(0, StreamId::new(2), DeliveryPolicy::ReliableOrdered)
        .unwrap();
    assert!(
        drained.is_empty(),
        "re-bind with no buffered frames drains nothing"
    );
}

#[test]
fn default_to_reliable_ordered_then_apply_in_order() {
    let mut t = DeliveryPolicyTable::new();
    // No bind — default kicks in on first apply.
    let d0 = t.apply(0, StreamId::new(3), Seq::new(0), p(0));
    let d1 = t.apply(0, StreamId::new(3), Seq::new(1), p(1));
    assert!(matches!(d0, Decision::Deliver(_)));
    assert!(matches!(d1, Decision::Deliver(_)));

    // Out-of-order under default policy buffers.
    let d3 = t.apply(0, StreamId::new(3), Seq::new(3), p(3));
    assert!(matches!(d3, Decision::Buffer));
}

#[test]
fn upgrade_from_default_flushes_buffered_before_swap() {
    let mut t = DeliveryPolicyTable::new();
    // Default ReliableOrdered via lazy insert.
    t.apply(0, StreamId::new(5), Seq::new(1), p(1));
    t.apply(0, StreamId::new(5), Seq::new(3), p(3)); // buffered
    t.apply(0, StreamId::new(5), Seq::new(5), p(5)); // buffered

    // StreamOpen arrives with DropOld — buffered frames must be flushed
    // out in seq order before the state is swapped.
    let drained = t
        .bind(0, StreamId::new(5), DeliveryPolicy::RealtimeDropOld)
        .unwrap();
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].0, Seq::new(3));
    assert_eq!(drained[1].0, Seq::new(5));

    // The new DropOld state starts fresh (no memory of the prior cursor).
    // First frame after upgrade is treated as the anchor.
    let d_anchor = t.apply(0, StreamId::new(5), Seq::new(2), p(2));
    assert!(matches!(d_anchor, Decision::Deliver(_)));

    // Now DropOld semantics kick in normally.
    let d_high = t.apply(0, StreamId::new(5), Seq::new(10), p(10));
    assert!(matches!(d_high, Decision::Deliver(_)));
    let d_stale = t.apply(0, StreamId::new(5), Seq::new(4), p(4));
    assert!(matches!(d_stale, Decision::Drop));
}

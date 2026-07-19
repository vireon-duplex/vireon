//! Integration tests for `DeliveryPolicy::LatestOnly`.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use delivery_policy::{Decision, DeliveryPolicy, DeliveryPolicyTable, FramePayload};
use frame::header::{FrameFlags, MessageType, Seq, StreamId};

const CONN: usize = 0;
const SID: StreamId = StreamId::new(99);

fn p(b: u8) -> FramePayload {
    FramePayload::new(
        MessageType::TextToken,
        FrameFlags::NONE,
        Bytes::from(vec![b]),
    )
}

#[test]
fn first_frame_buffers_not_delivered() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(CONN, SID, DeliveryPolicy::LatestOnly).unwrap();

    let d = t.apply(CONN, SID, Seq::new(1), p(0xAA));
    assert!(
        matches!(d, Decision::Buffer),
        "LatestOnly never emits synchronously"
    );
}

#[test]
fn newer_frame_replaces_pending_older_discarded() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(CONN, SID, DeliveryPolicy::LatestOnly).unwrap();

    let d1 = t.apply(CONN, SID, Seq::new(1), p(1));
    let d2 = t.apply(CONN, SID, Seq::new(5), p(5));
    assert!(matches!(d1, Decision::Buffer));
    assert!(matches!(d2, Decision::Buffer));

    // Drain should emit ONLY seq=5 (the newest).
    let mut out = Vec::new();
    t.drain_ready_for_connection(CONN, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].1, Seq::new(5));
    assert_eq!(out[0].2.bytes.as_ref(), &[5]);
}

#[test]
fn older_frame_after_newer_is_dropped() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(CONN, SID, DeliveryPolicy::LatestOnly).unwrap();

    t.apply(CONN, SID, Seq::new(10), p(10));
    let d_older = t.apply(CONN, SID, Seq::new(3), p(3));
    assert!(matches!(d_older, Decision::Drop));

    // Drain still emits seq=10.
    let mut out = Vec::new();
    t.drain_ready_for_connection(CONN, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].1, Seq::new(10));
}

#[test]
fn equal_seq_keeps_existing_drops_new() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(CONN, SID, DeliveryPolicy::LatestOnly).unwrap();

    t.apply(CONN, SID, Seq::new(7), p(7));
    let d_same = t.apply(CONN, SID, Seq::new(7), p(0xEE));
    assert!(matches!(d_same, Decision::Drop));

    let mut out = Vec::new();
    t.drain_ready_for_connection(CONN, &mut out);
    assert_eq!(out[0].2.bytes.as_ref(), &[7], "original payload retained");
}

#[test]
fn drain_clears_pending_second_drain_empty() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(CONN, SID, DeliveryPolicy::LatestOnly).unwrap();

    t.apply(CONN, SID, Seq::new(1), p(1));
    t.apply(CONN, SID, Seq::new(2), p(2));
    t.apply(CONN, SID, Seq::new(3), p(3));

    let mut out1 = Vec::new();
    t.drain_ready_for_connection(CONN, &mut out1);
    assert_eq!(out1.len(), 1);
    assert_eq!(out1[0].1, Seq::new(3));

    let mut out2 = Vec::new();
    t.drain_ready_for_connection(CONN, &mut out2);
    assert!(out2.is_empty());
}

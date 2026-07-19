//! Integration tests for `DeliveryPolicy::RealtimeDropOld`.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use delivery_policy::{Decision, DeliveryPolicy, DeliveryPolicyTable, FramePayload};
use frame::header::{FrameFlags, MessageType, Seq, StreamId};

const CONN: usize = 0;
const SID: StreamId = StreamId::new(11);

fn p(b: u8) -> FramePayload {
    FramePayload::new(
        MessageType::TextToken,
        FrameFlags::NONE,
        Bytes::from(vec![b]),
    )
}

#[test]
fn first_frame_delivers_and_sets_cursor() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(CONN, SID, DeliveryPolicy::RealtimeDropOld).unwrap();

    let d = t.apply(CONN, SID, Seq::new(42), p(0xAA));
    assert!(matches!(d, Decision::Deliver(_)));
}

#[test]
fn newer_seq_delivers_and_advances_cursor() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(CONN, SID, DeliveryPolicy::RealtimeDropOld).unwrap();

    t.apply(CONN, SID, Seq::new(10), p(10));
    let d = t.apply(CONN, SID, Seq::new(20), p(20));
    assert!(matches!(d, Decision::Deliver(_)));
}

#[test]
fn older_seq_dropped() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(CONN, SID, DeliveryPolicy::RealtimeDropOld).unwrap();

    t.apply(CONN, SID, Seq::new(100), p(100));
    let d = t.apply(CONN, SID, Seq::new(50), p(50));
    assert!(matches!(d, Decision::Drop));
}

#[test]
fn equal_seq_dropped_idempotent() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(CONN, SID, DeliveryPolicy::RealtimeDropOld).unwrap();

    t.apply(CONN, SID, Seq::new(7), p(7));
    let d_same = t.apply(CONN, SID, Seq::new(7), p(0xEE));
    assert!(matches!(d_same, Decision::Drop));
}

#[test]
fn drain_emits_nothing() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(CONN, SID, DeliveryPolicy::RealtimeDropOld).unwrap();

    for seq in [1u64, 5, 3, 10, 2] {
        let _ = t.apply(CONN, SID, Seq::new(seq), p(seq as u8));
    }

    let mut out = Vec::new();
    t.drain_ready_for_connection(CONN, &mut out);
    assert!(out.is_empty(), "DropOld never buffers");
}

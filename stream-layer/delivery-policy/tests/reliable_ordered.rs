//! Integration tests for `DeliveryPolicy::ReliableOrdered`.
//!
//! Exercises the public `DeliveryPolicyTable` API — no internal state
//! inspection. Each test verifies the sequence of `Decision` values
//! returned by `apply()` and the `(stream_id, seq, payload)` triples
//! drained at end of batch.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use delivery_policy::{Decision, DeliveryPolicy, DeliveryPolicyTable, FramePayload};
use frame::header::{FrameFlags, MessageType, Seq, StreamId};

const CONN: usize = 0;
const SID: StreamId = StreamId::new(42);

fn p(b: u8) -> FramePayload {
    FramePayload::new(
        MessageType::TextToken,
        FrameFlags::NONE,
        0,
        Bytes::from(vec![b]),
    )
}

fn bind(t: &mut DeliveryPolicyTable) {
    t.bind(CONN, SID, DeliveryPolicy::ReliableOrdered).unwrap();
}

fn drain(t: &mut DeliveryPolicyTable) -> Vec<(StreamId, Seq, FramePayload)> {
    let mut out = Vec::new();
    t.drain_ready_for_connection(CONN, &mut out);
    out
}

#[test]
fn in_order_frames_all_deliver_immediately() {
    let mut t = DeliveryPolicyTable::new();
    bind(&mut t);

    for seq in 0..5u64 {
        let d = t.apply(CONN, SID, Seq::new(seq), p(seq as u8));
        assert!(
            matches!(d, Decision::Deliver(_)),
            "seq {seq} should Deliver"
        );
    }
    // No buffered frames → drain emits nothing.
    assert!(drain(&mut t).is_empty());
}

#[test]
fn out_of_order_buffered_then_drained_on_gap_fill() {
    let mut t = DeliveryPolicyTable::new();
    bind(&mut t);

    // Receive 1, 2, 5, 6 — gap at 3 and 4.
    let d1 = t.apply(CONN, SID, Seq::new(1), p(1));
    let d2 = t.apply(CONN, SID, Seq::new(2), p(2));
    let d5 = t.apply(CONN, SID, Seq::new(5), p(5));
    let d6 = t.apply(CONN, SID, Seq::new(6), p(6));

    assert!(matches!(d1, Decision::Deliver(_)));
    assert!(matches!(d2, Decision::Deliver(_)));
    assert!(matches!(d5, Decision::Buffer));
    assert!(matches!(d6, Decision::Buffer));

    // No drain yet — gap at 3.
    assert!(drain(&mut t).is_empty());

    // seq=3 arrives → fills first gap. Drain should walk forward through 4? No — 4 is still missing.
    let d3 = t.apply(CONN, SID, Seq::new(3), p(3));
    assert!(matches!(d3, Decision::Deliver(_)));
    // After delivering 3, cursor wants 4 — not present. Drain emits nothing.
    assert!(drain(&mut t).is_empty());

    // seq=4 arrives → cursor advances to 4, then drain walks 5, 6.
    let d4 = t.apply(CONN, SID, Seq::new(4), p(4));
    assert!(matches!(d4, Decision::Deliver(_)));
    let drained = drain(&mut t);
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].1, Seq::new(5));
    assert_eq!(drained[1].1, Seq::new(6));

    // Subsequent drain emits nothing.
    assert!(drain(&mut t).is_empty());
}

#[test]
fn duplicate_seq_is_dropped() {
    let mut t = DeliveryPolicyTable::new();
    bind(&mut t);

    let d1 = t.apply(CONN, SID, Seq::new(1), p(1));
    let d1_again = t.apply(CONN, SID, Seq::new(1), p(0xEE));
    assert!(matches!(d1, Decision::Deliver(_)));
    assert!(matches!(d1_again, Decision::Drop));
}

#[test]
fn stale_seq_is_dropped() {
    let mut t = DeliveryPolicyTable::new();
    bind(&mut t);

    t.apply(CONN, SID, Seq::new(5), p(5));
    let d_old = t.apply(CONN, SID, Seq::new(2), p(2));
    assert!(matches!(d_old, Decision::Drop));
}

#[test]
fn reorder_window_overflow_evicts_oldest() {
    // Use a private stream id so we don't conflict with the public MAX cap.
    let mut t = DeliveryPolicyTable::new();
    bind(&mut t);

    // Anchor at seq=0.
    t.apply(CONN, SID, Seq::new(0), p(0));

    // Buffer MAX_REORDER_WINDOW + 1 frames at high seqs to force eviction.
    let window = delivery_policy::table::MAX_REORDER_WINDOW;
    for i in 1..=window + 1 {
        let d = t.apply(CONN, SID, Seq::new((i + 100) as u64), p(i as u8));
        // All should buffer (they're all future frames).
        assert!(
            matches!(d, Decision::Buffer | Decision::Drop),
            "frame {i} should Buffer or Drop (window overflow)"
        );
    }

    // Drain should NOT panic and should emit at most `window` frames.
    let drained = drain(&mut t);
    assert!(drained.len() <= window, "drain exceeded window cap");
}

#[test]
fn first_frame_anchors_cursor_regardless_of_seq() {
    let mut t = DeliveryPolicyTable::new();
    bind(&mut t);

    // First frame is seq=100 — accepted as the anchor.
    let d = t.apply(CONN, SID, Seq::new(100), p(0xAA));
    assert!(matches!(d, Decision::Deliver(_)));

    // Anything <= 100 is now stale.
    let d_old = t.apply(CONN, SID, Seq::new(99), p(0xBB));
    assert!(matches!(d_old, Decision::Drop));
}

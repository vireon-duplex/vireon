//! Integration tests for `DeliveryPolicy::ReliableUnordered`.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use delivery_policy::{Decision, DeliveryPolicy, DeliveryPolicyTable, FramePayload};
use frame::header::{FrameFlags, MessageType, Seq, StreamId};

const CONN: usize = 0;
const SID: StreamId = StreamId::new(7);

fn p(b: u8) -> FramePayload {
    FramePayload::new(
        MessageType::TextToken,
        FrameFlags::NONE,
        Bytes::from(vec![b]),
    )
}

#[test]
fn all_frames_deliver_regardless_of_order() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(CONN, SID, DeliveryPolicy::ReliableUnordered)
        .unwrap();

    // Receive out-of-order: 3, 1, 2.
    for seq in [3u64, 1, 2] {
        let d = t.apply(CONN, SID, Seq::new(seq), p(seq as u8));
        assert!(
            matches!(d, Decision::Deliver(_)),
            "seq {seq} should Deliver"
        );
    }

    // Drain produces nothing — this policy never buffers.
    let mut out = Vec::new();
    t.drain_ready_for_connection(CONN, &mut out);
    assert!(out.is_empty());
}

#[test]
fn duplicate_seq_dropped() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(CONN, SID, DeliveryPolicy::ReliableUnordered)
        .unwrap();

    let d1 = t.apply(CONN, SID, Seq::new(5), p(5));
    let d2 = t.apply(CONN, SID, Seq::new(5), p(0xDD));
    assert!(matches!(d1, Decision::Deliver(_)));
    assert!(matches!(d2, Decision::Drop));
}

#[test]
fn dedup_set_eviction_keeps_recent_correct() {
    let mut t = DeliveryPolicyTable::new();
    t.bind(CONN, SID, DeliveryPolicy::ReliableUnordered)
        .unwrap();

    // Fill the dedup window with seqs 0..MAX_DEDUP_SET.
    let cap = delivery_policy::table::MAX_DEDUP_SET;
    for i in 0..cap {
        let d = t.apply(CONN, SID, Seq::new(i as u64), p(i as u8));
        assert!(matches!(d, Decision::Deliver(_)), "seq {i} should Deliver");
    }

    // Add one more — should evict seq=0 from the dedup window.
    let d = t.apply(CONN, SID, Seq::new(cap as u64), p(0xCC));
    assert!(matches!(d, Decision::Deliver(_)));

    // Re-sending seq=0 may slip through (it was evicted). Acceptable
    // behaviour — dedup is best-effort under bounded memory. We do not
    // assert on the outcome here; this test only verifies that the
    // insertion path does not panic and that recent seqs are still
    // correctly deduped.
    let d_recent = t.apply(CONN, SID, Seq::new((cap) as u64), p(0xEE));
    assert!(
        matches!(d_recent, Decision::Drop),
        "recent seq still deduped"
    );
}

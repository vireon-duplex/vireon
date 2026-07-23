//! Integration tests for `SendPolicyTable` — exercises the public API
//! from a consumer perspective with multi-stream, multi-connection
//! scenarios that go beyond the per-policy unit tests in `state.rs`
//! and the single-stream cases in `table.rs`.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use frame::header::{FrameFlags, MessageType, Seq, StreamId};
use send_policy::{
    DequeueOutcome, DeliveryPolicy, EnqueueDecision, FramePayload, SendPolicyTable,
};

// ── helpers ─────────────────────────────────────────────────────

fn payload(seq: u64, byte: u8) -> FramePayload {
    FramePayload::new(
        MessageType::TextToken,
        FrameFlags::NONE,
        Seq::new(seq),
        Bytes::from(vec![byte]),
    )
}

/// Drain every ready frame from `(conn, stream)` into a `Vec<u8>`.
fn drain_all(table: &mut SendPolicyTable, conn: usize, stream: StreamId) -> Vec<u8> {
    let mut out = Vec::new();
    while let DequeueOutcome::Some(p) = table.dequeue(conn, stream) {
        out.push(p.bytes.as_ref()[0]);
    }
    out
}

// ── 1. Mixed policies on the same connection ───────────────────

#[test]
fn mixed_policies_on_same_connection() {
    let mut t = SendPolicyTable::new();

    // Four streams on conn 0, each with a different policy.
    t.bind(0, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
        .unwrap();
    t.bind(0, StreamId::new(2), DeliveryPolicy::ReliableUnordered)
        .unwrap();
    t.bind(0, StreamId::new(3), DeliveryPolicy::RealtimeDropOld)
        .unwrap();
    t.bind(0, StreamId::new(4), DeliveryPolicy::LatestOnly)
        .unwrap();

    // ReliableOrdered (FIFO) — enqueue 3, expect them back in order.
    for i in 0..3u64 {
        let _ = t.enqueue(0, StreamId::new(1), payload(i, (i + 10) as u8));
    }
    assert_eq!(drain_all(&mut t, 0, StreamId::new(1)), vec![10, 11, 12]);

    // ReliableUnordered (FIFO) — same behaviour on the send side.
    for i in 0..3u64 {
        let _ = t.enqueue(0, StreamId::new(2), payload(i, (i + 20) as u8));
    }
    assert_eq!(drain_all(&mut t, 0, StreamId::new(2)), vec![20, 21, 22]);

    // RealtimeDropOld — enqueue seq 5, then seq 3 (dropped), then seq 9.
    let _ = t.enqueue(0, StreamId::new(3), payload(5, 30));
    let d = t.enqueue(0, StreamId::new(3), payload(3, 31));
    assert!(matches!(d, EnqueueDecision::Drop));
    let _ = t.enqueue(0, StreamId::new(3), payload(9, 32));
    assert_eq!(drain_all(&mut t, 0, StreamId::new(3)), vec![30, 32]);

    // LatestOnly — enqueue seq 0, then seq 1 replaces.
    let _ = t.enqueue(0, StreamId::new(4), payload(0, 40));
    match t.enqueue(0, StreamId::new(4), payload(1, 41)) {
        EnqueueDecision::Replace(p) => assert_eq!(p.bytes.as_ref(), &[40]),
        _ => panic!("expected Replace"),
    }
    assert_eq!(drain_all(&mut t, 0, StreamId::new(4)), vec![41]);
}

// ── 2. Cross-connection isolation ──────────────────────────────

#[test]
fn cross_connection_isolation() {
    let mut t = SendPolicyTable::new();

    // Same stream id on two different connections — must be independent.
    t.bind(0, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
        .unwrap();
    t.bind(1, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
        .unwrap();

    let _ = t.enqueue(0, StreamId::new(1), payload(0, 0xAA));
    let _ = t.enqueue(1, StreamId::new(1), payload(0, 0xBB));

    // Conn 0 returns 0xAA; conn 1 returns 0xBB.
    assert_eq!(drain_all(&mut t, 0, StreamId::new(1)), vec![0xAA]);
    assert_eq!(drain_all(&mut t, 1, StreamId::new(1)), vec![0xBB]);

    // Removing conn 0 must not affect conn 1.
    t.remove_connection(0);
    assert!(t.contains(1, StreamId::new(1)));
    assert!(!t.contains(0, StreamId::new(1)));
}

// ── 3. Full lifecycle ──────────────────────────────────────────

#[test]
fn full_lifecycle_bind_enqueue_drain_remove() {
    let mut t = SendPolicyTable::new();

    let stream = StreamId::new(7);
    t.bind(0, stream, DeliveryPolicy::ReliableOrdered)
        .unwrap();

    // Enqueue 10 frames.
    for i in 0..10u64 {
        let d = t.enqueue(0, stream, payload(i, i as u8));
        assert!(matches!(d, EnqueueDecision::Queue));
    }

    // Drain all 10 in order.
    let drained = drain_all(&mut t, 0, stream);
    assert_eq!(drained.len(), 10);
    for (i, &b) in drained.iter().enumerate() {
        assert_eq!(b, i as u8);
    }

    // Queue should now be empty.
    assert!(matches!(
        t.dequeue(0, stream),
        DequeueOutcome::Empty
    ));

    // Remove the stream.
    t.remove_stream(0, stream);
    assert!(!t.contains(0, stream));
    assert!(matches!(
        t.dequeue(0, stream),
        DequeueOutcome::Unknown
    ));
    assert_eq!(t.len(), 0);
}

// ── 4. Lazy-default then upgrade ───────────────────────────────

#[test]
fn lazy_default_then_upgrade_to_latest_only() {
    let mut t = SendPolicyTable::new();
    let stream = StreamId::new(3);

    // Enqueue before bind — lazy-defaults to ReliableOrdered.
    let _ = t.enqueue(0, stream, payload(0, 1));
    let _ = t.enqueue(0, stream, payload(1, 2));

    // Upgrade to LatestOnly — old FIFO frames must be drained out.
    let drained = t
        .bind(0, stream, DeliveryPolicy::LatestOnly)
        .unwrap();
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].bytes.as_ref(), &[1]);
    assert_eq!(drained[1].bytes.as_ref(), &[2]);

    // Now behaves as LatestOnly.
    let _ = t.enqueue(0, stream, payload(2, 3));
    match t.enqueue(0, stream, payload(3, 4)) {
        EnqueueDecision::Replace(p) => assert_eq!(p.bytes.as_ref(), &[3]),
        _ => panic!("expected Replace"),
    }
    assert_eq!(drain_all(&mut t, 0, stream), vec![4]);
}

// ── 5. ready_streams across connections ────────────────────────

#[test]
fn ready_streams_across_connections() {
    let mut t = SendPolicyTable::new();

    // Bind streams on two connections.
    for &s in &[StreamId::new(1), StreamId::new(2), StreamId::new(3)] {
        t.bind(0, s, DeliveryPolicy::ReliableOrdered).unwrap();
        t.bind(1, s, DeliveryPolicy::ReliableOrdered).unwrap();
    }

    // Only enqueue on some.
    let _ = t.enqueue(0, StreamId::new(1), payload(0, 0));
    let _ = t.enqueue(0, StreamId::new(3), payload(0, 0));
    let _ = t.enqueue(1, StreamId::new(2), payload(0, 0));

    // Per-connection ready.
    let mut ready0 = Vec::new();
    t.ready_streams_for_connection(0, &mut ready0);
    assert_eq!(ready0.len(), 2);

    let mut ready1 = Vec::new();
    t.ready_streams_for_connection(1, &mut ready1);
    assert_eq!(ready1.len(), 1);
    assert_eq!(ready1[0], StreamId::new(2));

    // Across all connections.
    let mut all_ready = Vec::new();
    t.ready_streams_all(&mut all_ready);
    assert_eq!(all_ready.len(), 3);

    // Ready counts.
    assert_eq!(t.ready_count_for_connection(0), 2);
    assert_eq!(t.ready_count_for_connection(1), 1);
}

// ── 6. remove_connection clears all streams ────────────────────

#[test]
fn remove_connection_clears_all_streams() {
    let mut t = SendPolicyTable::new();

    // Bind multiple streams on two connections.
    for &s in &[StreamId::new(1), StreamId::new(2), StreamId::new(3)] {
        t.bind(0, s, DeliveryPolicy::ReliableOrdered).unwrap();
        t.bind(1, s, DeliveryPolicy::ReliableOrdered).unwrap();
    }
    assert_eq!(t.len(), 6);

    // Remove all of conn 0's streams.
    t.remove_connection(0);
    assert_eq!(t.len(), 3);

    // Conn 0 streams are gone.
    for &s in &[StreamId::new(1), StreamId::new(2), StreamId::new(3)] {
        assert!(!t.contains(0, s));
        assert!(t.contains(1, s));
    }

    // Conn 1 streams still work.
    let _ = t.enqueue(1, StreamId::new(1), payload(0, 42));
    match t.dequeue(1, StreamId::new(1)) {
        DequeueOutcome::Some(p) => assert_eq!(p.bytes.as_ref(), &[42]),
        _ => panic!("conn 1 should still work"),
    }
}

// ── 7. Interleaved enqueue/dequeue on multiple streams ─────────

#[test]
fn interleaved_enqueue_dequeue_multiple_streams() {
    let mut t = SendPolicyTable::new();

    t.bind(0, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
        .unwrap();
    t.bind(0, StreamId::new(2), DeliveryPolicy::ReliableOrdered)
        .unwrap();

    // Interleave: enqueue on stream 1, then stream 2, then stream 1 again.
    let _ = t.enqueue(0, StreamId::new(1), payload(0, 1));
    let _ = t.enqueue(0, StreamId::new(2), payload(0, 2));
    let _ = t.enqueue(0, StreamId::new(1), payload(1, 3));

    // Dequeue stream 1 first — gets both frames in FIFO order.
    assert_eq!(drain_all(&mut t, 0, StreamId::new(1)), vec![1, 3]);

    // Stream 2 unaffected.
    assert_eq!(drain_all(&mut t, 0, StreamId::new(2)), vec![2]);

    // Both empty now.
    assert_eq!(t.ready_count_for_connection(0), 0);
}

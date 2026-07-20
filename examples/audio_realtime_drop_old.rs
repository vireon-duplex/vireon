//! Example: `RealtimeDropOld` delivery policy for realtime audio.
//!
//! Scenario: 20 ms Opus frames streamed over a noisy link. Frames
//! may arrive out of order or be duplicated by retransmission. Once
//! the renderer has emitted frame N, any frame with `seq ≤ N` is
//! stale audio that would glitch the playback — drop it on the floor.

#![allow(clippy::print_stdout)]

use bytes::Bytes;
use delivery_policy::{
    Decision, DeliveryPolicy, DeliveryPolicyTable, FramePayload,
};
use frame::header::{FrameFlags, MessageType, Seq, StreamId};

const CONN: usize = 0;
const STREAM: StreamId = StreamId::new(11);

fn opus_frame(seq: u64) -> FramePayload {
    // 80-byte Opus frame: toc byte 0xC0 (CELT-only, 20 ms) + 79 bytes
    // of pseudo-compressed audio. The contents don't matter for the
    // delivery decision — only `seq` does.
    let mut bytes = vec![0xC0u8];
    bytes.extend(std::iter::repeat(0xAA).take(79));
    let _ = seq;
    FramePayload::new(
        MessageType::AudioFrame,
        FrameFlags::NONE,
        0,
        Bytes::from(bytes),
    )
}

fn label(d: &Decision) -> &'static str {
    match d {
        Decision::Deliver(_) => "Deliver",
        Decision::Buffer => "Buffer",
        Decision::Drop => "Drop",
    }
}

fn main() {
    let mut table = DeliveryPolicyTable::new();
    table
        .bind(CONN, STREAM, DeliveryPolicy::RealtimeDropOld)
        .unwrap();

    println!("RealtimeDropOld — Opus 20 ms frames over a reordering link");
    println!("Any seq ≤ last_delivered is dropped as stale.\n");

    // A jumbled arrival pattern: 10, 11, 9 (late), 12, 15 (gap), 12 (dup).
    let arrivals: &[(u64, &str)] = &[
        (10, "first frame"),
        (11, "in order"),
        (9, "arrived late — stale"),
        (12, "in order"),
        (15, "skip ahead (seq 13,14 still in flight)"),
        (12, "duplicate retransmit — stale"),
    ];

    let mut last_delivered: u64 = 0;
    for (seq, note) in arrivals {
        let decision = table.apply(CONN, STREAM, Seq::new(*seq), opus_frame(*seq));
        let lbl = label(&decision);
        if matches!(decision, Decision::Deliver(_)) {
            last_delivered = *seq;
        }
        println!("  apply seq={seq:>2}  {note:<38} → {lbl}");
    }

    println!("\n  last_delivered seq = {last_delivered}");
    println!("  (seq 9 and the duplicate 12 were dropped — stale audio would glitch playback)");

    // RealtimeDropOld never buffers; drain is always empty.
    let mut drained: Vec<(StreamId, Seq, FramePayload)> = Vec::new();
    table.drain_ready_for_connection(CONN, &mut drained);
    assert!(drained.is_empty());
    println!("\nOK: no buffered frames; stale frames dropped, latest delivered.");
}

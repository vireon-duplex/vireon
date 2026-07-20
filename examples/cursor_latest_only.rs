//! Example: `LatestOnly` delivery policy for cursor / pointer position.
//!
//! Scenario: a collaborative whiteboard broadcasts the remote cursor
//! position at 60 Hz. The local renderer polls the latest position
//! once per render frame (~16 ms). When multiple updates land between
//! two drains, only the newest matters — replaying stale positions
//! would make the cursor stutter backwards.
//!
//! Run with:
//! ```text
//! cargo run --example cursor_latest_only
//! ```

#![allow(clippy::print_stdout, clippy::unwrap_used)]

use bytes::Bytes;
use delivery_policy::{
    Decision, DeliveryPolicy, DeliveryPolicyTable, FramePayload,
};
use frame::header::{FrameFlags, MessageType, Seq, StreamId};

const CONN: usize = 0;
const STREAM: StreamId = StreamId::new(7);

fn cursor_payload(seq: u64, x: i32, y: i32) -> FramePayload {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&seq.to_le_bytes());
    bytes.extend_from_slice(&x.to_le_bytes());
    bytes.extend_from_slice(&y.to_le_bytes());
    FramePayload::new(
        MessageType::StateUpdate,
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
    table.bind(CONN, STREAM, DeliveryPolicy::LatestOnly).unwrap();

    println!("LatestOnly — remote cursor (60 Hz producer, ~60 Hz consumer)");
    println!("(x, y) coordinates stream in; only the newest survives between drains\n");

    let positions: &[(u64, i32, i32)] = &[
        (0, 10, 10),
        (1, 25, 18),
        (2, 40, 22),
        (3, 55, 30),
    ];

    for (seq, x, y) in positions {
        let decision = table.apply(CONN, STREAM, Seq::new(*seq), cursor_payload(*seq, *x, *y));
        println!("  apply seq={seq:>2} (x={x:>3}, y={y:>3}) → {}", label(&decision));
    }

    let mut drained: Vec<(StreamId, Seq, FramePayload)> = Vec::new();
    table.drain_ready_for_connection(CONN, &mut drained);
    println!("\n  drain → {} frame(s)", drained.len());
    for (_, seq, payload) in &drained {
        let x = i32::from_le_bytes(payload.bytes[8..12].try_into().unwrap());
        let y = i32::from_le_bytes(payload.bytes[12..16].try_into().unwrap());
        println!("    seq={} → cursor at ({x}, {y})", seq.get());
    }

    // The key invariant: only seq=3 should be drained, even though we
    // applied four frames. The first three were displaced.
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].1.get(), 3);
    println!("\nOK: only the latest cursor position (seq=3) was emitted.");
}

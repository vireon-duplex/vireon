//! Example: `ReliableUnordered` delivery policy for LLM token streams.
//!
//! Scenario: an LLM inference engine emits token chunks in parallel
//! (speculative decoding, multi-worker). Each chunk carries the
//! token's logical `seq` (position in the output). Every unique seq
//! must arrive at the renderer exactly once; duplicates from worker
//! races must be suppressed. Order is irrelevant — the renderer
//! places tokens by seq.

#![allow(clippy::print_stdout)]

use bytes::Bytes;
use delivery_policy::{
    Decision, DeliveryPolicy, DeliveryPolicyTable, FramePayload,
};
use frame::header::{FrameFlags, MessageType, Seq, StreamId};

const CONN: usize = 0;
const STREAM: StreamId = StreamId::new(23);

fn token_chunk(seq: u64, token: &str) -> FramePayload {
    let mut bytes = Vec::with_capacity(8 + token.len());
    bytes.extend_from_slice(&seq.to_le_bytes());
    bytes.extend_from_slice(token.as_bytes());
    FramePayload::new(
        MessageType::TextToken,
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

fn token_of(payload: &FramePayload) -> String {
    String::from_utf8_lossy(&payload.bytes[8..]).into_owned()
}

fn main() {
    let mut table = DeliveryPolicyTable::new();
    table
        .bind(CONN, STREAM, DeliveryPolicy::ReliableUnordered)
        .unwrap();

    println!("ReliableUnordered — LLM token stream (parallel inference workers)");
    println!("Every unique seq delivered once; duplicates dropped.\n");

    // Jumbled arrivals with duplicates.
    let arrivals: &[(u64, &str)] = &[
        (0, "\"Hello\""),
        (2, "\"world\""),
        (1, "\",\""),
        (3, "\"!\""),
        (1, "\",\" (dup)"), // duplicate from a racing worker
        (0, "\"Hello\" (dup)"), // duplicate
        (4, "\"How\""),
    ];

    let mut delivered: Vec<(u64, String)> = Vec::new();
    for (seq, tok) in arrivals {
        let decision = table.apply(CONN, STREAM, Seq::new(*seq), token_chunk(*seq, tok));
        let lbl = label(&decision);
        println!("  apply seq={seq} {tok:<18} → {lbl}");
        if let Decision::Deliver(p) = decision {
            delivered.push((*seq, token_of(&p)));
        }
    }

    println!("\n  delivered tokens (in arrival order, not seq order):");
    for (seq, tok) in &delivered {
        println!("    seq={seq}  {tok}");
    }

    // Five unique seqs delivered (0..=4); two duplicates dropped.
    assert_eq!(delivered.len(), 5);

    // No buffering under ReliableUnordered — everything is Deliver or Drop.
    let mut drained: Vec<(StreamId, Seq, FramePayload)> = Vec::new();
    table.drain_ready_for_connection(CONN, &mut drained);
    assert!(drained.is_empty());

    println!("\nOK: 5 unique tokens delivered; 2 duplicates dropped; no buffering.");
}

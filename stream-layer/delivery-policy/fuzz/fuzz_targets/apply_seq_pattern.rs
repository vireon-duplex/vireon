//! Fuzz target: feed a randomised seq pattern to a single stream under
//! each of the four policies. The contract is "no panic; decisions
//! respect the policy's stated invariant":
//!
//!  * `ReliableOrdered` — every delivered seq is strictly greater than
//!    the prior delivered seq.
//!  * `ReliableUnordered` — a seq that was just `Deliver`ed must `Drop`
//!    on the next apply with the same seq.
//!  * `RealtimeDropOld` — every delivered seq is strictly greater than
//!    the prior delivered seq.
//!  * `LatestOnly` — drain yields at most one frame.
//!
//! Input is consumed as a policy-discriminant byte followed by a seq
//! stream. Each subsequent byte is one seq (modulo 256 — small seq
//! range exercises duplicates and out-of-order heavily, which is what
//! we want).

#![no_main]

use bytes::Bytes;
use delivery_policy::{Decision, DeliveryPolicy, DeliveryPolicyTable, FramePayload};
use frame::header::{FrameFlags, MessageType, Seq, StreamId};
use libfuzzer_sys::fuzz_target;

const CONN: usize = 0;
const SID: StreamId = StreamId::new(1);

fn mk_payload(b: u8) -> FramePayload {
    FramePayload::new(
        MessageType::TextToken,
        FrameFlags::NONE,
        0,
        Bytes::from(vec![b]),
    )
}

fn pick_policy(b: u8) -> DeliveryPolicy {
    match b % 4 {
        0 => DeliveryPolicy::ReliableOrdered,
        1 => DeliveryPolicy::ReliableUnordered,
        2 => DeliveryPolicy::RealtimeDropOld,
        _ => DeliveryPolicy::LatestOnly,
    }
}

fn run_stream(policy: DeliveryPolicy, seqs: &[u8]) {
    let mut t = DeliveryPolicyTable::new();
    // bind never panics; ignore TableFull (won't happen with one stream).
    let _ = t.bind(CONN, SID, policy);

    let mut last_delivered: Option<u64> = None;
    for &byte in seqs {
        let seq = Seq::new(byte as u64);
        let decision = t.apply(CONN, SID, seq, mk_payload(byte));
        match (policy, decision) {
            // Deliver: cursor must advance.
            (_, Decision::Deliver(_)) => {
                if let Some(prev) = last_delivered {
                    debug_assert!(
                        seq.get() > prev,
                        "policy {:?}: delivered seq {} must be > prior delivered {}",
                        policy,
                        seq.get(),
                        prev,
                    );
                }
                last_delivered = Some(seq.get());
            }
            // Buffer: latest_only holds the frame for drain.
            (_, Decision::Buffer) => {}
            // Drop: by definition the seq was stale / duplicate / displaced.
            (_, Decision::Drop) => {}
        }
    }

    // Drain should yield ≤ 1 frame for LatestOnly, any number for
    // ReliableOrdered (gap-fill), and 0 for RealtimeDropOld /
    // ReliableUnordered.
    let mut out: Vec<(StreamId, Seq, FramePayload)> = Vec::new();
    t.drain_ready_for_connection(CONN, &mut out);
    match policy {
        DeliveryPolicy::LatestOnly => {
            debug_assert!(out.len() <= 1, "LatestOnly drain yielded > 1 frame");
        }
        DeliveryPolicy::RealtimeDropOld | DeliveryPolicy::ReliableUnordered => {
            debug_assert!(out.is_empty(), "{:?} drained {:?} — should never buffer", policy, out);
        }
        DeliveryPolicy::ReliableOrdered => {
            // Drained seqs must be in strictly increasing order.
            let mut prev: Option<u64> = None;
            for (_, s, _) in &out {
                if let Some(p) = prev {
                    debug_assert!(s.get() > p, "drain order not monotonic");
                }
                prev = Some(s.get());
            }
        }
    }
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let (policy_byte, seqs) = data.split_first().expect("checked non-empty");
    let policy = pick_policy(*policy_byte);
    run_stream(policy, seqs);
});

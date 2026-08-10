//! # send-policy
//!
//! Per-stream send-side delivery semantics for the Vireon pub/sub runtime.
//!
//! Mirrors [`delivery-policy`] on the egress side: the publisher picks the
//! policy for a logical stream when it issues `MessageType::StreamOpen`,
//! and from that point on every frame the application hands to
//! [`SendPolicyTable::enqueue`] is routed through the per-stream queue
//! strategy that matches the declared policy.
//!
//! ## Policies
//!
//! The wire byte is the same as the receive side (see
//! [`delivery_policy::DeliveryPolicy`]):
//!
//! | Policy              | Egress behaviour                                  | Use case                          |
//! |---------------------|---------------------------------------------------|-----------------------------------|
//! | `ReliableOrdered`     | FIFO queue; emitted in arrival order            | File transfer, RPC, DB replication|
//! | `ReliableUnordered`   | FIFO queue; emitted in arrival order            | Image chunks, AI tokens           |
//! | `RealtimeDropOld`     | Drop if seq < last enqueued; else append        | Audio, video                      |
//! | `LatestOnly`          | Replace pending slot; `dequeue` emits newest    | Cursor, mouse, GPS                |
//!
//! ## Why the receive-side policy byte is reused
//!
//! A stream's policy is a *contract* between publisher and subscriber —
//! both sides need to agree on the semantics at `StreamOpen` time, so the
//! policy byte is consumed by both [`delivery_policy::DeliveryPolicyTable`]
//! (egress) and `delivery-policy`'s table (ingress). The send-policy crate
//! re-exports [`DeliveryPolicy`] and [`StreamOpenMeta`] from
//! `delivery-policy` so wire encoding stays in exactly one place.
//!
//! ## Layering
//!
//! This crate is consumed by `stream-scheduler` (per-priority egress
//! selection). The scheduler owns one [`SendPolicyTable`] per reactor and
//! threads it through its per-tick selection loop. The table is keyed by
//! `(conn_idx, StreamId)` — the **logical** stream id from the frame
//! header, mirroring the receive-side table.

#![allow(clippy::module_inception)]

pub mod decision;
pub mod error;
pub mod metrics;
pub mod queue;
pub mod state;
pub mod table;

// ── Re-exports ────────────────────────────────────────────────────

pub use decision::{DequeueOutcome, EnqueueDecision, FramePayload};
pub use error::SendPolicyError;
pub use metrics::{MetricsSnapshot, SendPolicyMetrics};
pub use queue::StreamQueue;
pub use state::PerStreamSendState;
pub use table::SendPolicyTable;

// Re-export the wire-format types from delivery-policy so consumers of
// this crate don't need a second dependency for the policy enum. The wire
// byte is shared between send and receive sides — it represents the
// contract a stream declares at `StreamOpen` time, and both ends must
// interpret it identically.
pub use delivery_policy::{DeliveryPolicy, StreamOpenMeta, StreamPriority};

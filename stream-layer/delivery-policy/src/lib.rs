//! # delivery-policy
//!
//! Per-stream receive-side delivery semantics for the Vireon pub/sub runtime.
//!
//! A publisher declares the policy for a logical stream at `StreamOpen` time
//! by encoding a single [`DeliveryPolicy`] byte in the `StreamOpen` payload.
//! From that point on, every data frame the receiver decodes for that stream
//! is routed through [`DeliveryPolicyTable::apply`], which decides whether to
//! forward, buffer, or drop the frame according to the stream's policy.
//!
//! ## Policies
//!
//! | Policy              | Behaviour                                       | Use case                          |
//! |---------------------|-------------------------------------------------|-----------------------------------|
//! | [`ReliableOrdered`]   | In-order delivery, bounded reorder buffer     | File transfer, RPC, DB replication|
//! | [`ReliableUnordered`] | Deliver all immediately, dedup by `Seq`       | Image chunks, AI tokens           |
//! | [`RealtimeDropOld`]   | Drop if `seq ≤ last_delivered`                | Audio, video                      |
//! | [`LatestOnly`]        | Replace pending slot with newest; drain later | Cursor, mouse, GPS                |
//!
//! [`ReliableOrdered`]: DeliveryPolicy::ReliableOrdered
//! [`ReliableUnordered`]: DeliveryPolicy::ReliableUnordered
//! [`RealtimeDropOld`]: DeliveryPolicy::RealtimeDropOld
//! [`LatestOnly`]: DeliveryPolicy::LatestOnly
//!
//! ## Wire format
//!
//! `MessageType::StreamOpen` payload (defined in [`policy::StreamOpenMeta`]):
//!
//! ```text
//! byte 0: DeliveryPolicy discriminant (0x01..=0x04)
//! byte 1..: reserved (forward-compat for QoS, priority, topic binding)
//! ```
//!
//! ## Layering
//!
//! This crate is consumed by `quic-engine`'s stream bridge. The engine
//! owns one [`DeliveryPolicyTable`] per reactor and threads it through
//! `process_single_stream`. Policy state is keyed by `(conn_idx, StreamId)`
//! — the **logical** stream id from the frame header, not the QUIC
//! transport stream id (multiple logical streams may share one QUIC stream
//! in the future).

#![allow(clippy::module_inception)]

pub mod decision;
pub mod error;
pub mod policy;
pub mod state;
pub mod table;

// ── Re-exports ────────────────────────────────────────────────────

pub use decision::{Decision, FramePayload};
pub use error::DeliveryPolicyError;
pub use policy::{DeliveryPolicy, StreamOpenMeta};
pub use state::PerStreamState;
pub use table::DeliveryPolicyTable;

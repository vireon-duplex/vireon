//! # stream-manager
//!
//! Owner of every QUIC stream on a connection. Tracks per-stream
//! metadata, lifecycle, FSM, ID allocation, send/recv buffers, and
//! per-stream flow control credit. The Stream Manager is
//! **protocol-agnostic** — wire encoding (STREAM / RESET_STREAM /
//! MAX_STREAM_DATA / STOP_SENDING) stays in the transport layer
//! (`quic-engine` wrapping `quiche`); this crate emits scheduling
//! decisions + stream events.
//!
//! # Architecture (one-line)
//!
//! ```text
//! Application ─► Session ─► [Stream Manager] ─► Message / Transport
//!                                │
//!                                ├─ StreamIdAllocator (RFC 9000 §3.2)
//!                                ├─ StreamTable      (slab + index)
//!                                ├─ Stream FSM       (RFC 9000 §3.1/§3.3)
//!                                ├─ SendBuffer       (offset + FIN)
//!                                ├─ RecvBuffer       (gap + reassembly)
//!                                ├─ StreamFlowControl (RFC 9000 §4.1)
//!                                └─ StreamPriority   (RFC 9218)
//! ```
//!
//! See `DESIGN.md` for the full architecture specification.
//!
//! # Layer map
//!
//! | # | Module | Phase | Responsibility |
//! |---|--------|-------|----------------|
//! | 1 | [`error`]    | 0 ✅ | `StreamManagerError` thiserror enum |
//! | 2 | [`config`]   | 0 ✅ | `StreamManagerConfig`, `Preset` |
//! | 3 | [`id`]       | 0 ✅ | `StreamId` (RFC 9000 §3.2) + `StreamType` |
//! | 4 | [`priority`] | 0 ✅ | `StreamPriority` (RFC 9218) + `PriorityUpdate` |
//! | 5 | [`state`]    | 0 ✅ | `SendState`, `RecvState`, `StreamState` (RFC 9000 §3.1) |
//! | 6 | [`slab`]     | 0 ✅ | `StreamSlab` (slot + generation) |
//! | 7 | [`stats`]    | 0 ✅ | `StreamStats` atomics, `StatsSnapshot` |
//! | 8 | [`health`]   | 0 ✅ | `HealthReport`, `HealthState` |
//! | 9 | [`stream`]   | 1 | `Stream` struct (metadata + state) |
//! | 10| [`table`]    | 1 | `StreamTable` (slab + FxHashMap index) |
//! | 11| [`allocator`]| 1 | `StreamIdAllocator` |
//! | 12| [`send`]     | 2 | `SendBuffer` (offset + FIN) |
//! | 13| [`recv`]     | 2 | `RecvBuffer` (gap + reassembly) |
//! | 14| [`flow_control`] | 2 | `StreamFlowControl` (RFC 9000 §4.1) |
//! | 15| [`lifecycle`]| 3 | `StreamLifecycle` (create → drain → destroy) |
//! | 16| [`event`]    | 1 | `StreamEvent` enum |
//! | 17| [`builder`]  | 1 | `StreamManagerBuilder` (fluent) |
//! | 18| [`manager`]  | 1 | `StreamManager` facade |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations; modules 9–18 ship as
//! typed skeletons with `//!` docs pointing at `DESIGN.md`. The
//! functional stream table + FSM land in Phase 1.

#![allow(clippy::module_inception)]

pub mod allocator;
pub mod builder;
pub mod config;
pub mod error;
pub mod event;
pub mod flow_control;
pub mod health;
pub mod id;
pub mod lifecycle;
pub mod manager;
pub mod priority;
pub mod recv;
pub mod send;
pub mod slab;
pub mod state;
pub mod stats;
pub mod stream;
pub mod table;

// ── Re-exports ────────────────────────────────────────────────────

pub use config::{Preset, StreamManagerConfig};
pub use error::StreamManagerError;
pub use health::{HealthReport, HealthState};
pub use id::{StreamId, StreamType};
pub use priority::{PriorityUpdate, StreamPriority, URGENCY_DEFAULT, URGENCY_MAX, URGENCY_MIN};
pub use slab::{SlotGeneration, SlotIndex, StreamSlab};
pub use state::{RecvState, SendState, StreamState};
pub use stats::{StatsSnapshot, StreamStats};

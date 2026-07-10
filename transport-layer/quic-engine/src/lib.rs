//! # quic-engine
//!
//! Production-grade **QUIC Transport Layer** for the Vireon runtime.
//!
//! Wraps Cloudflare's [`quiche`] for the protocol core (TLS 1.3,
//! AEAD, packet parsing, loss recovery, congestion control, ACK
//! processing, flow control, stream scheduling) and adds the
//! Vireon-native integration substrate: CID-keyed connection table,
//! io_uring-native packet I/O via `cqe-processor` + `send-queue`,
//! GSO batching, pacing, path validation, timer binding, and
//! thread-per-core reactor integration.
//!
//! ## Status (Phase 0 — architecture review + skeleton)
//!
//! This crate currently ships **only the design and module skeleton**.
//! Functional QUIC behavior lands in Phases 1–15 as described in
//! [`DESIGN.md`](../DESIGN.md). Each module below is a placeholder
//! tagged with its target implementation phase.
//!
//! ## Quick start (target API — Phase 13+)
//!
//! ```no_run
//! use quic_engine::{QuicEngineBuilder, QuicPreset};
//!
//! // One engine per Reactor core (thread-per-core, shared-nothing).
//! let mut engine = QuicEngineBuilder::from_preset(QuicPreset::BareMetal)
//!     .reactor_id(0)
//!     .build()
//!     .expect("engine");
//! ```
//!
//! ## Layer map
//!
//! | Layer | Module | Phase | Responsibility |
//! |-------|--------|-------|----------------|
//! | 1 | [`error`]               | —   | `QuicError` enum + `is_fatal` / `is_retryable` |
//! | 2 | [`config`]              | —   | `QuicConfig`, `QuicPreset`, capacity / timeout constants |
//! | 3 | [`cid`]                 | 1   | `QuicCid` — connection identifier + rotation policy |
//! | 4 | [`token`]               | 1   | `StatelessResetToken`, `RetryToken` |
//! | 5 | [`conn_id_table`]       | 1   | `ConnIdTable` — CID → QuicConnection lookup |
//! | 6 | [`connection`]          | 1   | `QuicConnection` — wraps `quiche::Connection` |
//! | 7 | [`state`]               | 1   | `QuicConnState` — handshake / connected / closing / draining |
//! | 8 | [`packet`]              | 2   | `PacketRouter` — long / short header routing, coalescing |
//! | 9 | [`crypto`]              | 3   | `CryptoContext` — TLS 1.3 secrets handoff, keylog, 0-RTT |
//! | 10 | [`loss_recovery`]      | 4   | `LossRecovery` — wraps quiche loss detection (RFC 9002 §6) |
//! | 11 | [`congestion`]         | 5   | `CongestionController` — CUBIC / Reno / BBR / BBRv2 + ECN |
//! | 12 | [`ack_manager`]        | 6   | `AckManager` — delayed ACK, ACK Frequency extension |
//! | 13 | [`flow_control`]       | 7   | `FlowController` — MAX_DATA / MAX_STREAM_DATA / blocked |
//! | 14 | [`stream`]             | 8   | `Stream`, `StreamId` — bidirectional / unidirectional |
//! | 15 | [`stream_scheduler`]   | 8   | `StreamScheduler` — priority + fair queuing across streams |
//! | 16 | [`frame`]              | 9   | `Frame` — extension frame policy, DATAGRAM (RFC 9221) |
//! | 17 | [`packet_scheduler`]   | 10  | `PacketScheduler` — egress ordering: ACK > probe > app |
//! | 18 | [`pacing`]             | 10  | `Pacer` — token-bucket integration with `send-queue` |
//! | 19 | [`gso`]                | 10  | `GsoBatcher` — `UDP_SEGMENT` batching |
//! | 20 | [`timer_binding`]      | 11  | `TimerBinding` — quiche timeout → `timer-wheel` |
//! | 21 | [`path`]               | 12  | `PathManager` — PATH_CHALLENGE / RESPONSE, NAT rebinding |
//! | 22 | [`migration`]          | 12  | `Migration` — connection migration + multipath hooks |
//! | 23 | [`shard_binding`]      | 13  | `ShardBinding` — QuicConnection ↔ `connection_shard::ConnectionShard` |
//! | 24 | [`engine`]             | 13  | `QuicEngine` facade — one per Reactor core |
//! | 25 | [`builder`]            | 13  | `QuicEngineBuilder` (fluent + `from_preset`) |
//! | 26 | [`stats`]              | 14  | `StatsCollector` (atomic), `StatsSnapshot` (`Copy`) |
//! | 27 | [`health`]             | 14  | `HealthReport`, `HealthState` |
//!
//! See [`DESIGN.md`](../DESIGN.md) for the full architecture review,
//! RFC compliance matrix, hot-path analysis, production readiness
//! scorecard, and the 15-phase implementation roadmap.

#![allow(clippy::module_name_repetitions)]
#![allow(dead_code)] // Phase 0: stubs intentionally minimal until Phases 1–15.

// ── Core skeleton modules ────────────────────────────────────────
pub mod ack_manager;
pub mod builder;
pub mod cid;
pub mod config;
pub mod congestion;
pub mod conn_id_table;
pub mod connection;
pub mod crypto;
pub mod engine;
pub mod error;
pub mod flow_control;
pub mod frame;
pub mod gso;
pub mod health;
pub mod loss_recovery;
pub mod migration;
pub mod pacing;
pub mod packet;
pub mod packet_scheduler;
pub mod path;
pub mod shard_binding;
pub mod state;
pub mod stats;
pub mod stream;
pub mod stream_bridge;
pub mod stream_scheduler;
pub mod timer_binding;
pub mod token;

// ── Re-exports ───────────────────────────────────────────────────
pub use builder::QuicEngineBuilder;
pub use config::{Preset as QuicPreset, QuicConfig};
pub use crypto::build_quiche_config;
pub use engine::{QuicEngine, QuicEvent};
pub use error::QuicError;

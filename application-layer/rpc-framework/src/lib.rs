//! # rpc-framework
//!
//! **RPC framework** for the Vireon application layer. Provides unary,
//! server-streaming, client-streaming, and bidirectional RPC on QUIC
//! streams.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   Client Request ──► [RPC Framework] ──► Server Handler
//!                        │
//!                        ├─ MethodRegistry   (RPCMethodId → Handler)
//!                        ├─ Dispatcher       (request routing)
//!                        ├─ StreamManager    (QUIC stream lifecycle)
//!                        ├─ StatusCodes      (Ok, Error, Cancelled, etc.)
//!                        ├─ TimeoutManager   (per-call deadlines)
//!                        └─ StatsCollector   (calls, bytes, latency)
//! ```
//!
//! See `DESIGN.md` for the full 15-section specification.
//!
//! # Layer map
//!
//! | #  | Module        | Phase | Responsibility |
//! |----|---------------|-------|----------------|
//! | 1  | [`error`]     | 0 ✅  | `RpcError` thiserror enum |
//! | 2  | [`config`]    | 0 ✅  | `RpcConfig`, `Preset` |
//! | 3  | [`method`]    | 0 ✅  | `RpcMethodId`, `RpcMethodType` |
//! | 4  | [`handle`]    | 0 ✅  | `StreamHandle` newtype |
//! | 5  | [`status`]    | 0 ✅  | `RpcStatus` enum |
//! | 6  | [`stats`]     | 0 ✅  | `RpcStats` atomics, `StatsSnapshot` |
//! | 7  | [`health`]    | 0 ✅  | `HealthReport`, `HealthState` |
//! | 8  | [`builder`]   | 0 ✅  | `RpcFrameworkBuilder` (fluent) |
//! | 9  | [`server`]    | 1    | `RpcServer` facade |
//! | 10 | [`client`]    | 1    | `RpcClient` facade |
//! | 11 | [`dispatcher`]| 1   | `MethodDispatcher` (routing) |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations. Modules 9–11 ship as typed
//! skeletons. Functional server + client + dispatcher land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod builder;
pub mod client;
pub mod config;
pub mod dispatcher;
pub mod error;
pub mod handle;
pub mod health;
pub mod method;
pub mod server;
pub mod stats;
pub mod status;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::RpcFrameworkBuilder;
pub use client::RpcClient;
pub use config::{Preset, RpcConfig};
pub use dispatcher::MethodDispatcher;
pub use error::RpcError;
pub use handle::StreamHandle;
pub use health::{HealthReport, HealthState};
pub use method::{RpcMethodId, RpcMethodType};
pub use server::RpcServer;
pub use stats::{RpcStats, StatsSnapshot};
pub use status::RpcStatus;

// Re-export key adjacent-layer types for convenience.
pub use session_manager::SessionId;
pub use stream_manager::StreamId;

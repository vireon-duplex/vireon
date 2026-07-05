//! # connection-shard
//!
//! Production-grade **Connection Ownership Layer** for the Vireon
//! runtime. Each Reactor core owns exactly one [`ConnectionShard`] and
//! all connections on that core. There is no shared mutable connection
//! state, no `Arc<Mutex>`, no cross-core locks.
//!
//! ## Quick start
//!
//! ```
//! use connection_shard::{ConnectionShardBuilder, ConnectionSpec};
//! use std::net::SocketAddr;
//!
//! let mut shard = ConnectionShardBuilder::new()
//!     .reactor_id(0)
//!     .capacity(4096)
//!     .build()
//!     .expect("build shard");
//!
//! let spec = ConnectionSpec::new(42, "10.0.0.1:443".parse().unwrap());
//! let id = shard.create(spec).expect("create");
//! assert_eq!(shard.lookup_by_stream(42), Some(id));
//! ```

#![allow(clippy::module_name_repetitions)]

pub mod backpressure;
pub mod config;
pub mod connection;
pub mod error;
pub mod id;
pub mod priority;
pub mod spec;
pub mod state;
pub mod stats;

// Phase 2+ modules (declared now so docs render; implemented next)
pub mod builder;
pub mod health;
pub mod migration;
pub mod shard;
pub mod strategy;

// ── Re-exports ──────────────────────────────────────────────────

pub use backpressure::BackpressureLevel;
pub use builder::ConnectionShardBuilder;
pub use config::{Preset, ShardConfig};
pub use connection::ConnectionSnapshot;
pub use error::ShardError;
pub use health::{HealthReport, HealthState};
pub use id::{ConnectionId, NIL_ID};
pub use migration::MigrationRequest;
pub use priority::ConnectionPriority;
pub use shard::ConnectionShard;
pub use spec::ConnectionSpec;
pub use state::ConnectionState;
pub use stats::{StatsCollector, StatsSnapshot};
pub use strategy::{
    JumpHashSharding, LocalSharding, ModuloSharding, RendezvousSharding, ShardingStrategy,
};

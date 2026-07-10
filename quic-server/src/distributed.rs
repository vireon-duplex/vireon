//! Distributed server mode — coordination layer (future).
//!
//! In the full implementation, distributed mode adds:
//! - Session registry shared across nodes (via `session-registry` crate)
//! - Inter-node gossip for session migration
//! - Consistent hashing for session → node routing
//! - Health-check heartbeats between nodes
//!
//! Currently, distributed mode delegates to multi-worker. The
//! coordination layer will be wired in when the session-registry
//! crate reaches functional maturity.
//!
//! ## Planned architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────┐
//! │                  DistributedServer                   │
//! │                                                       │
//! │  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐ │
//! │  │ Worker 0    │  │ Worker 1    │  │ Worker N     │ │
//! │  │ (core 0)    │  │ (core 1)    │  │ (core N)     │ │
//! │  └──────┬──────┘  └──────┬──────┘  └──────┬───────┘ │
//! │         │                │                │          │
//! │  ┌──────┴────────────────┴────────────────┴───────┐ │
//! │  │            Session Registry (local)             │ │
//! │  └───────────────────┬────────────────────────────┘ │
//! │                      │                               │
//! │  ┌───────────────────┴────────────────────────────┐ │
//! │  │          Inter-Node Coordination                │ │
//! │  │  (gossip, consistent hashing, health probes)    │ │
//! │  └────────────────────────────────────────────────┘ │
//! └──────────────────────────────────────────────────────┘
//! ```

//! Library facade for the `quic-server` crate.
//!
//! The binary target (`src/main.rs`) owns the runtime entry point and
//! additional modules (cli, config, server, stack, worker, …) that the
//! bench does not need. This library exposes just the layer stack so
//! that integration benches in `benches/` can drive the same
//! `ApplicationLayer` / `VireonDispatcher` hot path the production
//! binary uses.
//!
//! ```text
//! quic_server
//!   ├─ error    (ServerError — referenced by layers::*)
//!   └─ layers
//!        ├─ ApplicationLayer, FrameContext, OutgoingFrame, RuntimeLayer
//!        └─ VireonDispatcher  (Phase 3 send-policy + Phase 7 ACL)
//! ```

// Warn on .unwrap() / .expect() in production code.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

pub mod acl_loader;
pub mod audit;
pub mod cluster;
pub mod conf;
pub mod error;
pub mod layers;
pub mod log_throttle;
pub mod metrics;
pub mod replay;
pub mod socket_registry;

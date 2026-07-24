//! Layer module — one struct per architectural layer.
//!
//! Each layer owns the layer below it, expressing the top-down
//! dependency chain through nested ownership:
//!
//! ```text
//! ApplicationLayer                      (application-layer)
//!   ├─ SessionLayer                     (session-layer)
//!   │    └─ StreamLayer                 (stream-layer)
//!   │         └─ MessageLayer           (message-layer)
//!   └─ dispatcher: VireonDispatcher     (application dispatch)
//! ```
//!
//! The `RuntimeLayer` is separate shared infrastructure (not nested
//! inside the application chain). The transport-layer (`QuicEngine`),
//! io-layer (`io::Reactor`), and kernel-layer (`kernel-nic`) are
//! separate crates used directly by the `VireonStack` and `Worker`.

pub mod application;
pub mod message;
pub mod runtime;
pub mod session;
pub mod stream;

// Re-export the types used by stack.rs and by integration benches.
// The binary crate (which still owns a duplicate copy of this module
// via `mod layers;` in main.rs) does not use `AppDispatcher` or
// `VireonDispatcher` directly — they're consumed by the lib's
// `benches/pubsub_dispatch.rs`. The allow silences the binary's
// duplicate-compile view without affecting the lib's public API.
#[allow(unused_imports)]
pub use application::{
    AppDispatcher, ApplicationLayer, FrameContext, InterWorkerPublish, OutgoingFrame,
    VireonDispatcher,
};
pub use runtime::RuntimeLayer;

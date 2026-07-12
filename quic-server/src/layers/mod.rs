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

// Re-export the types used by stack.rs.
pub use application::{ApplicationLayer, FrameContext};
pub use runtime::RuntimeLayer;

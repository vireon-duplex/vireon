//! Client-side layer stack — mirrors the server's application/session/
//! stream/message chain but optimized for the benchmark client.
//!
//! ```text
//! ClientApplicationLayer
//!   └─ ClientSessionLayer
//!        └─ ClientStreamLayer
//!             └─ ClientMessageLayer (stateless)
//! ```
//!
//! [`ClientStack`] wraps the chain and provides the encode/coalesce/
//! decode machinery that `worker.rs` drives.

pub mod application;
pub mod client_stack;
pub mod message;
pub mod session;
pub mod stream;

#[allow(unused_imports)]
pub use application::ClientApplicationLayer;
#[allow(unused_imports)]
pub use client_stack::ClientStack;
#[allow(unused_imports)]
pub use message::{ClientMessageLayer, FrameAction};
#[allow(unused_imports)]
pub use session::ClientSessionLayer;
#[allow(unused_imports)]
pub use stream::ClientStreamLayer;

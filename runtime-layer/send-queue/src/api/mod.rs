//! Public API (Layer 12): [`SendQueue`] facade, [`SendQueueBuilder`],
//! and application-facing metadata types.

pub mod builder;
pub mod facade;
pub mod meta;

pub use builder::SendQueueBuilder;
pub use facade::SendQueue;
pub use meta::{PacketId, PacketMeta};

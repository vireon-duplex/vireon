//! Ingress ring and packet-slot types (Layer 3).
//!
//! The [`IngressRing`] is a bounded single-producer single-consumer ring
//! of [`PacketSlot`]s. In the thread-per-core model, the application task
//! is the sole producer and the reactor is the sole consumer.

pub mod ring;
pub mod slot;

pub use ring::IngressRing;
pub use slot::{PacketSlot, SlotState};

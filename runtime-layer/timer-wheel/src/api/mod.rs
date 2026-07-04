//! Public API (Layer 9).
//!
//! [`facade`] owns the [`TimerWheel`] handle, the [`TimerWheelBuilder`], and
//! the [`TimerHandle`] returned to callers.

pub mod facade;

pub use facade::{TimerHandle, TimerWheel, TimerWheelBuilder};

//! Backpressure subsystem (Layer 23).
//!
//! - [`policy`] — byte thresholds for the allocation gate.
//! - [`waiter`] — `Condvar`-based park / notify.

pub mod policy;
pub mod waiter;

pub use policy::BackpressurePolicy;
pub use waiter::Waiter;

//! Debugging and diagnostics subsystem.
//!
//! - [`leak`] — outstanding-allocation tracking with optional backtraces.
//! - [`validate`] — side-table double-free / unknown-pointer detection.
//! - [`trace`] — per-operation `tracing` events.

pub mod leak;
pub mod trace;
pub mod validate;

pub use leak::{LeakEntry, LeakTracker};
pub use validate::Validator;

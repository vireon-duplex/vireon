//! Maintenance subsystem (Layers 16-19).
//!
//! - [`pressure`] — RSS monitoring and [`PressureLevel`](crate::PressureLevel) classification.
//! - [`shrink`] — cache trimming driven by pressure and policy.
//! - [`tuning`] — adaptive batch / cache-size recommendations.
//! - [`worker`] — background thread that ties everything together.

pub mod pressure;
pub mod shrink;
pub mod tuning;
pub mod worker;

pub use worker::MaintenanceWorker;

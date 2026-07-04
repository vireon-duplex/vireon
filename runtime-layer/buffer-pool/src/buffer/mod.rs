//! Public API surface for the pool.
//!
//! - [`config`] — [`BufferPoolConfig`] value type and defaults.
//! - [`policy`] — [`ZeroingPolicy`], [`ShrinkPolicy`], [`PressureLevel`].
//! - [`pool`] — [`BufferPool`] handle and internal state.
//! - [`builder`] — [`BufferPoolBuilder`] for tuned construction.
//!
//! [`ZeroingPolicy`]: policy::ZeroingPolicy
//! [`ShrinkPolicy`]: policy::ShrinkPolicy
//! [`PressureLevel`]: policy::PressureLevel

pub mod builder;
pub mod config;
pub mod policy;
pub mod pool;

pub use builder::BufferPoolBuilder;
pub use config::BufferPoolConfig;
pub use pool::BufferPool;

//! Allocation hierarchy (Layers 3-6).
//!
//! - [`size_class`] / [`classifier`] — map a request size to a fixed class.
//! - [`slab`] — bitmap-tracked slot container.
//! - [`arena`] — slab owner and address-range index.
//! - [`huge`] — direct allocation above the class table.
//! - [`allocator`] — top-level orchestrator routing to arena or huge.

pub mod allocator;
pub mod arena;
pub mod classifier;
pub mod huge;
pub mod size_class;
pub mod slab;

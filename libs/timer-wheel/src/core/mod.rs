//! Core wheel data structures (Layers 3-5).
//!
//! - [`allocator`] — slab allocator with an O(1) free-list.
//! - [`timer_entry`] — 24-byte compact timer record.
//! - [`bucket`] — intrusive doubly-linked-list bucket operations.
//! - [`wheel`] — hierarchical timing wheel with bitwise slot lookup and cascading.

pub mod allocator;
pub mod bucket;
pub mod timer_entry;
pub mod wheel;

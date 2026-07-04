//! Memory primitives shared across the allocation hierarchy.
//!
//! - [`alignment`] — power-of-two alignment helpers.
//! - [`page`] — page-size constants.
//! - [`zeroing`] — release-time clearing (Layer 10).
//! - [`recycle`] — slot recycling hooks (Layer 11).

pub mod alignment;
pub mod page;
pub mod recycle;
pub mod zeroing;

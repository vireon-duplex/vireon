//! NUMA awareness (Layer 22).
//!
//! - [`node`] — [`NodeId`] type and [`current_node`] detection.
//! - [`bind`] — memory binding via the Linux `mbind` syscall.
//!
//! Both modules compile to single-node stubs when the `numa` cargo feature is
//! disabled.
//!
//! [`current_node`]: node::current_node
//! [`bind`]: bind::bind

pub mod bind;
pub mod node;

pub use bind::{allocate_node, bind, deallocate, migrate};
pub use node::{NodeId, current_node, is_numa_available};

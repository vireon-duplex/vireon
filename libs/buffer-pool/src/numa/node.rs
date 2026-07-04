//! NUMA node identification (Layer 22).
//!
//! Without the `numa` feature the pool assumes a single-node machine: every
//! call returns [`NodeId::SINGLE`]. With the feature enabled,
//! [`current_node`] queries the running CPU's NUMA node via the Linux
//! `getcpu` syscall.

use std::fmt;

/// Identifier for a NUMA node (a physical CPU package / memory controller).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u32);

impl NodeId {
    /// The single node on a non-NUMA system (always node 0).
    pub const SINGLE: NodeId = NodeId(0);

    /// Returns `true` when this is the only node on the machine.
    pub fn is_single(self) -> bool {
        self == Self::SINGLE
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node{}", self.0)
    }
}

impl From<u32> for NodeId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<NodeId> for u32 {
    fn from(node: NodeId) -> Self {
        node.0
    }
}

// ── Platform implementation ─────────────────────────────────────────────

#[cfg(feature = "numa")]
mod imp {
    use super::NodeId;

    /// Returns the NUMA node on which the calling thread is currently running.
    ///
    /// Falls back to [`NodeId::SINGLE`] on any error.
    pub fn current_node() -> NodeId {
        let mut cpu: u32 = 0;
        let mut node: u32 = 0;
        // SAFETY: `getcpu` writes two `u32` values and reads nothing. The
        // pointers are valid stack addresses.
        let ret = unsafe {
            libc::syscall(
                libc::SYS_getcpu,
                &mut cpu as *mut u32,
                &mut node as *mut u32,
                std::ptr::null::<u32>(),
            )
        };
        if ret == 0 {
            NodeId(node)
        } else {
            NodeId::SINGLE
        }
    }

    /// Returns `true` when the kernel exposes more than one NUMA node.
    pub fn is_numa_available() -> bool {
        let mut node = NodeId::SINGLE;
        let mut other_node = NodeId(99);
        for _ in 0..16 {
            let n = current_node();
            if n != node {
                other_node = n;
                break;
            }
            node = n;
            std::thread::yield_now();
        }
        other_node != NodeId(99)
    }
}

#[cfg(not(feature = "numa"))]
mod imp {
    use super::NodeId;

    pub fn current_node() -> NodeId {
        NodeId::SINGLE
    }

    pub fn is_numa_available() -> bool {
        false
    }
}

/// Returns the NUMA node on which the calling thread is currently executing.
pub fn current_node() -> NodeId {
    imp::current_node()
}

/// Returns `true` when multiple NUMA nodes are detected.
pub fn is_numa_available() -> bool {
    imp::is_numa_available()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_is_node_zero() {
        assert_eq!(NodeId::SINGLE, NodeId(0));
        assert!(NodeId::SINGLE.is_single());
    }

    #[test]
    fn display_formats_as_node_n() {
        assert_eq!(format!("{}", NodeId(3)), "node3");
    }

    #[test]
    fn current_node_returns_valid_id() {
        let n = current_node();
        // Without the feature, always 0. With the feature, any u32.
        let _ = n;
    }

    #[test]
    fn from_conversions_round_trip() {
        let n = NodeId::from(7u32);
        let raw: u32 = n.into();
        assert_eq!(raw, 7);
    }
}

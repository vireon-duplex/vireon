//! Typed errors for the `buffer-pool` crate (Layer 25).
//!
//! Every fallible public operation returns [`BufferPoolError`]; no site in
//! non-test code panics (per `AGENTS.md` rule 1).

use crate::allocator::size_class::ClassId;

/// Errors returned by [`BufferPool`](crate::buffer::BufferPool) operations.
#[derive(Debug, thiserror::Error)]
pub enum BufferPoolError {
    /// The pool rejected the request under memory pressure (`try_get` path).
    #[error("buffer pool rejected request for {size} bytes under pressure")]
    Rejected {
        /// Requested size in bytes.
        size: usize,
    },

    /// The requested size exceeds the absolute maximum the pool will serve.
    #[error("requested {size} bytes exceeds pool maximum {max}")]
    Oversized {
        /// Requested size.
        size: usize,
        /// Pool maximum.
        max: usize,
    },

    /// A blocking [`get`](crate::buffer::BufferPool::get) timed out.
    #[error("timed out after {timeout_ms} ms waiting for a {size}-byte buffer")]
    Timeout {
        /// Requested size.
        size: usize,
        /// Elapsed wait in milliseconds.
        timeout_ms: u64,
    },

    /// The pool has been closed and refuses new acquisitions.
    #[error("buffer pool is closed")]
    PoolClosed,

    /// A buffer's internal invariant was violated (magic mismatch or overrun).
    #[error("corrupted buffer at {address:#x} (class {class:?})")]
    CorruptedBuffer {
        /// Address of the offending slot.
        address: usize,
        /// Size class the slot belonged to.
        class: ClassId,
    },

    /// The underlying allocator could not satisfy the request.
    #[error("out of memory: requested {size} bytes")]
    OutOfMemory {
        /// Requested size.
        size: usize,
    },

    /// A buffer was returned to a pool that did not allocate it.
    #[error("buffer belongs to a different pool (class {class:?})")]
    WrongPool {
        /// Size class reported by the buffer.
        class: ClassId,
    },

    /// A buffer was released twice (use-after-drop).
    #[error("double free detected at {address:#x} (class {class:?})")]
    DoubleFree {
        /// Address of the offending slot.
        address: usize,
        /// Size class the slot belonged to.
        class: ClassId,
    },

    /// The supplied configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_informative() {
        assert_eq!(
            BufferPoolError::Rejected { size: 1024 }.to_string(),
            "buffer pool rejected request for 1024 bytes under pressure"
        );
        assert_eq!(
            BufferPoolError::Oversized {
                size: 999,
                max: 256
            }
            .to_string(),
            "requested 999 bytes exceeds pool maximum 256"
        );
        assert_eq!(
            BufferPoolError::Timeout {
                size: 64,
                timeout_ms: 10
            }
            .to_string(),
            "timed out after 10 ms waiting for a 64-byte buffer"
        );
        assert_eq!(
            BufferPoolError::PoolClosed.to_string(),
            "buffer pool is closed"
        );
    }

    #[test]
    fn classed_variants_format_address_and_class() {
        let err = BufferPoolError::CorruptedBuffer {
            address: 0xDEAD,
            class: ClassId(3),
        };
        assert!(err.to_string().contains("0xdead"));
        assert!(err.to_string().contains("class ClassId(3)"));

        let err = BufferPoolError::OutOfMemory { size: 4096 };
        assert_eq!(err.to_string(), "out of memory: requested 4096 bytes");
    }

    #[test]
    fn invalid_config_carries_context() {
        let err = BufferPoolError::InvalidConfig("batch exceeds local cache".into());
        assert_eq!(
            err.to_string(),
            "invalid configuration: batch exceeds local cache"
        );
    }
}

//! Per-operation tracing helpers (Layer 21).
//!
//! Thin wrappers around `tracing::trace!` so call sites read clearly. All
//! functions are no-ops when no `tracing` subscriber is active.

use crate::allocator::size_class::ClassId;

/// Emits a trace event for a fresh allocation from the arena or huge path.
pub fn trace_alloc(size: usize, class: ClassId) {
    tracing::trace!(
        target: "buffer_pool::alloc",
        size,
        class = ?class,
        "allocate"
    );
}

/// Emits a trace event for a release.
pub fn trace_free(size: usize, class: ClassId) {
    tracing::trace!(
        target: "buffer_pool::free",
        size,
        class = ?class,
        "release"
    );
}

/// Emits a trace event for a thread-local cache hit.
pub fn trace_cache_hit(class: ClassId) {
    tracing::trace!(
        target: "buffer_pool::cache",
        class = ?class,
        "hit"
    );
}

/// Emits a trace event for a thread-local cache miss.
pub fn trace_cache_miss(class: ClassId) {
    tracing::trace!(
        target: "buffer_pool::cache",
        class = ?class,
        "miss"
    );
}

/// Emits a trace event for a global-cache reuse.
pub fn trace_cache_reuse(class: ClassId) {
    tracing::trace!(
        target: "buffer_pool::cache",
        class = ?class,
        "reuse"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_functions_do_not_panic() {
        trace_alloc(64, ClassId(0));
        trace_free(64, ClassId(0));
        trace_cache_hit(ClassId(0));
        trace_cache_miss(ClassId(0));
        trace_cache_reuse(ClassId(0));
    }
}

//! Thread-local call-tree context.
//!
//! Maintains a per-thread stack of function names so each [`Guard`]
//! knows its caller. The (caller, callee) edges are accumulated
//! thread-locally and flushed alongside timing data — they power the
//! flamegraph / call-tree export.

use std::cell::RefCell;

use rustc_hash::FxHashMap;

thread_local! {
    /// Per-thread call stack. `stack[i]` is the function name at depth `i`.
    static CALL_STACK: RefCell<Vec<&'static str>> =
        RefCell::new(Vec::with_capacity(64));

    /// Per-thread edge counts: `(parent, child) → count`.
    static LOCAL_EDGES: RefCell<FxHashMap<(&'static str, &'static str), u64>> =
        RefCell::new(FxHashMap::default());
}

/// Pushes `name` onto the call stack. Returns the parent function name
/// (if any) so the caller can record the edge.
#[inline]
pub fn push(name: &'static str) -> Option<&'static str> {
    CALL_STACK.with(|stack| {
        let mut s = stack.borrow_mut();
        let parent = s.last().copied();
        s.push(name);
        parent
    })
}

/// Pops the top of the call stack.
#[inline]
pub fn pop() {
    CALL_STACK.with(|stack| {
        stack.borrow_mut().pop();
    });
}

/// Records a `(parent → child)` edge in the thread-local edge map.
#[inline]
pub fn record_edge(parent: &'static str, child: &'static str) {
    LOCAL_EDGES.with(|edges| {
        *edges.borrow_mut().entry((parent, child)).or_insert(0) += 1;
    });
}

/// Returns `true` if the call stack is non-empty.
#[inline]
pub fn is_in_call() -> bool {
    CALL_STACK.with(|s| !s.borrow().is_empty())
}

/// Current call depth (0 = not inside any instrumented function).
#[inline]
pub fn depth() -> usize {
    CALL_STACK.with(|s| s.borrow().len())
}

/// Drains all accumulated edges from the thread-local map, returning
/// them as a `Vec`. Called during periodic flush.
#[must_use]
pub fn drain_edges() -> Vec<((&'static str, &'static str), u64)> {
    LOCAL_EDGES.with(|edges| {
        let mut e = edges.borrow_mut();
        let drained: Vec<_> = e.drain().collect();
        drained
    })
}

/// Resets the thread-local call stack and edges (used by tests).
pub fn reset_thread() {
    CALL_STACK.with(|s| s.borrow_mut().clear());
    LOCAL_EDGES.with(|e| e.borrow_mut().clear());
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn push_pop_lifo() {
        reset_thread();
        assert_eq!(depth(), 0);
        assert!(push("outer").is_none());
        assert_eq!(depth(), 1);
        assert_eq!(push("inner"), Some("outer"));
        assert_eq!(depth(), 2);
        pop();
        assert_eq!(depth(), 1);
        pop();
        assert_eq!(depth(), 0);
    }

    #[test]
    fn push_returns_parent() {
        reset_thread();
        push("a");
        assert_eq!(push("b"), Some("a"));
        assert_eq!(push("c"), Some("b"));
        pop();
        // After pop, stack is ["a", "b"]. Parent of "d" is "b".
        assert_eq!(push("d"), Some("b"));
        pop();
        pop();
    }

    #[test]
    fn record_edge_accumulates() {
        reset_thread();
        record_edge("a", "b");
        record_edge("a", "b");
        record_edge("a", "b");
        record_edge("b", "c");

        let edges = drain_edges();
        let ab = edges.iter().find(|(e, _)| *e == ("a", "b"));
        assert_eq!(ab.unwrap().1, 3);

        let bc = edges.iter().find(|(e, _)| *e == ("b", "c"));
        assert_eq!(bc.unwrap().1, 1);
    }

    #[test]
    fn drain_edges_clears_map() {
        reset_thread();
        record_edge("x", "y");
        let first = drain_edges();
        assert!(!first.is_empty());
        let second = drain_edges();
        assert!(second.is_empty());
    }

    #[test]
    fn depth_tracks_nesting() {
        reset_thread();
        assert_eq!(depth(), 0);
        push("f1");
        assert_eq!(depth(), 1);
        push("f2");
        assert_eq!(depth(), 2);
        push("f3");
        assert_eq!(depth(), 3);
        pop();
        pop();
        pop();
        assert_eq!(depth(), 0);
    }

    #[test]
    fn is_in_call_flag() {
        reset_thread();
        assert!(!is_in_call());
        push("test");
        assert!(is_in_call());
        pop();
        assert!(!is_in_call());
    }

    #[test]
    fn deep_nesting_does_not_overflow() {
        reset_thread();
        // Push many entries — stack grows as needed.
        for _ in 0..500 {
            push("deep");
        }
        assert_eq!(depth(), 500);
        // Pop all.
        for _ in 0..500 {
            pop();
        }
        assert_eq!(depth(), 0);
    }
}

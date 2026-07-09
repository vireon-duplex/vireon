//! [`StreamHandle`] — handle for tracking active RPC streams.

// ── StreamHandle ──────────────────────────────────────────────────

/// Handle that uniquely identifies an active RPC stream.
///
/// Handles are allocated when a stream is opened and remain valid
/// until the stream is closed or cancelled. Zero (`NIL`) is reserved
/// and never assigned to a real stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamHandle(u64);

impl StreamHandle {
    /// The reserved nil value (0).
    pub const NIL: Self = Self(0);

    /// Construct a new stream handle.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Construct the nil stream handle (0).
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this is the nil stream handle.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        self.0 == 0
    }

    /// Get the raw underlying value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl Default for StreamHandle {
    fn default() -> Self {
        Self::NIL
    }
}

impl std::fmt::Display for StreamHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stream#{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_handle_new() {
        let handle = StreamHandle::new(1);
        assert_eq!(handle.raw(), 1);
        assert!(!handle.is_nil());
    }

    #[test]
    fn stream_handle_nil() {
        let handle = StreamHandle::nil();
        assert_eq!(handle.raw(), 0);
        assert!(handle.is_nil());
    }

    #[test]
    fn stream_handle_nil_constant() {
        assert_eq!(StreamHandle::NIL, StreamHandle::nil());
        assert!(StreamHandle::NIL.is_nil());
    }

    #[test]
    fn stream_handle_default_is_nil() {
        let handle = StreamHandle::default();
        assert_eq!(handle, StreamHandle::NIL);
        assert!(handle.is_nil());
    }

    #[test]
    fn stream_handle_equality() {
        let h1 = StreamHandle::new(42);
        let h2 = StreamHandle::new(42);
        let h3 = StreamHandle::new(43);
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
    }

    #[test]
    fn stream_handle_ordering() {
        let h1 = StreamHandle::new(10);
        let h2 = StreamHandle::new(20);
        assert!(h1 < h2);
        assert!(h2 > h1);
        assert_eq!(h1.min(h2), h1);
        assert_eq!(h1.max(h2), h2);
    }

    #[test]
    fn stream_handle_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(StreamHandle::new(1));
        set.insert(StreamHandle::new(2));
        set.insert(StreamHandle::new(1)); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn stream_handle_clone() {
        let h1 = StreamHandle::new(100);
        let h2 = h1;
        assert_eq!(h1, h2);
        assert_eq!(h1.raw(), 100);
    }

    #[test]
    fn stream_handle_raw_roundtrip() {
        let handle = StreamHandle::new(12345);
        assert_eq!(StreamHandle::new(handle.raw()), handle);
    }

    #[test]
    fn stream_handle_debug_format() {
        let handle = StreamHandle::new(42);
        let s = format!("{handle:?}");
        assert!(s.contains("42"));
    }

    #[test]
    fn stream_handle_nil_is_less_than_any_valid_handle() {
        let nil = StreamHandle::NIL;
        let valid = StreamHandle::new(1);
        assert!(nil < valid);
        assert!(valid > nil);
    }

    #[test]
    fn stream_handle_partial_ord_total() {
        // Ensure PartialOrd behaves correctly for all comparisons.
        let handles = [
            StreamHandle::NIL,
            StreamHandle::new(1),
            StreamHandle::new(5),
            StreamHandle::new(10),
        ];
        for i in 0..handles.len() {
            for j in 0..handles.len() {
                let ordering = handles[i].cmp(&handles[j]);
                let expected = i.cmp(&j);
                assert_eq!(ordering, expected);
            }
        }
    }

    #[test]
    fn stream_handle_const_evaluation() {
        // Ensure const functions can be evaluated at compile time.
        const HANDLE: StreamHandle = StreamHandle::new(42);
        const NIL: StreamHandle = StreamHandle::nil();
        assert!(!HANDLE.is_nil());
        assert!(NIL.is_nil());
        assert_eq!(HANDLE.raw(), 42);
        assert_eq!(NIL.raw(), 0);
    }

    #[test]
    fn stream_handle_can_be_used_in_vec() {
        let mut vec = vec![
            StreamHandle::new(1),
            StreamHandle::new(2),
            StreamHandle::NIL,
        ];
        vec.sort();
        assert_eq!(vec[0], StreamHandle::NIL);
        assert_eq!(vec[1], StreamHandle::new(1));
        assert_eq!(vec[2], StreamHandle::new(2));
    }

    #[test]
    fn stream_handle_can_be_used_in_btree_map() {
        use std::collections::BTreeMap;
        let mut map = BTreeMap::new();
        map.insert(StreamHandle::new(1), "first");
        map.insert(StreamHandle::new(2), "second");
        assert_eq!(map.get(&StreamHandle::new(1)), Some(&"first"));
        assert_eq!(map.get(&StreamHandle::new(2)), Some(&"second"));
    }

    #[test]
    fn stream_handle_copy_trait() {
        let h1 = StreamHandle::new(999);
        let h2 = h1; // Copy, not Move
        assert_eq!(h1, h2); // h1 is still valid
        assert_eq!(h1.raw(), 999);
        assert_eq!(h2.raw(), 999);
    }
}

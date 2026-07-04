//! Request classification (Layer 3 fast path).
//!
//! Thin, inlinable entry point the pool calls before touching any cache. It
//! returns a [`Classification`] telling the caller whether to serve the
//! request from a fixed size class or route it to the huge allocator.

use crate::allocator::size_class::{ClassId, SizeClass, class_for, classify};

/// The outcome of classifying a request.
#[derive(Clone, Copy, Debug)]
pub enum Classification {
    /// Serve from a fixed size class.
    Classed {
        /// The chosen class identifier.
        id: ClassId,
        /// The static description of `id`.
        class: SizeClass,
    },
    /// Route to the huge allocator.
    Huge {
        /// The original (unaligned) requested size.
        size: usize,
    },
}

impl Classification {
    /// The [`ClassId`] for this request, or [`ClassId::HUGE`].
    #[inline]
    pub fn id(&self) -> ClassId {
        match self {
            Classification::Classed { id, .. } => *id,
            Classification::Huge { .. } => ClassId::HUGE,
        }
    }

    /// True when this request must bypass the slab hierarchy.
    #[inline]
    pub fn is_huge(&self) -> bool {
        matches!(self, Classification::Huge { .. })
    }
}

/// Classifies `size` into either a fixed class or the huge path.
#[inline]
pub fn classify_request(size: usize) -> Classification {
    let id = classify(size);
    match class_for(id) {
        Some(class) => Classification::Classed { id, class },
        None => Classification::Huge { size },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::size_class::MAX_CLASS_SIZE;

    #[test]
    fn small_request_is_classed() {
        match classify_request(100) {
            Classification::Classed { id, class } => {
                assert_eq!(id, ClassId(1)); // 128-byte class
                assert_eq!(class.size, 128);
                assert!(class.size >= 100);
            }
            Classification::Huge { .. } => panic!("100 bytes must not be huge"),
        }
    }

    #[test]
    fn huge_request_is_routed() {
        match classify_request(MAX_CLASS_SIZE + 1) {
            Classification::Classed { .. } => {
                panic!("above-class request must be huge")
            }
            Classification::Huge { size } => {
                assert_eq!(size, MAX_CLASS_SIZE + 1);
            }
        }
    }

    #[test]
    fn id_accessor_matches_classify() {
        for &size in &[
            0usize,
            1,
            64,
            1024,
            65536,
            MAX_CLASS_SIZE,
            MAX_CLASS_SIZE + 1,
        ] {
            let class_id = classify(size);
            assert_eq!(classify_request(size).id(), class_id);
        }
    }

    #[test]
    fn is_huge_only_for_above_class() {
        assert!(!classify_request(1024).is_huge());
        assert!(!classify_request(MAX_CLASS_SIZE).is_huge());
        assert!(classify_request(MAX_CLASS_SIZE + 1).is_huge());
    }
}

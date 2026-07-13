//! Size classification (Layer 3).
//!
//! Every request is mapped to a [`ClassId`] whose [`SizeClass`] (capacity,
//! alignment, batch) governs allocation and recycling. Classes follow a
//! power-of-two progression from 64 B up to 1 MiB; anything larger is routed
//! to the huge allocator via [`ClassId::HUGE`].

/// Opaque identifier for one entry in [`CLASS_TABLE`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ClassId(pub u8);

impl ClassId {
    /// Sentinel marking allocations that bypass the slab hierarchy.
    pub const HUGE: ClassId = ClassId(u8::MAX);
}

/// Static description of one size class.
#[derive(Clone, Copy, Debug)]
pub struct SizeClass {
    /// Slot capacity in bytes.
    pub size: usize,
    /// Slot alignment in bytes (always a power of two).
    pub align: usize,
    /// Recommended refill/drain batch size.
    pub batch: usize,
}

/// The fixed size-class table, smallest first.
///
/// Index into this array is the [`ClassId`] value (excluding the huge sentinel).
pub const CLASS_TABLE: [SizeClass; 15] = [
    SizeClass {
        size: 64,
        align: 64,
        batch: 32,
    },
    SizeClass {
        size: 128,
        align: 64,
        batch: 32,
    },
    SizeClass {
        size: 256,
        align: 64,
        batch: 16,
    },
    SizeClass {
        size: 512,
        align: 64,
        batch: 16,
    },
    SizeClass {
        size: 1024,
        align: 64,
        batch: 16,
    },
    SizeClass {
        size: 2048,
        align: 64,
        batch: 8,
    },
    SizeClass {
        size: 4096,
        align: 4096,
        batch: 8,
    },
    SizeClass {
        size: 8192,
        align: 4096,
        batch: 4,
    },
    SizeClass {
        size: 16384,
        align: 4096,
        batch: 4,
    },
    SizeClass {
        size: 32768,
        align: 4096,
        batch: 2,
    },
    SizeClass {
        size: 65536,
        align: 4096,
        batch: 2,
    },
    SizeClass {
        size: 131072,
        align: 4096,
        batch: 1,
    },
    SizeClass {
        size: 262144,
        align: 4096,
        batch: 1,
    },
    SizeClass {
        size: 524288,
        align: 4096,
        batch: 1,
    },
    SizeClass {
        size: 1_048_576,
        align: 4096,
        batch: 1,
    },
];

/// Number of size classes (excluding the huge sentinel).
pub const NUM_CLASSES: usize = CLASS_TABLE.len();

/// Largest class size; requests above this go through the huge allocator.
pub const MAX_CLASS_SIZE: usize = 1_048_576;

/// Maps a requested size to its [`ClassId`].
///
/// Returns [`ClassId::HUGE`] when `size` exceeds [`MAX_CLASS_SIZE`]. A zero
/// request maps to the smallest class (64 B).
///
/// Uses O(1) bit-manipulation (`ilog2`) instead of a linear scan, since all
/// class sizes are exact powers of two in the range 2⁶ … 2²⁰.
#[inline]
pub fn classify(size: usize) -> ClassId {
    if size > MAX_CLASS_SIZE {
        return ClassId::HUGE;
    }
    // Sizes up to 64 all map to the smallest class (index 0).
    if size <= 64 {
        return ClassId(0);
    }
    // For size > 64: size−1 ≥ 64, so ilog2 returns ≥ 6 (never panics).
    // Class sizes are 2⁶…2²⁰, so index = floor(log2(size−1)) − 5.
    ClassId(((size - 1).ilog2() - 5) as u8)
}

/// Returns the [`SizeClass`] for `id`, or `None` for [`ClassId::HUGE`].
#[inline]
pub fn class_for(id: ClassId) -> Option<SizeClass> {
    if id == ClassId::HUGE {
        return None;
    }
    let idx = usize::from(id.0);
    CLASS_TABLE.get(idx).copied()
}

/// Slot capacity for `id` (0 for [`ClassId::HUGE`]).
#[inline]
pub fn object_size(id: ClassId) -> usize {
    class_for(id).map_or(0, |c| c.size)
}

/// Slot alignment for `id` (0 for [`ClassId::HUGE`]).
#[inline]
pub fn alignment(id: ClassId) -> usize {
    class_for(id).map_or(0, |c| c.align)
}

/// Recommended batch size for `id` (1 for [`ClassId::HUGE`]).
#[inline]
pub fn batch_size(id: ClassId) -> usize {
    class_for(id).map_or(1, |c| c.batch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_is_sorted_ascending() {
        for w in CLASS_TABLE.windows(2) {
            assert!(w[0].size < w[1].size, "classes must be strictly increasing");
        }
    }

    #[test]
    fn alignments_are_power_of_two() {
        for c in CLASS_TABLE {
            assert!(c.align.is_power_of_two(), "align {} not pow2", c.align);
            assert!(c.align >= crate::memory::alignment::MIN_ALIGN);
        }
    }

    #[test]
    fn classify_round_trips_across_class_boundaries() {
        // Each class size maps to itself.
        for (idx, c) in CLASS_TABLE.iter().enumerate() {
            assert_eq!(classify(c.size), ClassId(idx as u8));
        }
        // Sizes between classes round up.
        assert_eq!(classify(1), ClassId(0));
        assert_eq!(classify(65), ClassId(1)); // 64 < 65 <= 128
        assert_eq!(classify(129), ClassId(2)); // 128 < 129 <= 256
        assert_eq!(classify(513), ClassId(4)); // 512 < 513 <= 1024
    }

    #[test]
    fn classify_zero_maps_to_smallest() {
        assert_eq!(classify(0), ClassId(0));
    }

    #[test]
    fn classify_above_max_is_huge() {
        assert_eq!(classify(MAX_CLASS_SIZE + 1), ClassId::HUGE);
        assert_eq!(classify(usize::MAX), ClassId::HUGE);
    }

    #[test]
    fn class_for_returns_none_for_huge() {
        assert!(class_for(ClassId::HUGE).is_none());
    }

    #[test]
    fn accessors_are_consistent() {
        for (idx, c) in CLASS_TABLE.iter().enumerate() {
            let id = ClassId(idx as u8);
            assert_eq!(object_size(id), c.size);
            assert_eq!(alignment(id), c.align);
            assert_eq!(batch_size(id), c.batch);
        }
        assert_eq!(object_size(ClassId::HUGE), 0);
        assert_eq!(alignment(ClassId::HUGE), 0);
        assert_eq!(batch_size(ClassId::HUGE), 1);
    }
}

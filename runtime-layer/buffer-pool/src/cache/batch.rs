//! Adaptive batch sizing (Layer 9).
//!
//! [`BatchManager`] turns a recent miss rate into a batch size: high miss
//! rates pull larger batches from the global cache (amortising lock
//! acquisition); low miss rates pull smaller batches (avoiding overfilling the
//! thread-local cache).

/// Suggests refill/drain batch sizes from observed miss rates.
pub struct BatchManager {
    default: usize,
    min: usize,
    max: usize,
}

impl BatchManager {
    /// Creates a manager bounded by `[min, max]` with `default` as the
    /// mid-range suggestion.
    pub fn new(min: usize, default: usize, max: usize) -> Self {
        Self { default, min, max }
    }

    /// Lower bound on the suggested batch.
    #[inline]
    pub fn min(&self) -> usize {
        self.min
    }

    /// Upper bound on the suggested batch.
    #[inline]
    pub fn max(&self) -> usize {
        self.max
    }

    /// Default (mid-range) batch size.
    #[inline]
    pub fn default_batch(&self) -> usize {
        self.default
    }

    /// Picks a batch size for the given miss rate in `[0.0, 1.0]`.
    ///
    /// Miss rates above 0.5 return `max`; below 0.2 return `min`; in between
    /// returns `default`. Out-of-range rates are clamped.
    pub fn adaptive_batch(&self, miss_rate: f64) -> usize {
        let clamped = miss_rate.clamp(0.0, 1.0);
        if clamped > 0.5 {
            self.max
        } else if clamped > 0.2 {
            self.default
        } else {
            self.min
        }
    }
}

impl Default for BatchManager {
    fn default() -> Self {
        Self::new(8, 16, 32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn low_miss_returns_min() {
        let bm = BatchManager::new(8, 16, 32);
        assert_eq!(bm.adaptive_batch(0.0), 8);
        assert_eq!(bm.adaptive_batch(0.2), 8);
    }

    #[test]
    fn mid_miss_returns_default() {
        let bm = BatchManager::new(8, 16, 32);
        assert_eq!(bm.adaptive_batch(0.21), 16);
        assert_eq!(bm.adaptive_batch(0.5), 16);
    }

    #[test]
    fn high_miss_returns_max() {
        let bm = BatchManager::new(8, 16, 32);
        assert_eq!(bm.adaptive_batch(0.51), 32);
        assert_eq!(bm.adaptive_batch(1.0), 32);
    }

    #[test]
    fn out_of_range_clamps() {
        let bm = BatchManager::new(8, 16, 32);
        assert_eq!(bm.adaptive_batch(-1.0), 8);
        assert_eq!(bm.adaptive_batch(2.0), 32);
    }
}

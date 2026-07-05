//! Fixed-size CPU mask (1024 bits).
//!
//! [`CpuSet`] is bounded by [`MAX_CPUS`](crate::config::MAX_CPUS) so it
//! is stack-/inline-allocatable and never touches the heap allocator
//! on the hot path. Used by IRQ affinity, XPS, and per-queue stats.

use crate::config::MAX_CPUS;
use crate::topology::parse_cpu_list;

/// 1024-bit CPU mask, fixed-size.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CpuSet {
    bits: Box<[u64; MAX_CPUS / 64]>,
}

impl CpuSet {
    /// Empty set.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            bits: Box::new([0; MAX_CPUS / 64]),
        }
    }

    /// Returns `true` if the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bits.iter().all(|&w| w == 0)
    }

    /// Set the bit for `cpu`.
    pub fn set(&mut self, cpu: u32) {
        if (cpu as usize) < MAX_CPUS {
            self.bits[(cpu as usize) / 64] |= 1u64 << (cpu % 64);
        }
    }

    /// Clear the bit for `cpu`.
    pub fn clear(&mut self, cpu: u32) {
        if (cpu as usize) < MAX_CPUS {
            self.bits[(cpu as usize) / 64] &= !(1u64 << (cpu % 64));
        }
    }

    /// `true` if `cpu` is set.
    #[must_use]
    pub fn contains(&self, cpu: u32) -> bool {
        if (cpu as usize) >= MAX_CPUS {
            return false;
        }
        (self.bits[(cpu as usize) / 64] & (1u64 << (cpu % 64))) != 0
    }

    /// Number of CPUs in the set.
    #[must_use]
    pub fn count(&self) -> u32 {
        self.bits.iter().map(|w| w.count_ones()).sum()
    }

    /// Lowest cpu_id in the set, or `None` if empty.
    #[must_use]
    pub fn first(&self) -> Option<u32> {
        for (i, &w) in self.bits.iter().enumerate() {
            if w != 0 {
                let bit = w.trailing_zeros();
                return Some(i as u32 * 64 + bit);
            }
        }
        None
    }

    /// Iterate over all set cpu_ids in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.bits.iter().enumerate().flat_map(|(i, &w)| {
            let base = i as u32 * 64;
            (0..64).filter_map(move |b| {
                if (w & (1u64 << b)) != 0 {
                    Some(base + b)
                } else {
                    None
                }
            })
        })
    }

    /// Construct from a Linux cpu list string (`"0,2-5,10"`).
    pub fn from_list_str(s: &str) -> Self {
        let mut set = Self::empty();
        for cpu in parse_cpu_list(s) {
            set.set(cpu);
        }
        set
    }

    /// Render as a hex string suitable for `/proc/irq/<n>/smp_affinity`.
    /// Format: 32-bit comma-separated words, little-endian (lowest cpu
    /// first), no leading zeros, e.g. `"0,0,0,0000ffff"`.
    #[must_use]
    pub fn to_smp_affinity_hex(&self) -> String {
        // `/proc/irq/<n>/smp_affinity` accepts a bitmap laid out as
        // 32-bit little-endian words separated by commas.
        // For 1024 CPUs we need 32 32-bit words.
        let mut words: Vec<u32> = Vec::with_capacity(MAX_CPUS / 32);
        for chunk in self.bits.chunks_exact(2) {
            // chunk[0] is low 64 bits, chunk[1] is high 64 bits
            let low = chunk[0] as u32;
            let high = (chunk[0] >> 32) as u32;
            let higher_low = chunk[1] as u32;
            let higher_high = (chunk[1] >> 32) as u32;
            words.push(low);
            words.push(high);
            words.push(higher_low);
            words.push(higher_high);
        }
        let mut s = String::new();
        for (i, &w) in words.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            s.push_str(&format!("{w:08x}"));
        }
        s
    }
}

impl std::ops::BitOrAssign<&CpuSet> for CpuSet {
    fn bitor_assign(&mut self, rhs: &CpuSet) {
        for (a, &b) in self.bits.iter_mut().zip(rhs.bits.iter()) {
            *a |= b;
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn set_and_contains() {
        let mut s = CpuSet::empty();
        assert!(!s.contains(0));
        s.set(0);
        s.set(63);
        s.set(64);
        s.set(1023);
        assert!(s.contains(0));
        assert!(s.contains(63));
        assert!(s.contains(64));
        assert!(s.contains(1023));
        assert!(!s.contains(1));
        assert!(!s.contains(u32::MAX));
    }

    #[test]
    fn count_matches_set_bits() {
        let mut s = CpuSet::empty();
        assert_eq!(s.count(), 0);
        for c in [0, 1, 5, 100, 1000] {
            s.set(c);
        }
        assert_eq!(s.count(), 5);
    }

    #[test]
    fn first_returns_lowest() {
        let mut s = CpuSet::empty();
        assert_eq!(s.first(), None);
        s.set(100);
        s.set(2);
        assert_eq!(s.first(), Some(2));
    }

    #[test]
    fn iter_yields_ascending() {
        let mut s = CpuSet::empty();
        for c in [5, 1, 3] {
            s.set(c);
        }
        let got: Vec<u32> = s.iter().collect();
        assert_eq!(got, vec![1, 3, 5]);
    }

    #[test]
    fn from_list_str_handles_ranges() {
        let s = CpuSet::from_list_str("0,2-3");
        assert!(s.contains(0));
        assert!(s.contains(2));
        assert!(s.contains(3));
        assert!(!s.contains(1));
    }

    #[test]
    fn out_of_range_ignored() {
        let mut s = CpuSet::empty();
        s.set(u32::MAX); // ignored
        assert!(s.is_empty());
    }

    #[test]
    fn smp_affinity_hex_format() {
        let mut s = CpuSet::empty();
        s.set(0);
        s.set(1);
        let hex = s.to_smp_affinity_hex();
        // First word should be 0x00000003 (bits 0+1 set).
        assert!(hex.starts_with("00000003,"));
        // 32 words total.
        assert_eq!(hex.split(',').count(), MAX_CPUS / 32);
    }

    #[test]
    fn bitor_assign_unions() {
        let mut a = CpuSet::from_list_str("0,1");
        let b = CpuSet::from_list_str("1,2");
        a |= &b;
        assert!(a.contains(0));
        assert!(a.contains(1));
        assert!(a.contains(2));
    }
}

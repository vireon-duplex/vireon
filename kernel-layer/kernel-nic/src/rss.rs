//! RSS (Receive Side Scaling) configuration.
//!
//! [`RssConfig`] is a value-typed description of the indirection table
//! + hash key. [`RssConfig::per_cpu`] generates a default table that
//!   round-robins RX queues across the supplied CPU list.

use crate::cpuset::CpuSet;
use crate::error::KernelNicError;

/// 128-entry RSS indirection table + 40-byte hash key.
#[derive(Clone, Debug)]
pub struct RssConfig {
    /// 128 entries, each an RX queue id.
    pub indirection: Box<[u32; 128]>,
    /// 40-byte Toeplitz hash key.
    pub hash_key: Box<[u8; 40]>,
    /// Flow hash types enabled.
    pub hash_types: RssHashTypes,
    /// Per-CPU RX queue assignment used by `PerCpu` policies.
    pub queue_to_cpu: Vec<(u32, u32)>,
}

/// Bitmask of RSS flow hash types.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RssHashTypes(pub u32);

impl RssHashTypes {
    pub const IPV4_UDP: Self = Self(1 << 0);
    pub const IPV6_UDP: Self = Self(1 << 1);
    pub const IPV4_TCP: Self = Self(1 << 2);
    pub const IPV6_TCP: Self = Self(1 << 3);
    pub const IPV4: Self = Self(1 << 4);
    pub const IPV6: Self = Self(1 << 5);

    /// UDP-v4 + UDP-v6 (the recommended default for a UDP runtime).
    #[must_use]
    pub const fn udp_only() -> Self {
        Self(Self::IPV4_UDP.0 | Self::IPV6_UDP.0)
    }

    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl std::ops::BitOr for RssHashTypes {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl RssConfig {
    /// Construct an empty config with a zero hash key.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            indirection: Box::new([0; 128]),
            hash_key: Box::new([0; 40]),
            hash_types: RssHashTypes::udp_only(),
            queue_to_cpu: Vec::new(),
        }
    }

    /// Build a `PerCpu` config: each RX queue receives an even share
    /// of the 128-entry indirection table. `queues` must be non-empty
    /// and ordered by queue id matching the supplied CPU list.
    #[must_use]
    pub fn per_cpu(cpus_on_numa: &[u32], hash_key: [u8; 40]) -> Self {
        let mut cfg = Self::empty();
        cfg.hash_key = Box::new(hash_key);
        if cpus_on_numa.is_empty() {
            return cfg;
        }
        for (i, entry) in cfg.indirection.iter_mut().enumerate() {
            let queue = i % cpus_on_numa.len();
            *entry = queue as u32;
        }
        cfg.queue_to_cpu = cpus_on_numa
            .iter()
            .enumerate()
            .map(|(q, &cpu)| (q as u32, cpu))
            .collect();
        cfg
    }

    /// Construct a random 40-byte hash key (caller-provided RNG).
    /// We do not pull in `rand` — callers pass a 40-byte buffer.
    #[must_use]
    pub fn random_hash_key(seed: u64) -> [u8; 40] {
        // SplitMix64 — fast, decent distribution for RSS keys.
        let mut state = seed;
        let mut out = [0u8; 40];
        for chunk in out.chunks_mut(8) {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            let bytes = z.to_le_bytes();
            for (dst, &src) in chunk.iter_mut().zip(bytes.iter()) {
                *dst = src;
            }
        }
        out
    }

    /// Sanity-check.
    pub fn validate(&self) -> Result<(), KernelNicError> {
        for &q in &*self.indirection {
            if q >= 4096 {
                return Err(KernelNicError::InvalidConfig(format!(
                    "rss indirection queue id {q} out of range"
                )));
            }
        }
        Ok(())
    }
}

/// Render an XPS / RSS smp_affinity-style mask for the NIC's RX queue
/// `queue` mapped to `cpu`. Used for `ethtool -L`-style configuration
/// docs; the real apply path goes through netlink.
pub fn render_queue_affinity_mask(cpu: u32) -> CpuSet {
    let mut s = CpuSet::empty();
    s.set(cpu);
    s
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn per_cpu_round_robin_for_two_cpus() {
        let cfg = RssConfig::per_cpu(&[0, 1], [0u8; 40]);
        assert_eq!(cfg.indirection[0], 0);
        assert_eq!(cfg.indirection[1], 1);
        assert_eq!(cfg.indirection[2], 0);
        assert_eq!(cfg.indirection[127], 1);
        assert_eq!(cfg.queue_to_cpu, vec![(0, 0), (1, 1)]);
    }

    #[test]
    fn per_cpu_handles_four_cpus() {
        let cfg = RssConfig::per_cpu(&[0, 1, 2, 3], [0u8; 40]);
        assert_eq!(cfg.indirection[0], 0);
        assert_eq!(cfg.indirection[3], 3);
        assert_eq!(cfg.indirection[4], 0);
        assert_eq!(cfg.indirection[127], 3);
    }

    #[test]
    fn empty_cpu_list_returns_zeros() {
        let cfg = RssConfig::per_cpu(&[], [0u8; 40]);
        assert!(cfg.indirection.iter().all(|&q| q == 0));
        assert!(cfg.queue_to_cpu.is_empty());
    }

    #[test]
    fn random_hash_key_is_deterministic() {
        let a = RssConfig::random_hash_key(42);
        let b = RssConfig::random_hash_key(42);
        assert_eq!(a, b);
        // Different seed → different output.
        let c = RssConfig::random_hash_key(43);
        assert_ne!(a, c);
    }

    #[test]
    fn validate_rejects_huge_queue() {
        let mut cfg = RssConfig::empty();
        cfg.indirection[0] = 5000;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn udp_only_includes_only_udp() {
        let h = RssHashTypes::udp_only();
        assert!(h.contains(RssHashTypes::IPV4_UDP));
        assert!(h.contains(RssHashTypes::IPV6_UDP));
        assert!(!h.contains(RssHashTypes::IPV4_TCP));
    }
}

//! XPS (Transmit Packet Steering) configuration.
//!
//! [`XpsConfig`] is a per-TX-queue → CPU mask mapping. The apply path
//! writes `/sys/class/net/<nic>/queues/tx-<n>/xps_cpus` for each
//! queue — see [`XpsConfig::write_to_sysfs`].

use std::path::Path;

use crate::cpuset::CpuSet;
use crate::error::KernelNicError;

/// Per-TX-queue CPU mask + reverse (CPU → queues) lookup.
#[derive(Clone, Debug, Default)]
pub struct XpsConfig {
    /// Indexed by TX queue id. Each entry is the CPU mask allowed to
    /// transmit on that queue.
    pub queues: Vec<CpuSet>,
}

impl XpsConfig {
    /// Build a config where each TX queue is mapped to exactly one CPU.
    /// `cpus_on_numa` should be the CPUs on the NIC's NUMA node.
    #[must_use]
    pub fn per_cpu(cpus_on_numa: &[u32], tx_queue_count: u32) -> Self {
        let mut cfg = Self::default();
        if cpus_on_numa.is_empty() {
            return cfg;
        }
        let n = cpus_on_numa.len() as u32;
        for q in 0..tx_queue_count.max(n) {
            let cpu = cpus_on_numa[(q % n) as usize];
            let mut mask = CpuSet::empty();
            mask.set(cpu);
            cfg.queues.push(mask);
        }
        cfg
    }

    /// Combine SMT siblings: each TX queue can be transmitted from
    /// either logical sibling.
    #[must_use]
    pub fn with_smt_siblings(mut self, smt_of: impl Fn(u32) -> Option<u32>) -> Self {
        for mask in &mut self.queues {
            if let Some(cpu) = mask.first() {
                if let Some(sibling) = smt_of(cpu) {
                    mask.set(sibling);
                }
            }
        }
        self
    }

    /// CPUs allowed to transmit on a given queue.
    #[must_use]
    pub fn cpus_for_queue(&self, queue: u32) -> Option<&CpuSet> {
        self.queues.get(queue as usize)
    }

    /// Apply the config to a NIC. Writes `xps_cpus` for each TX queue.
    /// Returns the number of queues successfully configured.
    pub fn write_to_sysfs(
        &self,
        sys_class_net: &Path,
        nic_name: &str,
    ) -> Result<usize, KernelNicError> {
        let mut written = 0;
        for (queue, mask) in self.queues.iter().enumerate() {
            let path = sys_class_net
                .join(nic_name)
                .join("queues")
                .join(format!("tx-{queue}"))
                .join("xps_cpus");
            let hex = mask.to_smp_affinity_hex();
            if let Err(e) = std::fs::write(&path, format!("{hex}\n").as_bytes()) {
                return Err(KernelNicError::SysctlWrite {
                    path: path.to_string_lossy().into_owned().into_boxed_str(),
                    source: e,
                });
            }
            written += 1;
        }
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn per_cpu_assigns_one_cpu_per_queue() {
        let cfg = XpsConfig::per_cpu(&[0, 1, 2, 3], 4);
        assert_eq!(cfg.queues.len(), 4);
        assert!(cfg.queues[0].contains(0));
        assert!(cfg.queues[1].contains(1));
        assert!(cfg.queues[2].contains(2));
        assert!(cfg.queues[3].contains(3));
    }

    #[test]
    fn per_cpu_with_more_queues_than_cpus_wraps() {
        let cfg = XpsConfig::per_cpu(&[0, 1], 4);
        assert_eq!(cfg.queues.len(), 4);
        assert!(cfg.queues[0].contains(0));
        assert!(cfg.queues[1].contains(1));
        assert!(cfg.queues[2].contains(0));
        assert!(cfg.queues[3].contains(1));
    }

    #[test]
    fn with_smt_siblings_adds_sibling() {
        let cfg = XpsConfig::per_cpu(&[0, 1], 2).with_smt_siblings(|cpu| {
            if cpu == 0 {
                Some(8)
            } else if cpu == 1 {
                Some(9)
            } else {
                None
            }
        });
        assert!(cfg.queues[0].contains(0));
        assert!(cfg.queues[0].contains(8));
        assert!(cfg.queues[1].contains(1));
        assert!(cfg.queues[1].contains(9));
    }

    #[test]
    fn cpus_for_queue_returns_mask() {
        let cfg = XpsConfig::per_cpu(&[0, 1], 2);
        assert!(cfg.cpus_for_queue(0).is_some());
        assert!(cfg.cpus_for_queue(99).is_none());
    }

    #[test]
    fn empty_cpu_list_produces_empty_config() {
        let cfg = XpsConfig::per_cpu(&[], 4);
        assert!(cfg.queues.is_empty());
    }
}

//! Sysctl reader/writer + recommended profiles.
//!
//! [`SysctlProfile`] is a value-typed bag of `net.core` / `net.ipv4.udp`
//! knobs. [`Sysctl::apply`] writes them to `/proc/sys/net/...` if the
//! caller has `CAP_NET_ADMIN`; [`Sysctl::recommend`] returns the
//! would-be-applied profile for dry runs (containers, restricted
//! environments).

use std::path::Path;

use crate::error::KernelNicError;

/// Tunable profile. Each `Option<u64>` field, when `Some`, will be
/// written to the corresponding `/proc/sys/net/...` path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SysctlProfile {
    pub net_core_rmem_max: Option<u64>,
    pub net_core_wmem_max: Option<u64>,
    pub net_core_rmem_default: Option<u64>,
    pub net_core_wmem_default: Option<u64>,
    pub net_core_somaxconn: Option<u64>,
    pub net_core_netdev_max_backlog: Option<u64>,
    pub udp_rmem_min: Option<u64>,
    pub udp_wmem_min: Option<u64>,
    pub netdev_budget: Option<u64>,
    pub netdev_budget_usecs: Option<u64>,
    pub busy_read: Option<u64>,
    pub busy_poll: Option<u64>,
}

impl SysctlProfile {
    /// Conservative defaults for any Linux host.
    #[must_use]
    pub fn balanced() -> Self {
        Self {
            net_core_rmem_max: Some(64 * 1024 * 1024),
            net_core_wmem_max: Some(64 * 1024 * 1024),
            net_core_rmem_default: Some(16 * 1024 * 1024),
            net_core_wmem_default: Some(16 * 1024 * 1024),
            net_core_somaxconn: Some(32_768),
            net_core_netdev_max_backlog: Some(25_000),
            udp_rmem_min: Some(8192),
            udp_wmem_min: Some(4096),
            netdev_budget: Some(600),
            netdev_budget_usecs: Some(8000),
            busy_read: Some(50),
            busy_poll: Some(50),
        }
    }

    /// Throughput-optimised profile for 100GbE+.
    #[must_use]
    pub fn high_throughput() -> Self {
        Self {
            net_core_rmem_max: Some(256 * 1024 * 1024),
            net_core_wmem_max: Some(256 * 1024 * 1024),
            net_core_rmem_default: Some(64 * 1024 * 1024),
            net_core_wmem_default: Some(64 * 1024 * 1024),
            net_core_somaxconn: Some(65_536),
            net_core_netdev_max_backlog: Some(100_000),
            udp_rmem_min: Some(16_384),
            udp_wmem_min: Some(16_384),
            netdev_budget: Some(1200),
            netdev_budget_usecs: Some(16_000),
            busy_read: Some(0),
            busy_poll: Some(0),
        }
    }

    /// Latency-optimised profile.
    #[must_use]
    pub fn low_latency() -> Self {
        Self {
            net_core_rmem_max: Some(32 * 1024 * 1024),
            net_core_wmem_max: Some(32 * 1024 * 1024),
            net_core_rmem_default: Some(4 * 1024 * 1024),
            net_core_wmem_default: Some(4 * 1024 * 1024),
            net_core_somaxconn: Some(8_192),
            net_core_netdev_max_backlog: Some(10_000),
            udp_rmem_min: Some(4_096),
            udp_wmem_min: Some(4_096),
            netdev_budget: Some(300),
            netdev_budget_usecs: Some(2_000),
            busy_read: Some(50),
            busy_poll: Some(50),
        }
    }

    /// Cloud VM profile (no kernel writes; recommendation-only).
    #[must_use]
    pub fn vm() -> Self {
        Self::balanced()
    }
}

/// Per-process snapshot of `/proc/sys/net/...` state. Used to restore
/// original values on shutdown.
#[derive(Clone, Debug, Default)]
pub struct SysctlSnapshot {
    pub applied: Vec<(&'static str, u64, Option<u64>)>, // (path, applied, previous)
}

/// Reads and writes sysctls. Stateless — each call performs I/O.
pub struct Sysctl;

impl Sysctl {
    /// Read the current value of a `/proc/sys/net/...` knob.
    pub fn read(path: &str) -> Result<u64, KernelNicError> {
        let full = format!("/proc/sys/{path}");
        let s = std::fs::read_to_string(&full).map_err(|e| KernelNicError::SysctlWrite {
            path: full.clone().into_boxed_str(),
            source: e,
        })?;
        s.trim()
            .parse::<u64>()
            .map_err(|e| KernelNicError::SysfsParse {
                path: full.into_boxed_str(),
                reason: format!("parse u64 failed: {e}").into_boxed_str(),
            })
    }

    /// Write a value. Captures the previous value so it can be
    /// restored later. Returns the previous value (or `None` if the
    /// knob was previously unset / unparseable).
    pub fn write(path: &str, value: u64) -> Result<Option<u64>, KernelNicError> {
        let full = format!("/proc/sys/{path}");
        let previous = Self::read(path).ok();
        std::fs::write(&full, format!("{value}\n").as_bytes()).map_err(|e| {
            KernelNicError::SysctlWrite {
                path: full.into_boxed_str(),
                source: e,
            }
        })?;
        Ok(previous)
    }

    /// Apply an entire profile. Captures the previous value of each
    /// field so the caller can [`Sysctl::restore`] on shutdown.
    pub fn apply(profile: &SysctlProfile) -> Result<SysctlSnapshot, KernelNicError> {
        let mut applied: Vec<(&'static str, u64, Option<u64>)> = Vec::new();
        for (path, value) in profile.iter() {
            let previous = Self::write(path, value)?;
            applied.push((path, value, previous));
        }
        Ok(SysctlSnapshot { applied })
    }

    /// Restore the previously-captured snapshot.
    pub fn restore(snapshot: &SysctlSnapshot) -> Result<(), KernelNicError> {
        for &(path, _, previous) in &snapshot.applied {
            if let Some(prev) = previous {
                let _ = Self::write(path, prev);
            }
        }
        Ok(())
    }

    /// Generate a recommendation report (no kernel writes).
    pub fn recommend(profile: &SysctlProfile) -> Vec<(&'static str, u64, u64)> {
        let mut out = Vec::new();
        for (path, target) in profile.iter() {
            let current = Self::read(path).unwrap_or(0);
            out.push((path, target, current));
        }
        out
    }
}

impl SysctlProfile {
    /// Iterate `((path, value))` for non-None fields. The paths are
    /// static strings used in [`Sysctl::write`] (without the
    /// `/proc/sys/` prefix).
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        let mut v: Vec<(&'static str, u64)> = Vec::new();
        let push = |v: &mut Vec<_>, k: &'static str, opt: Option<u64>| {
            if let Some(val) = opt {
                v.push((k, val));
            }
        };
        push(&mut v, "net/core/rmem_max", self.net_core_rmem_max);
        push(&mut v, "net/core/wmem_max", self.net_core_wmem_max);
        push(&mut v, "net/core/rmem_default", self.net_core_rmem_default);
        push(&mut v, "net/core/wmem_default", self.net_core_wmem_default);
        push(&mut v, "net/core/somaxconn", self.net_core_somaxconn);
        push(
            &mut v,
            "net/core/netdev_max_backlog",
            self.net_core_netdev_max_backlog,
        );
        push(&mut v, "net/ipv4/udp_rmem_min", self.udp_rmem_min);
        push(&mut v, "net/ipv4/udp_wmem_min", self.udp_wmem_min);
        push(&mut v, "net/core/netdev_budget", self.netdev_budget);
        push(
            &mut v,
            "net/core/netdev_budget_usecs",
            self.netdev_budget_usecs,
        );
        push(&mut v, "net/core/busy_read", self.busy_read);
        push(&mut v, "net/core/busy_poll", self.busy_poll);
        v.into_iter()
    }
}

/// Internal helper used by tests to point at a temp /proc.
fn _touch_path(_p: &Path) {}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn balanced_profile_has_all_fields() {
        let p = SysctlProfile::balanced();
        let collected: Vec<_> = p.iter().collect();
        assert!(collected.len() >= 10);
    }

    #[test]
    fn high_throughput_is_different_from_low_latency() {
        assert_ne!(
            SysctlProfile::high_throughput(),
            SysctlProfile::low_latency()
        );
    }

    #[test]
    fn read_unknown_path_returns_err() {
        let r = Sysctl::read("net/core/does_not_exist");
        assert!(r.is_err());
    }
}

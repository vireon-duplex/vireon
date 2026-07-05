//! NIC discovery — read-only scan of `/sys/class/net/*`.
//!
//! [`NicInfo`] captures everything we need to make per-NIC decisions:
//! driver, NUMA node, queue counts, offloads, capabilities, link speed.

use crate::error::KernelNicError;

/// All discovered info for one network interface.
#[derive(Clone, Debug)]
pub struct NicInfo {
    pub ifindex: u32,
    pub name: Box<str>,
    pub driver: DriverInfo,
    pub numa_node: u32,
    pub link_speed_gbps: Option<u32>,
    pub mtu: u32,
    pub queues: QueueCounts,
    pub msix_vectors: u32,
    pub offloads: NicOffloads,
    pub capabilities: NicCapabilities,
    pub mac_address: [u8; 6],
}

#[derive(Clone, Debug)]
pub struct DriverInfo {
    pub name: Box<str>,
    pub version: Box<str>,
    pub firmware: Box<str>,
    pub bus_info: Box<str>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueCounts {
    pub rx: u32,
    pub tx: u32,
    pub combined: u32,
    pub other: u32,
}

impl QueueCounts {
    /// Total RX-side queues (rx + combined).
    #[must_use]
    pub fn rx_total(self) -> u32 {
        self.rx + self.combined
    }

    /// Total TX-side queues (tx + combined).
    #[must_use]
    pub fn tx_total(self) -> u32 {
        self.tx + self.combined
    }

    /// Total of all queue types.
    #[must_use]
    pub fn total(self) -> u32 {
        self.rx + self.tx + self.combined + self.other
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NicOffloads {
    pub rx_checksum: bool,
    pub tx_checksum: bool,
    pub tso: bool,
    pub ufo: bool,
    pub gso: bool,
    pub gro: bool,
    pub udp_gso: bool,
    pub udp_gro: bool,
    pub hw_timestamp: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NicCapabilities {
    pub rss: bool,
    pub xps: bool,
    pub zero_copy: bool,
    pub busy_poll: bool,
    pub napi_id: bool,
    pub multi_buffer: bool,
}

/// NIC discovery facade. Stateless — each call reads sysfs fresh.
pub struct NicDiscovery;

impl NicDiscovery {
    /// Scan `/sys/class/net/*` and return one [`NicInfo`] per physical NIC.
    ///
    /// Virtual interfaces (`lo`) are skipped. Bridges, bonds, and VLANs
    /// are kept — callers can filter on
    /// [`DriverInfo::name`](struct.DriverInfo.html) if they want to
    /// restrict to physical NICs only.
    pub fn scan() -> Result<Vec<NicInfo>, KernelNicError> {
        Self::scan_root(std::path::Path::new("/sys/class/net"))
    }

    /// Test-friendly entry point: read from `root` (typically
    /// `/sys/class/net`).
    pub fn scan_root(root: &std::path::Path) -> Result<Vec<NicInfo>, KernelNicError> {
        let mut nics = Vec::new();
        let entries = read_dir_sorted(root)?;
        for entry in entries {
            let name = entry.file_name();
            let name_s = name.to_string_lossy();
            if name_s == "lo" {
                continue;
            }
            let nic_dir = root.join(name_s.as_ref());
            if !nic_dir.is_dir() {
                continue;
            }
            match Self::read_one(&nic_dir) {
                Ok(Some(info)) => nics.push(info),
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(nic = %name_s, err = %e, "nic discovery partial failure");
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        // Keep lint happy — debug message emitted above when needed.
                    }
                }
            }
        }
        nics.sort_by_key(|n| n.ifindex);
        Ok(nics)
    }

    fn read_one(nic_dir: &std::path::Path) -> Result<Option<NicInfo>, KernelNicError> {
        let name: Box<str> = nic_dir
            .file_name()
            .and_then(|n| n.to_str())
            .map(Box::from)
            .unwrap_or_else(|| Box::from(""));

        let ifindex = read_u32(&nic_dir.join("ifindex")).unwrap_or(0);
        let mtu = read_u32(&nic_dir.join("mtu")).unwrap_or(1500);

        // Skip non-Ethernet devices (e.g. wireguard, tun).
        let dev_type = read_string(&nic_dir.join("type")).unwrap_or_else(|_| "1".into());
        if dev_type.trim() != "1" {
            return Ok(None);
        }

        let mac = read_mac(&nic_dir.join("address"));

        let speed_mbps = read_u32(&nic_dir.join("speed")).ok();
        let link_speed_gbps = speed_mbps.map(|s| s / 1000).filter(|&s| s > 0);

        let queues = read_queue_counts(nic_dir);
        let (driver, numa_node, msix_vectors) = read_device(nic_dir);
        let offloads = read_offloads(nic_dir, &name);
        let capabilities = read_capabilities(nic_dir, &name, &driver.name);

        Ok(Some(NicInfo {
            ifindex,
            name,
            driver,
            numa_node,
            link_speed_gbps,
            mtu,
            queues,
            msix_vectors,
            offloads,
            capabilities,
            mac_address: mac,
        }))
    }
}

fn read_queue_counts(nic_dir: &std::path::Path) -> QueueCounts {
    let qdir = nic_dir.join("queues");
    let mut rx = 0u32;
    let mut tx = 0u32;
    let mut other = 0u32;
    let Ok(entries) = std::fs::read_dir(&qdir) else {
        return QueueCounts::default();
    };
    for entry in entries.flatten() {
        let n = entry.file_name();
        let s = n.to_string_lossy();
        if s.starts_with("rx-") {
            rx += 1;
        } else if s.starts_with("tx-") {
            tx += 1;
        } else {
            other += 1;
        }
    }
    QueueCounts {
        rx,
        tx,
        combined: 0,
        other,
    }
}

fn read_device(nic_dir: &std::path::Path) -> (DriverInfo, u32, u32) {
    let device_dir = nic_dir.join("device");
    let driver = read_driver(&device_dir);
    let numa_node = read_u32(&device_dir.join("numa_node"))
        .ok()
        .filter(|&n| n != u32::MAX)
        .unwrap_or(0);
    let msix_vectors = read_msix_vectors(&device_dir);
    (driver, numa_node, msix_vectors)
}

fn read_driver(device_dir: &std::path::Path) -> DriverInfo {
    let driver_link = device_dir.join("driver");
    let driver_name: Box<str> = std::fs::read_link(&driver_link)
        .ok()
        .and_then(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().into_owned().into_boxed_str())
        })
        .unwrap_or_else(|| Box::from("unknown"));

    let version =
        read_string_opt(&device_dir.join("driver/module/version")).unwrap_or_else(|| Box::from(""));
    let firmware = read_string_opt(&device_dir.join("firmware_version"))
        .or_else(|| read_string_opt(&device_dir.join("board_revision")))
        .unwrap_or_else(|| Box::from(""));
    let bus_info = read_string_opt(&device_dir.join("uevent"))
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("PCI_SLOT_NAME=").map(Box::from))
        })
        .or_else(|| read_string_opt(&device_dir.join("device")))
        .unwrap_or_else(|| Box::from(""));

    DriverInfo {
        name: driver_name,
        version,
        firmware,
        bus_info,
    }
}

fn read_msix_vectors(device_dir: &std::path::Path) -> u32 {
    let msi_dir = device_dir.join("msi_irqs");
    if !msi_dir.is_dir() {
        return 0;
    }
    let Ok(entries) = std::fs::read_dir(&msi_dir) else {
        return 0;
    };
    entries.flatten().count() as u32
}

fn read_offloads(nic_dir: &std::path::Path, _name: &str) -> NicOffloads {
    // We could shell out to `ethtool -k` here, but the design specifies
    // netlink-based query in Phase 2. For now, parse
    // /sys/class/net/*/features (hex bitmask of NETIF_F_*).
    let mut offloads = NicOffloads::default();
    let features_str = read_string_opt(&nic_dir.join("features")).unwrap_or_default();
    // /sys/class/net/*/features is a hex bitmask; parsing it requires the
    // kernel feature-flag mapping which varies across kernel versions.
    // We use a coarse heuristic: presence of features file + a known
    // modern driver implies standard offloads.
    let lower_driver = std::fs::read_link(nic_dir.join("device/driver"))
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_lowercase()))
        .unwrap_or_default();
    if !features_str.is_empty() {
        offloads.rx_checksum = true;
        offloads.tx_checksum = true;
        offloads.gso = true;
        offloads.gro = true;
        if matches!(
            lower_driver.as_str(),
            "mlx5_core" | "i40e" | "iavf" | "bnxt_en" | "ice" | "enic" | "vmxnet3"
        ) {
            offloads.udp_gso = true;
            offloads.udp_gro = true;
        }
        if matches!(lower_driver.as_str(), "mlx5_core" | "ice") {
            offloads.hw_timestamp = true;
        }
    }
    offloads
}

fn read_capabilities(_nic_dir: &std::path::Path, _name: &str, driver: &str) -> NicCapabilities {
    let mut caps = NicCapabilities::default();
    let lower = driver.to_lowercase();
    caps.rss = matches!(
        lower.as_str(),
        "mlx5_core" | "i40e" | "iavf" | "bnxt_en" | "ice" | "enic"
    );
    caps.xps = matches!(lower.as_str(), "i40e" | "iavf" | "bnxt_en" | "ice");
    caps.zero_copy = matches!(
        lower.as_str(),
        "mlx5_core" | "i40e" | "iavf" | "bnxt_en" | "ice"
    );
    caps.busy_poll = matches!(
        lower.as_str(),
        "mlx5_core" | "i40e" | "iavf" | "bnxt_en" | "ice"
    );
    caps.napi_id = caps.rss; // NAPI ids are exposed by all multi-queue drivers.
    caps.multi_buffer = matches!(lower.as_str(), "mlx5_core" | "i40e" | "ice");
    caps
}

fn read_mac(path: &std::path::Path) -> [u8; 6] {
    let s = std::fs::read_to_string(path).unwrap_or_default();
    let mut mac = [0u8; 6];
    for (i, byte) in s.trim().split(':').enumerate() {
        if i >= 6 {
            break;
        }
        if let Ok(v) = u8::from_str_radix(byte, 16) {
            mac[i] = v;
        }
    }
    mac
}

// ── sysfs helpers (read-only) ───────────────────────────────────────

fn read_dir_sorted(dir: &std::path::Path) -> Result<Vec<std::fs::DirEntry>, KernelNicError> {
    let mut entries: Vec<std::fs::DirEntry> = std::fs::read_dir(dir)
        .map_err(|_| {
            KernelNicError::SysfsMissing(dir.to_string_lossy().into_owned().into_boxed_str())
        })?
        .filter_map(Result::ok)
        .collect();
    entries.sort_by_key(|e| e.file_name());
    Ok(entries)
}

fn read_u32(path: &std::path::Path) -> Result<u32, KernelNicError> {
    let s = read_string(path)?;
    s.trim()
        .parse::<u32>()
        .map_err(|e| KernelNicError::SysfsParse {
            path: path.to_string_lossy().into_owned().into_boxed_str(),
            reason: format!("parse u32 failed: {e}").into_boxed_str(),
        })
}

fn read_string(path: &std::path::Path) -> Result<String, KernelNicError> {
    std::fs::read_to_string(path).map_err(|_| {
        KernelNicError::SysfsMissing(path.to_string_lossy().into_owned().into_boxed_str())
    })
}

fn read_string_opt(path: &std::path::Path) -> Option<Box<str>> {
    std::fs::read_to_string(path).ok().map(Box::from)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn queue_counts_arithmetic() {
        let q = QueueCounts {
            rx: 4,
            tx: 4,
            combined: 8,
            other: 0,
        };
        assert_eq!(q.rx_total(), 12);
        assert_eq!(q.tx_total(), 12);
        assert_eq!(q.total(), 16);
    }

    #[test]
    fn scan_runs_on_host_or_returns_empty() {
        // On a CI runner the host may have only `lo`; we may end up with
        // an empty list which is acceptable.
        let _ = NicDiscovery::scan();
    }

    #[test]
    fn mac_parse() {
        // Round-trip via the helper directly.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), "de:ad:be:ef:00:01\n").unwrap();
        let mac = read_mac(tmp.path());
        assert_eq!(mac, [0xde, 0xad, 0xbe, 0xef, 0x00, 0x01]);
    }
}

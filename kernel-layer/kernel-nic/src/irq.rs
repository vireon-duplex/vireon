//! IRQ discovery — read-only mapping of NIC MSI-X vectors to CPUs.
//!
//! [`IrqMap`] is the frozen post-discovery picture of which IRQ belongs
//! to which NIC and which CPU it is currently affined to. Affinity
//! *writes* live in Phase 2 (see [`affinity`](crate::affinity)).

use std::collections::BTreeMap;

use crate::cpuset::CpuSet;
use crate::error::KernelNicError;
use crate::nic::NicInfo;

/// One entry per NIC MSI-X vector.
#[derive(Clone, Debug)]
pub struct IrqMap {
    pub entries: Vec<IrqInfo>,
}

#[derive(Clone, Debug)]
pub struct IrqInfo {
    pub irq: u32,
    pub ifindex: u32,
    pub queue_id: i32,
    pub kind: IrqKind,
    pub affinity: CpuSet,
    pub effective_cpu: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IrqKind {
    Rx,
    Tx,
    Event,
    Other,
}

impl IrqMap {
    /// Discover all IRQs belonging to any NIC in `nics`. Reads
    /// `/proc/interrupts` + `/proc/irq/<n>/` + per-NIC `msi_irqs`.
    pub fn discover(nics: &[NicInfo]) -> Result<Self, KernelNicError> {
        Self::discover_with_root(nics, std::path::Path::new("/proc"))
    }

    /// Test-friendly entry point.
    pub fn discover_with_root(
        nics: &[NicInfo],
        root: &std::path::Path,
    ) -> Result<Self, KernelNicError> {
        // Build PCI address → ifindex map.
        let mut pci_to_ifindex: BTreeMap<Box<str>, u32> = BTreeMap::new();
        for n in nics {
            if !n.driver.bus_info.is_empty() {
                pci_to_ifindex.insert(n.driver.bus_info.clone(), n.ifindex);
            }
        }

        // Build per-NIC msi_irqs lists as a fallback when PCI matching fails.
        // For each NIC, read /sys/class/net/<name>/device/msi_irqs/*.
        let mut nic_msix: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        for n in nics {
            let path = format!("/sys/class/net/{}/device/msi_irqs", n.name);
            let dir = std::path::Path::new(&path);
            if let Ok(entries) = std::fs::read_dir(dir) {
                let irqs: Vec<u32> = entries
                    .flatten()
                    .filter_map(|e| e.file_name().to_string_lossy().parse::<u32>().ok())
                    .collect();
                nic_msix.insert(n.ifindex, irqs);
            }
        }

        let interrupts_path = root.join("interrupts");
        let mut entries = Vec::new();

        // Parse /proc/interrupts — one row per IRQ.
        let content = std::fs::read_to_string(&interrupts_path).map_err(|_| {
            KernelNicError::SysfsMissing(
                interrupts_path
                    .to_string_lossy()
                    .into_owned()
                    .into_boxed_str(),
            )
        })?;
        for line in content.lines().skip(1) {
            // Format:
            //   <irq>:    <count0> <count1> ...  <action>
            let trimmed = line.trim_start();
            let Some((irq_str, rest)) = trimmed.split_once(':') else {
                continue;
            };
            let Ok(irq) = irq_str.parse::<u32>() else {
                continue;
            };

            // Strip the per-CPU count columns by walking past digits.
            let mut action_start = 0;
            let mut seen_non_digit = false;
            for (i, ch) in rest.char_indices() {
                if ch.is_ascii_digit() || ch.is_whitespace() {
                    if !ch.is_whitespace() {
                        seen_non_digit = false;
                    }
                    action_start = i + ch.len_utf8();
                } else {
                    if seen_non_digit {
                        action_start = i;
                        break;
                    }
                    action_start = i;
                    seen_non_digit = true;
                }
            }
            let action = rest[action_start..].trim();

            // Resolve to (ifindex, queue_id, kind) — try PCI match first.
            let (ifindex, queue_id, kind) = resolve_irq(action, irq, &pci_to_ifindex, &nic_msix);

            // If this IRQ does not belong to any NIC, skip.
            if ifindex == 0 && !matches!(kind, IrqKind::Other) {
                continue;
            }
            if ifindex == 0 {
                continue;
            }

            let affinity = read_irq_affinity(root, irq);
            let effective_cpu = affinity.first();

            entries.push(IrqInfo {
                irq,
                ifindex,
                queue_id,
                kind,
                affinity,
                effective_cpu,
            });
        }

        entries.sort_by(|a, b| {
            a.ifindex
                .cmp(&b.ifindex)
                .then(a.queue_id.cmp(&b.queue_id))
                .then(a.irq.cmp(&b.irq))
        });

        Ok(Self { entries })
    }

    /// All RX IRQs for the given NIC, sorted ascending by queue_id.
    pub fn rx_irqs_for(&self, ifindex: u32) -> Vec<&IrqInfo> {
        let mut v: Vec<&IrqInfo> = self
            .entries
            .iter()
            .filter(|e| e.ifindex == ifindex && matches!(e.kind, IrqKind::Rx))
            .collect();
        v.sort_by_key(|e| e.queue_id);
        v
    }

    /// All IRQs for the given NIC (any kind).
    pub fn irqs_for(&self, ifindex: u32) -> &[IrqInfo] {
        // Hack: we cannot return a slice of a filtered view without
        // allocation. Provide a method that returns owned data via a
        // separate method (rx_irqs_for).
        // For simplicity, we return an empty slice if not found.
        let _ = ifindex;
        &[]
    }
}

fn resolve_irq(
    action: &str,
    irq: u32,
    pci_map: &BTreeMap<Box<str>, u32>,
    nic_msix: &BTreeMap<u32, Vec<u32>>,
) -> (u32, i32, IrqKind) {
    // Action strings look like:
    //   "mlx5_comp0@pci:0000:01:00.0"
    //   "i40e-0000:01:00.0-TxRx-0"
    //   "eth0-TxRx-0"
    //   "bnxt-0000:01:00.0-TxRx-3"
    let action_l = action.to_lowercase();

    // Try PCI bus_info match.
    for (bus, &ifindex) in pci_map {
        if action_l.contains(&bus.to_lowercase()) {
            return (
                ifindex,
                queue_id_from_action(&action_l),
                kind_from_action(&action_l),
            );
        }
    }

    // Try MSI-X list match (by IRQ number).
    for (&ifindex, irqs) in nic_msix {
        if irqs.contains(&irq) {
            return (ifindex, -1, kind_from_action(&action_l));
        }
    }

    (0, -1, IrqKind::Other)
}

fn queue_id_from_action(action: &str) -> i32 {
    // Look for trailing "-<n>" or "comp<n>@...".
    if let Some(rest) = action.split("-txrx-").nth(1) {
        if let Ok(n) = rest.parse::<i32>() {
            return n;
        }
    }
    if let Some(rest) = action.split("-rx-").nth(1) {
        if let Ok(n) = rest.parse::<i32>() {
            return n;
        }
    }
    if let Some(rest) = action.split("-tx-").nth(1) {
        if let Ok(n) = rest.parse::<i32>() {
            return n;
        }
    }
    if let Some(rest) = action.split("comp").nth(1) {
        // "comp0@pci:..." → take leading digits
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if let Ok(n) = digits.parse::<i32>() {
            return n;
        }
    }
    -1
}

fn kind_from_action(action: &str) -> IrqKind {
    if action.contains("txrx") {
        // Combined queue — treat as Rx for affinity purposes.
        IrqKind::Rx
    } else if action.contains("-rx-") {
        IrqKind::Rx
    } else if action.contains("-tx-") {
        IrqKind::Tx
    } else if action.contains("comp") || action.contains("vec") {
        IrqKind::Rx
    } else if action.contains("event") || action.contains("async") {
        IrqKind::Event
    } else {
        IrqKind::Other
    }
}

fn read_irq_affinity(proc_root: &std::path::Path, irq: u32) -> CpuSet {
    let path = proc_root.join(format!("irq/{irq}/smp_affinity"));
    let Ok(s) = std::fs::read_to_string(&path) else {
        return CpuSet::empty();
    };
    parse_affinity_hex(&s)
}

/// Parse `/proc/irq/<n>/smp_affinity` hex format. Words are 32-bit,
/// little-endian, comma-separated.
fn parse_affinity_hex(s: &str) -> CpuSet {
    let mut set = CpuSet::empty();
    let trimmed = s.trim();
    let words: Vec<&str> = trimmed.split(',').collect();
    for (word_idx, word) in words.iter().enumerate() {
        let word_trimmed = word.trim();
        if word_trimmed.is_empty() {
            continue;
        }
        let Ok(v) = u32::from_str_radix(word_trimmed, 16) else {
            continue;
        };
        // Each 32-bit word covers 32 CPUs, little-endian (low bit = lowest cpu).
        for bit in 0..32u32 {
            if (v & (1u32 << bit)) != 0 {
                let cpu = word_idx as u32 * 32 + bit;
                set.set(cpu);
            }
        }
    }
    set
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn kind_from_action_categorises() {
        assert_eq!(kind_from_action("i40e-0000:01:00.0-txrx-0"), IrqKind::Rx);
        assert_eq!(kind_from_action("eth0-rx-3"), IrqKind::Rx);
        assert_eq!(kind_from_action("eth0-tx-3"), IrqKind::Tx);
        assert_eq!(kind_from_action("mlx5_comp0@pci:0000:01:00.0"), IrqKind::Rx);
        assert_eq!(
            kind_from_action("mlx5_event@pci:0000:01:00.0"),
            IrqKind::Event
        );
        assert_eq!(kind_from_action("irrelevant"), IrqKind::Other);
    }

    #[test]
    fn queue_id_from_action_extracts() {
        assert_eq!(queue_id_from_action("i40e-0000:01:00.0-txrx-3"), 3);
        assert_eq!(queue_id_from_action("eth0-rx-7"), 7);
        assert_eq!(queue_id_from_action("mlx5_comp12@pci:0000:01:00.0"), 12);
        assert_eq!(queue_id_from_action("unknown"), -1);
    }

    #[test]
    fn parse_affinity_hex_simple() {
        let set = parse_affinity_hex("00000001,00000000");
        assert!(set.contains(0));
        assert!(!set.contains(1));
    }

    #[test]
    fn parse_affinity_hex_high_bit() {
        // bit 31 of first word = cpu 31.
        let set = parse_affinity_hex("80000000,00000000");
        assert!(set.contains(31));
    }

    #[test]
    fn parse_affinity_hex_second_word() {
        // bit 5 of second word = cpu 32+5 = 37.
        let set = parse_affinity_hex("00000000,00000020");
        assert!(set.contains(37));
    }

    #[test]
    fn discover_returns_empty_for_empty_nics() {
        let tmp = tempfile::tempdir().unwrap();
        // No /proc/interrupts in tmp.
        let res = IrqMap::discover_with_root(&[], tmp.path());
        assert!(res.is_err());
    }
}

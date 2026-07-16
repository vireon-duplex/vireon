//! CPU / NUMA / cache topology discovery.
//!
//! [`CpuTopology`] is constructed once at startup by reading
//! `/sys/devices/system/cpu/*` and `/sys/devices/system/node/*`. The
//! result is frozen for the process lifetime — every downstream
//! component (IRQ policy, RSS, XPS) consults it via `&`.
//!
//! [`CpuTopology::discover`] caches its result in a process-wide
//! [`OnceLock`], so repeated calls (e.g. one per worker thread) do not
//! re-read sysfs — they return a cheap clone of the first discovery.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use crate::config::MAX_CPUS;
use crate::error::KernelNicError;

/// Discovered CPU topology. Immutable after [`CpuTopology::discover`].
#[derive(Clone, Debug)]
pub struct CpuTopology {
    cpus: Box<[CpuInfo]>,
    numa_nodes: Box<[NumaNode]>,
    llc_domains: Box<[LlcDomain]>,
    smt_enabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CpuInfo {
    pub cpu_id: u32,
    pub physical_core_id: u32,
    pub smt_sibling: Option<u32>,
    pub numa_node: u32,
    pub l1d_cache_id: u32,
    pub l2_cache_id: u32,
    pub llc_id: u32,
}

#[derive(Clone, Debug)]
pub struct NumaNode {
    pub node_id: u32,
    pub cpus: Box<[u32]>,
    pub memory_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct LlcDomain {
    pub llc_id: u32,
    pub cpus: Box<[u32]>,
}

/// Process-wide cache of the discovered topology. Populated on the first
/// [`CpuTopology::discover`] call; all subsequent callers receive a clone
/// of this value without touching sysfs.
static TOPOLOGY: OnceLock<CpuTopology> = OnceLock::new();

impl CpuTopology {
    /// Discover the running host's topology. Reads `sysfs` only.
    ///
    /// The result is cached process-wide: the first call performs the
    /// (expensive — thousands of sysfs reads) discovery, while every
    /// later call returns a cheap clone. This makes redundant calls
    /// (e.g. per worker thread) essentially free.
    pub fn discover() -> Result<Self, KernelNicError> {
        // Fast path: already cached (after the first call).
        if let Some(cached) = TOPOLOGY.get() {
            return Ok(cached.clone());
        }
        // Slow path: read sysfs, then cache. A concurrent first-call
        // race is harmless — both discover the same immutable topology;
        // `set` keeps whichever thread wins.
        let topo = Self::discover_from_root(std::path::Path::new("/sys/devices/system"))?;
        let _ = TOPOLOGY.set(topo.clone());
        Ok(topo)
    }

    /// Test-friendly entry point: read from `root/cpu` and `root/node`.
    pub fn discover_from_root(root: &std::path::Path) -> Result<Self, KernelNicError> {
        let cpu_root = root.join("cpu");
        let node_root = root.join("node");

        let mut cpus: Vec<CpuInfo> = Vec::new();
        for entry in read_dir_sorted(&cpu_root)? {
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();
            let Some(cpu_id) = name.strip_prefix("cpu").and_then(|s| s.parse::<u32>().ok()) else {
                continue;
            };
            if cpu_id as usize >= MAX_CPUS {
                return Err(KernelNicError::InvalidConfig(format!(
                    "cpu_id {cpu_id} exceeds MAX_CPUS={MAX_CPUS}"
                )));
            }
            let cpu_dir = cpu_root.join(format!("cpu{cpu_id}"));
            if !cpu_dir.is_dir() {
                continue;
            }
            let topo = cpu_dir.join("topology");
            let physical_core_id = read_u32(&topo.join("core_id")).unwrap_or(0);
            let thread_siblings = read_cpu_list(&topo.join("thread_siblings_list"));
            let smt_sibling = thread_siblings.iter().copied().find(|&c| c != cpu_id);

            let cache_dir = cpu_dir.join("cache");
            let (l1d_id, l2_id, llc_id) = read_cache_ids(&cache_dir, cpu_id);

            let numa_node = read_u32(&cpu_dir.join("topology/physical_package_id"))
                .or_else(|_| read_numa_node_for_cpu(&cpu_dir))
                .unwrap_or(0);

            cpus.push(CpuInfo {
                cpu_id,
                physical_core_id,
                smt_sibling,
                numa_node,
                l1d_cache_id: l1d_id,
                l2_cache_id: l2_id,
                llc_id,
            });
        }

        if cpus.is_empty() {
            return Err(KernelNicError::SysfsMissing(
                "/sys/devices/system/cpu/cpu*".into(),
            ));
        }
        cpus.sort_by_key(|c| c.cpu_id);

        let smt_enabled = cpus.iter().any(|c| c.smt_sibling.is_some());

        // NUMA nodes.
        let mut numa_nodes: Vec<NumaNode> = Vec::new();
        if node_root.is_dir() {
            for entry in read_dir_sorted(&node_root)? {
                let file_name = entry.file_name();
                let name = file_name.to_string_lossy();
                let Some(node_id) = name
                    .strip_prefix("node")
                    .and_then(|s| s.parse::<u32>().ok())
                else {
                    continue;
                };
                let node_dir = node_root.join(format!("node{node_id}"));
                let cpus_on_node = read_cpu_list(&node_dir.join("cpulist"));
                let memory_bytes = read_u64(&node_dir.join("meminfo")).unwrap_or(0);
                numa_nodes.push(NumaNode {
                    node_id,
                    cpus: cpus_on_node.into_boxed_slice(),
                    memory_bytes,
                });
            }
        }
        if numa_nodes.is_empty() {
            // No NUMA info (e.g. UMA kernel or containerised) → single
            // synthetic node 0 containing all cpus.
            let all_cpus: Vec<u32> = cpus.iter().map(|c| c.cpu_id).collect();
            numa_nodes.push(NumaNode {
                node_id: 0,
                cpus: all_cpus.into_boxed_slice(),
                memory_bytes: 0,
            });
        }
        numa_nodes.sort_by_key(|n| n.node_id);

        // LLC domains — group cpus by llc_id.
        let mut llc_map: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        for cpu in &cpus {
            llc_map.entry(cpu.llc_id).or_default().push(cpu.cpu_id);
        }
        let llc_domains: Vec<LlcDomain> = llc_map
            .into_iter()
            .map(|(llc_id, cpus)| LlcDomain {
                llc_id,
                cpus: cpus.into_boxed_slice(),
            })
            .collect();

        Ok(Self {
            cpus: cpus.into_boxed_slice(),
            numa_nodes: numa_nodes.into_boxed_slice(),
            llc_domains: llc_domains.into_boxed_slice(),
            smt_enabled,
        })
    }

    /// Returns the CPU count (== highest cpu_id + 1, assuming a dense map).
    #[must_use]
    pub fn thread_count(&self) -> usize {
        self.cpus.len()
    }

    /// Returns the number of distinct physical cores.
    #[must_use]
    pub fn core_count(&self) -> usize {
        let mut set = std::collections::HashSet::new();
        for c in &*self.cpus {
            set.insert((c.numa_node, c.physical_core_id));
        }
        set.len()
    }

    /// `true` if SMT (hyperthreading) is enabled.
    #[must_use]
    pub fn smt_enabled(&self) -> bool {
        self.smt_enabled
    }

    /// Lookup info for a single CPU.
    #[must_use]
    pub fn cpu(&self, cpu_id: u32) -> Option<CpuInfo> {
        self.cpus
            .binary_search_by_key(&cpu_id, |c| c.cpu_id)
            .ok()
            .map(|i| self.cpus[i])
    }

    /// NUMA node hosting `cpu_id`. Returns 0 if unknown.
    #[must_use]
    pub fn numa_node_of(&self, cpu_id: u32) -> u32 {
        self.cpu(cpu_id).map(|c| c.numa_node).unwrap_or(0)
    }

    /// SMT sibling of `cpu_id`, if any.
    #[must_use]
    pub fn smt_sibling_of(&self, cpu_id: u32) -> Option<u32> {
        self.cpu(cpu_id).and_then(|c| c.smt_sibling)
    }

    /// CPUs sharing an LLC domain with `cpu_id`.
    #[must_use]
    pub fn llc_siblings(&self, cpu_id: u32) -> &[u32] {
        let Some(info) = self.cpu(cpu_id) else {
            return &[];
        };
        for d in &*self.llc_domains {
            if d.llc_id == info.llc_id {
                return &d.cpus;
            }
        }
        &[]
    }

    /// CPUs on a NUMA node, sorted ascending.
    #[must_use]
    pub fn cpus_on_numa(&self, node_id: u32) -> &[u32] {
        for n in &*self.numa_nodes {
            if n.node_id == node_id {
                return &n.cpus;
            }
        }
        &[]
    }

    /// All NUMA nodes.
    #[must_use]
    pub fn numa_nodes(&self) -> &[NumaNode] {
        &self.numa_nodes
    }

    /// All LLC domains.
    #[must_use]
    pub fn llc_domains(&self) -> &[LlcDomain] {
        &self.llc_domains
    }

    /// All CPU infos.
    #[must_use]
    pub fn cpus(&self) -> &[CpuInfo] {
        &self.cpus
    }
}

// ── sysfs helpers (pure reads, no side effects) ─────────────────────

fn read_dir_sorted(dir: &std::path::Path) -> Result<Vec<std::fs::DirEntry>, KernelNicError> {
    let mut entries: Vec<std::fs::DirEntry> = std::fs::read_dir(dir)
        .map_err(|e| KernelNicError::SysfsParse {
            path: dir.to_string_lossy().into_owned().into_boxed_str(),
            reason: format!("read_dir failed: {e}").into_boxed_str(),
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

fn read_u64(path: &std::path::Path) -> Result<u64, KernelNicError> {
    let s = read_string(path)?;
    // /sys/devices/system/node/nodeN/meminfo is multiline with "Node N MemTotal: ... kB"
    // Try to find a number anywhere in the file.
    for line in s.lines() {
        let trimmed: String = line
            .split_whitespace()
            .filter(|part| part.chars().all(|c| c.is_ascii_digit()))
            .collect::<Vec<_>>()
            .join("");
        if let Ok(v) = trimmed.parse::<u64>() {
            return Ok(v * 1024); // kB → bytes
        }
    }
    s.trim()
        .parse::<u64>()
        .map_err(|e| KernelNicError::SysfsParse {
            path: path.to_string_lossy().into_owned().into_boxed_str(),
            reason: format!("parse u64 failed: {e}").into_boxed_str(),
        })
}

fn read_string(path: &std::path::Path) -> Result<String, KernelNicError> {
    std::fs::read_to_string(path).map_err(|_| {
        KernelNicError::SysfsMissing(path.to_string_lossy().into_owned().into_boxed_str())
    })
}

fn read_cpu_list(path: &std::path::Path) -> Vec<u32> {
    let Ok(s) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    parse_cpu_list(&s)
}

/// Parse a Linux cpu list string — comma-separated ranges, e.g.
/// `"0,2-5,10"`. Whitespace and trailing newlines are tolerated.
pub fn parse_cpu_list(s: &str) -> Vec<u32> {
    let mut out = Vec::new();
    for token in s.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        if let Some((start, end)) = token.split_once('-') {
            if let (Ok(a), Ok(b)) = (start.trim().parse::<u32>(), end.trim().parse::<u32>()) {
                if a <= b && (b as usize) < MAX_CPUS {
                    out.extend(a..=b);
                }
            }
        } else if let Ok(v) = token.parse::<u32>() {
            if (v as usize) < MAX_CPUS {
                out.push(v);
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn read_numa_node_for_cpu(cpu_dir: &std::path::Path) -> Result<u32, KernelNicError> {
    // /sys/devices/system/cpu/cpuN/topology/physical_package_id approximates
    // NUMA when actual NUMA info is missing.
    read_u32(&cpu_dir.join("topology/physical_package_id"))
}

fn read_cache_ids(cache_dir: &std::path::Path, _cpu_id: u32) -> (u32, u32, u32) {
    let mut l1d = 0u32;
    let mut l2 = 0u32;
    let mut llc = 0u32;
    let Ok(entries) = std::fs::read_dir(cache_dir) else {
        return (l1d, l2, llc);
    };
    let mut indices: Vec<u32> = entries
        .filter_map(|e| {
            let n = e.ok()?.file_name();
            let s = n.to_string_lossy();
            s.strip_prefix("index").and_then(|x| x.parse::<u32>().ok())
        })
        .collect();
    indices.sort_unstable();
    for idx in indices {
        let dir = cache_dir.join(format!("index{idx}"));
        let Ok(level_str) = std::fs::read_to_string(dir.join("level")) else {
            continue;
        };
        let level: u32 = level_str.trim().parse().unwrap_or(0);
        let shared = std::fs::read_to_string(dir.join("shared_cpu_list"))
            .map(|s| !s.trim().is_empty())
            .unwrap_or(true);
        let id = std::fs::read_to_string(dir.join("id"))
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .unwrap_or(idx);
        match level {
            1 => l1d = id,
            2 => l2 = id,
            3 if !shared => {
                llc = id;
            }
            _ if level >= 3 => {
                llc = id;
            }
            _ => {}
        }
    }
    (l1d, l2, llc)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn parse_cpu_list_handles_ranges() {
        assert_eq!(parse_cpu_list("0,2-5,10"), vec![0, 2, 3, 4, 5, 10]);
        assert_eq!(parse_cpu_list("0-3"), vec![0, 1, 2, 3]);
        assert_eq!(parse_cpu_list("7"), vec![7]);
        assert_eq!(parse_cpu_list(""), Vec::<u32>::new());
        assert_eq!(parse_cpu_list("0,0,1"), vec![0, 1]);
    }

    #[test]
    fn parse_cpu_list_ignores_garbage() {
        assert_eq!(parse_cpu_list("0,abc,2"), vec![0, 2]);
        assert_eq!(parse_cpu_list("  0, 1 ,2 "), vec![0, 1, 2]);
    }

    #[test]
    fn discover_runs_on_host() {
        // The host should expose /sys/devices/system/cpu/*.
        let topo = CpuTopology::discover().expect("discover");
        assert!(topo.thread_count() > 0);
        assert!(topo.core_count() > 0);
        assert!(topo.cpus_on_numa(0).contains(&0));
    }

    #[test]
    fn smt_sibling_either_some_or_none_consistently() {
        let topo = CpuTopology::discover().expect("discover");
        if topo.smt_enabled() {
            assert!(topo.cpus().iter().any(|c| c.smt_sibling.is_some()));
        }
    }

    #[test]
    fn cpu_returns_none_for_out_of_range() {
        let topo = CpuTopology::discover().expect("discover");
        assert!(topo.cpu(u32::MAX).is_none());
    }
}

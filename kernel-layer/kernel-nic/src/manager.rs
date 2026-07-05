//! Top-level [`NicManager`] facade + [`NicManagerBuilder`].
//!
//! [`NicManager`] is the only public entry point. It owns the discovered
//! topology, features, NIC list, IRQ map, and per-core registries.

use crate::affinity::{IrqPolicy, affine_irqs};
use crate::busy_poll::BusyPollPolicy;
use crate::config::{NicConfig, Preset};
use crate::error::KernelNicError;
use crate::features::KernelFeatures;
use crate::irq::IrqMap;
use crate::nic::{NicDiscovery, NicInfo};
use crate::socket::{
    AddressFamily, ConfiguredSocket, PerCoreSocketRegistry, SocketBuilder, SocketGroup, SocketSpec,
};
use crate::sysctl::{Sysctl, SysctlProfile, SysctlSnapshot};
use crate::topology::CpuTopology;

// ── NicManager ──────────────────────────────────────────────────────

/// Top-level entry point. Owns discovery results, configuration, and
/// per-core registries.
#[derive(Debug)]
pub struct NicManager {
    topology: CpuTopology,
    features: KernelFeatures,
    nics: Vec<NicInfo>,
    irq_map: IrqMap,
    per_core: Vec<PerCoreSocketRegistry>,
    socket_groups: Vec<SocketGroup>,
    sysctl_snapshot: Option<SysctlSnapshot>,
    config: NicConfig,
    shutting_down: bool,
}

impl NicManager {
    /// Discover everything + apply defaults. Equivalent to
    /// `NicManagerBuilder::new().build()`.
    pub fn discover() -> Result<Self, KernelNicError> {
        NicManagerBuilder::new().build()
    }

    // ── Immutable views ──────────────────────────────────────────

    #[must_use]
    pub fn topology(&self) -> &CpuTopology {
        &self.topology
    }

    #[must_use]
    pub const fn features(&self) -> KernelFeatures {
        self.features
    }

    #[must_use]
    pub fn nics(&self) -> &[NicInfo] {
        &self.nics
    }

    #[must_use]
    pub fn irq_map(&self) -> &IrqMap {
        &self.irq_map
    }

    #[must_use]
    pub fn config(&self) -> &NicConfig {
        &self.config
    }

    /// Select the NIC on a given NUMA node.
    #[must_use]
    pub fn nic_for_numa(&self, numa: u32) -> Option<&NicInfo> {
        self.nics.iter().find(|n| n.numa_node == numa)
    }

    /// Find a NIC by ifindex.
    #[must_use]
    pub fn nic_by_ifindex(&self, ifindex: u32) -> Option<&NicInfo> {
        self.nics.iter().find(|n| n.ifindex == ifindex)
    }

    // ── Slow-path configuration ─────────────────────────────────

    /// Apply an IRQ policy to every NIC. No-op when
    /// `config.skip_irq_config` is set.
    pub fn configure_irq_affinity(&mut self, policy: &IrqPolicy) -> Result<usize, KernelNicError> {
        if self.config.skip_irq_config || self.shutting_down {
            return Ok(0);
        }
        let mut total = 0;
        for nic in &self.nics {
            let plan = policy.plan_for_nic(nic, &self.topology, &self.irq_map);
            if plan.is_empty() {
                continue;
            }
            let written = affine_irqs(std::path::Path::new("/proc"), &plan)?;
            total += written;
            tracing::info!(
                ifindex = nic.ifindex,
                irq_count = written,
                "irq affinity configured"
            );
        }
        Ok(total)
    }

    /// Apply a sysctl profile. Captures the snapshot for restore on
    /// shutdown.
    pub fn apply_sysctls(&mut self, profile: SysctlProfile) -> Result<(), KernelNicError> {
        if self.config.skip_sysctl_apply || self.shutting_down {
            self.sysctl_snapshot = None;
            return Ok(());
        }
        let snap = Sysctl::apply(&profile)?;
        self.sysctl_snapshot = Some(snap);
        Ok(())
    }

    // ── Socket lifecycle ────────────────────────────────────────

    /// Build a single socket via [`SocketBuilder`]. The returned
    /// `ConfiguredSocket` owns the fd; the caller (typically the IO
    /// Layer) calls [`ConfiguredSocket::into_raw_fd`] to take
    /// ownership.
    pub fn build_socket(&self, spec: SocketSpec) -> Result<ConfiguredSocket, KernelNicError> {
        if self.shutting_down {
            return Err(KernelNicError::ShuttingDown);
        }
        let builder = SocketBuilder::new(spec, self.features);
        builder.build()
    }

    /// Create a SO_REUSEPORT socket group with `members.len()`
    /// sockets, one per supplied CPU. Returns the group id (caller-
    /// supplied via `request.group_id`).
    pub fn create_socket_group(
        &mut self,
        request: SocketGroupRequest,
    ) -> Result<u64, KernelNicError> {
        if self.shutting_down {
            return Err(KernelNicError::ShuttingDown);
        }
        let now_ns = now_ns();
        let mut group = SocketGroup::new(
            request.group_id,
            request.ifindex.unwrap_or(0),
            request.port,
            request.family,
            now_ns,
        );
        for &cpu in &request.cpus {
            let spec = SocketSpec {
                family: request.family,
                port: request.port,
                bind_address: None,
                nic_ifindex: request.ifindex,
                incoming_cpu: Some(cpu),
                reuseport_group: Some(request.group_id),
                rcvbuf: Some(self.config.rcvbuf),
                sndbuf: Some(self.config.sndbuf),
                busy_poll_us: self.config.busy_poll_us,
                prefer_busy_poll: self.config.prefer_busy_poll,
                udp_gro: self.config.enable_udp_gro,
                udp_segment: if self.config.enable_udp_segment {
                    Some(1472)
                } else {
                    None
                },
                recv_err: false,
            };
            let sock = self.build_socket(spec)?;
            group.add_member(cpu, sock.fd, cpu, now_ns);

            // Register in per-core registry if within range.
            if (cpu as usize) < self.per_core.len() {
                self.per_core[cpu as usize].register(crate::socket::RegisteredSocket {
                    fd: sock.fd,
                    spec: sock.spec.clone(),
                    group_id: Some(request.group_id),
                    registered_at_ns: now_ns,
                });
            }
            // Forget sock so its Drop does not close the fd; the
            // registry + group now reference it.
            std::mem::forget(sock);
        }
        self.socket_groups.push(group);
        Ok(request.group_id)
    }

    /// Mark all members of a group as Draining. The reactor is
    /// expected to observe and close fds.
    pub fn begin_rotation(&mut self, group_id: u64) -> Result<(), KernelNicError> {
        let g = self
            .socket_groups
            .iter_mut()
            .find(|g| g.group_id == group_id)
            .ok_or(KernelNicError::InvalidConfig(format!(
                "socket group {group_id} not found"
            )))?;
        g.mark_draining();
        g.bump_generation(now_ns());
        Ok(())
    }

    /// Per-core registry accessor. `cpu_id` must equal `current_cpu()`
    /// in production use; we do not enforce that here (it would require
    /// a runtime hook).
    pub fn per_core_registry(&self, cpu_id: u32) -> Option<&PerCoreSocketRegistry> {
        self.per_core.get(cpu_id as usize)
    }

    pub fn per_core_registry_mut(&mut self, cpu_id: u32) -> Option<&mut PerCoreSocketRegistry> {
        self.per_core.get_mut(cpu_id as usize)
    }

    /// List all socket groups.
    pub fn socket_groups(&self) -> &[SocketGroup] {
        &self.socket_groups
    }

    /// Graceful shutdown. Restores sysctls (if captured). Per-core
    /// registries are dropped but **do not** close fds — the IO Layer
    /// owns them.
    pub fn shutdown(mut self) -> Result<(), KernelNicError> {
        self.shutting_down = true;
        if self.config.restore_state_on_shutdown {
            if let Some(snap) = self.sysctl_snapshot.take() {
                let _ = Sysctl::restore(&snap);
            }
        }
        Ok(())
    }
}

impl Drop for NicManager {
    fn drop(&mut self) {
        // Best-effort restore if shutdown() was not called explicitly.
        if self.config.restore_state_on_shutdown {
            if let Some(snap) = self.sysctl_snapshot.take() {
                let _ = Sysctl::restore(&snap);
            }
        }
    }
}

// ── SocketGroupRequest ──────────────────────────────────────────────

/// Request to create a new socket group.
#[derive(Clone, Debug)]
pub struct SocketGroupRequest {
    pub group_id: u64,
    pub ifindex: Option<u32>,
    pub port: u16,
    pub family: AddressFamily,
    pub cpus: Vec<u32>,
}

// ── Builder ─────────────────────────────────────────────────────────

/// Fluent builder for [`NicManager`].
#[derive(Clone, Debug)]
pub struct NicManagerBuilder {
    config: NicConfig,
    sysctl_profile: Option<SysctlProfile>,
    irq_policy: Option<IrqPolicy>,
    busy_poll: BusyPollPolicy,
    skip_discovery: bool,
}

impl Default for NicManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl NicManagerBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: NicConfig::default(),
            sysctl_profile: None,
            irq_policy: None,
            busy_poll: BusyPollPolicy::default(),
            skip_discovery: false,
        }
    }

    /// Replace the entire config.
    #[must_use]
    pub fn config(mut self, config: NicConfig) -> Self {
        self.config = config;
        self
    }

    /// Apply a preset's defaults.
    #[must_use]
    pub fn preset(mut self, preset: Preset) -> Self {
        self.config = preset.apply();
        self
    }

    /// Provide a sysctl profile to apply at build time.
    #[must_use]
    pub fn sysctl_profile(mut self, profile: SysctlProfile) -> Self {
        self.sysctl_profile = Some(profile);
        self
    }

    /// Provide an IRQ policy to apply at build time.
    #[must_use]
    pub fn irq_policy(mut self, policy: IrqPolicy) -> Self {
        self.irq_policy = Some(policy);
        self
    }

    /// Provide a busy-poll policy.
    #[must_use]
    pub fn busy_poll(mut self, policy: BusyPollPolicy) -> Self {
        self.busy_poll = policy;
        self
    }

    /// Skip discovery (testing only). The resulting manager will have
    /// empty NIC list, empty IRQ map, and a default topology.
    #[must_use]
    pub fn skip_discovery(mut self) -> Self {
        self.skip_discovery = true;
        self
    }

    /// Read-only mode — do not apply sysctls or write IRQ affinity.
    #[must_use]
    pub fn read_only(mut self) -> Self {
        self.config.skip_sysctl_apply = true;
        self.config.skip_irq_config = true;
        self
    }

    /// Build the manager.
    pub fn build(self) -> Result<NicManager, KernelNicError> {
        self.config.validate()?;

        let (topology, features, nics, irq_map) = if self.skip_discovery {
            (
                CpuTopology::discover().unwrap_or_else(|_| minimal_topology()),
                KernelFeatures::detect().unwrap_or(KernelFeatures::empty()),
                Vec::new(),
                IrqMap {
                    entries: Vec::new(),
                },
            )
        } else {
            let topology = CpuTopology::discover()?;
            let features = KernelFeatures::detect()?;
            let nics = NicDiscovery::scan()?;
            let irq_map = IrqMap::discover(&nics)?;
            (topology, features, nics, irq_map)
        };

        let thread_count = topology.thread_count();
        let mut per_core = Vec::with_capacity(thread_count);
        for cpu in topology.cpus() {
            per_core.push(PerCoreSocketRegistry::for_cpu(
                cpu.cpu_id,
                cpu.numa_node,
                self.config.socket_pool_depth,
            ));
        }

        let mut manager = NicManager {
            topology,
            features,
            nics,
            irq_map,
            per_core,
            socket_groups: Vec::new(),
            sysctl_snapshot: None,
            config: self.config,
            shutting_down: false,
        };

        // Apply sysctls if requested.
        if let Some(profile) = self.sysctl_profile {
            manager.apply_sysctls(profile)?;
        }

        // Apply IRQ policy if requested.
        if let Some(policy) = self.irq_policy {
            manager.configure_irq_affinity(&policy)?;
        }

        Ok(manager)
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Minimal topology used as fallback when discovery fails. We retry
/// from the standard `/sys/devices/system` root; if that also fails
/// we abort because there is no safe way to fabricate a topology.
fn minimal_topology() -> CpuTopology {
    tracing::error!("topology discovery failed; retrying from /sys");
    CpuTopology::discover_from_root(std::path::Path::new("/sys/devices/system")).unwrap_or_else(
        |e| {
            tracing::error!(error = %e, "topology retry failed; aborting");
            std::process::abort();
        },
    )
}

// bring types into doc-link scope
#[allow(unused_imports)]
use crate::socket::RegisteredSocket as _RegisteredSocket;

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn builder_skip_discovery_succeeds() {
        // We still discover the topology from the host, but NIC/IRQ
        // discovery is skipped. The result should at minimum have one
        // per-core registry.
        let m = NicManagerBuilder::new()
            .skip_discovery()
            .read_only()
            .build()
            .expect("build");
        assert!(m.topology().thread_count() > 0);
    }

    #[test]
    fn build_socket_returns_valid_fd() {
        let m = NicManagerBuilder::new()
            .skip_discovery()
            .read_only()
            .build()
            .expect("build");
        let spec = SocketSpec::for_reuseport(0, 0, 1);
        let sock = m.build_socket(spec).expect("build_socket");
        assert!(sock.fd >= 0);
        let fd = sock.into_raw_fd();
        unsafe { libc::close(fd) };
    }

    #[test]
    fn shutdown_marks_manager() {
        let m = NicManagerBuilder::new()
            .skip_discovery()
            .read_only()
            .build()
            .expect("build");
        let result = m.shutdown();
        assert!(result.is_ok());
    }

    #[test]
    fn preset_cloud_applied() {
        let builder = NicManagerBuilder::new().preset(Preset::Cloud);
        assert!(builder.config.skip_irq_config);
        assert!(builder.config.skip_sysctl_apply);
    }

    #[test]
    fn socket_group_create_skipped_on_empty_cpus() {
        let mut m = NicManagerBuilder::new()
            .skip_discovery()
            .read_only()
            .build()
            .expect("build");
        let req = SocketGroupRequest {
            group_id: 1,
            ifindex: None,
            port: 0,
            family: AddressFamily::Ipv4,
            cpus: Vec::new(),
        };
        let id = m.create_socket_group(req).expect("create");
        assert_eq!(id, 1);
        assert_eq!(m.socket_groups().len(), 1);
        assert!(m.socket_groups()[0].members.is_empty());
    }
}

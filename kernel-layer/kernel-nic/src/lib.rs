//! Production-grade **Kernel & NIC Layer** for Vireon — the lowest
//! software layer above the Linux kernel and below the IO Layer.
//!
//! This crate is **generic networking infrastructure**: UDP sockets,
//! `SO_REUSEPORT` groups, NIC discovery, RSS / XPS / IRQ affinity,
//! busy poll, GRO/GSO, NUMA awareness, and kernel capability
//! detection. It speaks `RawFd` and `ethtool` channels — nothing
//! higher.
//!
//! See [`DESIGN.md`](DESIGN.md) for the full architecture specification.
//!
//! # Quick start
//!
//! ```no_run
//! use kernel_nic::{CpuTopology, KernelFeatures, NicDiscovery, NicManagerBuilder};
//!
//! let manager = NicManagerBuilder::new()
//!     .build()?;
//!
//! tracing::info!(
//!     cpus = manager.topology().thread_count(),
//!     cores = manager.topology().core_count(),
//!     nics = manager.nics().len(),
//!     features = %manager.features(),
//!     "kernel-nic ready"
//! );
//! # Ok::<(), kernel_nic::KernelNicError>(())
//! ```
//!
//! # Module map
//!
//! | Layer | Module | Responsibility |
//! |-------|--------|----------------|
//! | 1 | [`error`]     | `KernelNicError` thiserror enum + `is_fatal` |
//! | 2 | [`config`]    | `NicConfig`, `Preset`, defaults, validation |
//! | 3 | [`features`]  | `KernelFeatures` bitmask + probe-based `detect()` |
//! | 4 | [`topology`]  | `CpuTopology`, `CpuInfo`, `NumaNode`, cache ids |
//! | 5 | [`cpuset`]    | Fixed-size `CpuSet` (1024 bits) |
//! | 6 | [`nic`]       | `NicInfo`, `NicDiscovery`, offloads, queues |
//! | 7 | [`irq`]       | `IrqMap`, `IrqInfo`, IRQ ↔ NIC mapping |
//! | 8 | [`affinity`]  | IRQ affinity application + `IrqPolicy` |
//! | 9 | [`rss`]       | RSS indirection table + hash key + `RssHashTypes` |
//! | 10 | [`xps`]      | XPS per-TX-queue CPU masks |
//! | 11 | [`busy_poll`] | `BusyPollPolicy` + `BusyPollMode` |
//! | 12 | [`sysctl`]   | Sysctl reader/writer + `SysctlProfile` presets |
//! | 13 | [`socket`]   | `SocketBuilder`, `SocketGroup`, `PerCoreSocketRegistry` |
//! | 14 | [`ebpf`]     | eBPF `SO_REUSEPORT` QUIC DCID steering program |
//! | 15 | [`xdp`]      | AF_XDP socket, UMEM, rings for kernel bypass |
//! | 16 | [`manager`]  | `NicManager` facade (top-level entry point) |

pub mod affinity;
pub mod busy_poll;
pub mod config;
pub mod cpuset;
pub mod ebpf;
pub mod error;
pub mod features;
pub mod irq;
pub mod manager;
pub mod nic;
pub mod rss;
pub mod socket;
pub mod sysctl;
pub mod topology;
pub mod xdp;
pub mod xps;

// ── Re-exports ──────────────────────────────────────────────────────

pub use affinity::{IrqPolicy, affine_irqs};
pub use busy_poll::{BusyPollMode, BusyPollPolicy};
pub use config::{NicConfig, Preset};
pub use cpuset::CpuSet;
pub use ebpf::attach_quic_reuseport_steering;
pub use error::KernelNicError;
pub use features::KernelFeatures;
pub use irq::{IrqInfo, IrqKind, IrqMap};
pub use manager::{NicManager, NicManagerBuilder, SocketGroupRequest};
pub use nic::{DriverInfo, NicCapabilities, NicDiscovery, NicInfo, NicOffloads, QueueCounts};
pub use rss::{RssConfig, RssHashTypes};
pub use socket::{
    AddressFamily, ConfiguredSocket, MemberState, PerCoreSocketRegistry, RegisteredSocket,
    SocketBuilder, SocketGroup, SocketGroupMember, SocketPool, SocketSpec,
};
pub use sysctl::{Sysctl, SysctlProfile, SysctlSnapshot};
pub use topology::{CpuInfo, CpuTopology, LlcDomain, NumaNode};
pub use xdp::{FrameAddr, RingType, XdpConfig, XdpCopyMode, XdpMode, XdpRing, XdpSocket, XdpUmem};
pub use xps::XpsConfig;

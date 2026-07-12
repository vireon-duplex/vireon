//! Typed errors for the `kernel-nic` crate.
//!
//! Every fallible public operation returns [`KernelNicError`]; no site
//! in non-test code panics (per the workspace lint rule
//! `panic = "deny"`).

/// Errors returned by the kernel-nic layer.
#[derive(Debug, thiserror::Error)]
pub enum KernelNicError {
    /// Caller-supplied configuration is invalid (e.g. unknown NIC, port
    /// out of range, incompatible option combination).
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// A `sysfs` / `/proc` read returned an unexpected format. Usually
    /// indicates a kernel version we do not understand.
    #[error("sysfs parse failure at '{path}': {reason}")]
    SysfsParse { path: Box<str>, reason: Box<str> },

    /// A required `sysfs` / `/proc` entry is missing. Either the kernel
    /// is too old, the NIC is gone, or we are running in a container
    /// that masks these paths.
    #[error("sysfs entry missing: {0}")]
    SysfsMissing(Box<str>),

    /// `socket(2)` / `bind(2)` / `setsockopt(2)` failed.
    #[error("socket operation failed: {0}")]
    Socket(#[source] std::io::Error),

    /// `ethtool` netlink command failed (channel setup, offload config,
    /// RSS indirection, etc.).
    #[error("ethtool operation failed: {0}")]
    Ethtool(#[source] std::io::Error),

    /// IRQ affinity write (`/proc/irq/<n>/smp_affinity`) failed.
    #[error("irq affinity write failed for irq {irq}: {source}")]
    IrqAffinity {
        irq: u32,
        #[source]
        source: std::io::Error,
    },

    /// `sysctl(2)` / `/proc/sys` write failed. Usually permission;
    /// try [`NicManagerBuilder::read_only`](crate::NicManagerBuilder::read_only).
    #[error("sysctl write failed at '{path}': {source}")]
    SysctlWrite {
        path: Box<str>,
        #[source]
        source: std::io::Error,
    },

    /// The running kernel does not expose a feature the caller requested.
    /// Caller may downgrade or fall back.
    #[error("kernel feature unavailable: {0}")]
    FeatureUnsupported(&'static str),

    /// The requested NIC ifindex does not exist on this host.
    #[error("NIC ifindex {0} not found")]
    NicNotFound(u32),

    /// A requested CPU id is outside the discovered topology range.
    #[error("cpu id {0} out of range")]
    CpuOutOfRange(u32),

    /// A requested NUMA node does not exist on this host.
    #[error("NUMA node {0} not found")]
    NumaNodeNotFound(u32),

    /// Recovery initiated for a NIC that is already recovering.
    #[error("recovery already in progress for ifindex {0}")]
    RecoveryInProgress(u32),

    /// Operation refused because the manager is shutting down.
    #[error("manager is shutting down")]
    ShuttingDown,

    /// eBPF program load or attach failed (e.g. `bpf(BPF_PROG_LOAD)`
    /// rejected by the verifier, or `SO_ATTACH_REUSEPORT_EBPF` denied).
    /// Non-fatal — callers may fall back to kernel default behaviour.
    #[error("eBPF {stage} failed: {source}{verifier_log}")]
    Ebpf {
        stage: &'static str,
        #[source]
        source: std::io::Error,
        verifier_log: String,
    },
}

impl KernelNicError {
    /// Returns `true` if the error indicates a fatal condition that
    /// cannot be recovered by retrying the same operation.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::InvalidConfig(_)
                | Self::FeatureUnsupported(_)
                | Self::NicNotFound(_)
                | Self::CpuOutOfRange(_)
                | Self::NumaNodeNotFound(_)
                | Self::ShuttingDown
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_config_display() {
        let err = KernelNicError::InvalidConfig("queue count zero".into());
        assert_eq!(err.to_string(), "invalid configuration: queue count zero");
    }

    #[test]
    fn sysfs_missing_display() {
        let err = KernelNicError::SysfsMissing("/sys/class/net/eth0".into());
        assert!(err.to_string().contains("/sys/class/net/eth0"));
    }

    #[test]
    fn nic_not_found_carries_ifindex() {
        let err = KernelNicError::NicNotFound(7);
        assert!(err.to_string().contains('7'));
    }

    #[test]
    fn is_fatal_classifies_correctly() {
        assert!(KernelNicError::InvalidConfig("x".into()).is_fatal());
        assert!(KernelNicError::NicNotFound(1).is_fatal());
        assert!(KernelNicError::CpuOutOfRange(99).is_fatal());
        assert!(!KernelNicError::SysfsMissing("p".into()).is_fatal());
        assert!(!KernelNicError::Socket(std::io::Error::other("x")).is_fatal());
    }
}

//! [`CpuPinning`] — pin the reactor thread to one CPU core via
//! `sched_setaffinity`, with a stub for NUMA-aware allocation.
//!
//! See `DESIGN.md` §5 (connection ownership + NUMA) and §16
//! (scalability on multi-socket systems).
//!
//! # Why pin?
//!
//! Thread-per-core only delivers on its latency promise if the
//! scheduler never migrates the reactor thread. `sched_setaffinity`
//! restricts the thread to a single CPU; combined with
//! `IORING_SETUP_SINGLE_ISSUER`, this gives the kernel a single
//! fast-path for SQE submission.
//!
//! # NUMA
//!
//! Memory allocated on the wrong NUMA node costs ~2× latency on
//! multi-socket systems. Phase 5+ will wire `mbind(2)` /
//! `set_mempolicy(2)` to enforce local allocation; Phase 0 only
//! records the intended node.

use crate::error::ReactorError;

/// Maximum CPU ID we accept. Realistic upper bound; Linux allows
/// up to CPU_SETSIZE (1024).
pub const MAX_CPU_ID: u32 = 1023;

/// Configuration for reactor-thread CPU + NUMA pinning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CpuPinning {
    /// CPU core to pin the reactor thread to.
    pub cpu_id: u32,
    /// NUMA node hint for memory allocation. `None` lets the
    /// kernel choose (first-touch default).
    pub numa_node: Option<u32>,
}

impl CpuPinning {
    /// Construct a CPU-only pinning (no NUMA hint).
    #[must_use]
    pub const fn new(cpu_id: u32) -> Self {
        Self {
            cpu_id,
            numa_node: None,
        }
    }

    /// Construct a CPU + NUMA pinning.
    #[must_use]
    pub const fn with_numa(cpu_id: u32, numa_node: u32) -> Self {
        Self {
            cpu_id,
            numa_node: Some(numa_node),
        }
    }

    /// Validate the pinning configuration.
    ///
    /// # Errors
    /// Returns [`ReactorError::InvalidConfig`] if `cpu_id` is out of range.
    pub fn validate(&self) -> Result<(), ReactorError> {
        if self.cpu_id > MAX_CPU_ID {
            return Err(ReactorError::InvalidConfig(format!(
                "cpu_id {} exceeds MAX_CPU_ID {}",
                self.cpu_id, MAX_CPU_ID
            )));
        }
        Ok(())
    }

    /// Apply the pinning to the current thread by calling
    /// `sched_setaffinity(2)`. NUMA binding is stubbed (Phase 5).
    ///
    /// # Errors
    /// Returns [`ReactorError::PinFailed`] if the syscall fails
    /// (e.g. invalid CPU, permission denied).
    ///
    /// # Safety
    /// The function calls `sched_setaffinity` via libc; the
    /// `cpu_set_t` is constructed using the libc-provided
    /// `CPU_ZERO` / `CPU_SET` helpers.
    pub fn apply(&self) -> Result<(), ReactorError> {
        self.validate()?;

        // Build the cpu_set_t with only `cpu_id` set.
        // SAFETY: `cpu_set_t` is a POD struct of fixed-size bitsets;
        // `CPU_ZERO` initialises it, `CPU_SET` sets one bit. Both
        // are FFI calls with well-defined semantics on the value
        // we just initialised.
        let mut set: libc::cpu_set_t = unsafe { core::mem::zeroed() };
        unsafe { libc::CPU_ZERO(&mut set) };
        unsafe { libc::CPU_SET(self.cpu_id as usize, &mut set) };

        // SAFETY: `set` is properly initialised; `pid=0` means
        // current thread; `cpusetsize` matches the struct size.
        let rc =
            unsafe { libc::sched_setaffinity(0, core::mem::size_of::<libc::cpu_set_t>(), &set) };
        if rc != 0 {
            let errno = unsafe { *libc::__errno_location() };
            return Err(ReactorError::PinFailed(format!(
                "sched_setaffinity(cpu={}) failed (errno={errno})",
                self.cpu_id
            )));
        }

        // NUMA bind deferred to Phase 5 (libnuma / mbind).
        Ok(())
    }
}

impl Default for CpuPinning {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rejects_huge_cpu() {
        let p = CpuPinning::new(MAX_CPU_ID + 1);
        assert!(matches!(p.validate(), Err(ReactorError::InvalidConfig(_))));
    }

    #[test]
    fn validate_accepts_zero() -> Result<(), ReactorError> {
        CpuPinning::new(0).validate()?;
        Ok(())
    }

    #[test]
    fn with_numa_carries_node() {
        let p = CpuPinning::with_numa(2, 1);
        assert_eq!(p.cpu_id, 2);
        assert_eq!(p.numa_node, Some(1));
    }

    #[test]
    fn apply_pinned_to_cpu_zero_succeeds() -> Result<(), ReactorError> {
        // CPU 0 is always present on Linux; pinning self to it
        // succeeds (subject to cgroup cpuset restrictions).
        let p = CpuPinning::new(0);
        p.apply()?;
        Ok(())
    }
}

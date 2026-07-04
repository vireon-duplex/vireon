//! Kernel capability detection for io_uring features.
//!
//! Different kernel versions expose different io_uring capabilities.
//! [`IoFeatures`] probes the running kernel at ring-init time so the
//! reactor can choose the best available code path without hard-coding
//! `uname` checks at every call site.
//!
//! # Detection strategy
//!
//! 1. **Primary**: parse `uname(2)` release string (e.g. `"6.8.0-…"`).
//!    Reliable on every Linux kernel; does not depend on crate internals.
//! 2. **Secondary** (future): read `io_uring_params.features` bitmask
//!    returned by `io_uring_setup(2)` for `IORING_FEAT_*` flags.
//!
//! # Minimum kernel requirements
//!
//! | Feature | First kernel |
//! |---------|-------------|
//! | `MULTISHOT_RECV`    | 6.0  |
//! | `SEND_ZC`           | 6.0  |
//! | `BUF_RING`          | 5.19 |
//! | `FIXED_FILE_SPARSE` | 5.19 |
//! | `MULTISHOT_ACCEPT`  | 5.19 |
//! | `SQPOLL`            | 5.11 |

use io_uring::IoUring;

/// Bitmask of detected kernel io_uring features.
///
/// Constructed via [`IoFeatures::detect`]; query with the `supports_*`
/// helpers or [`IoFeatures::contains`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IoFeatures(u32);

impl IoFeatures {
    /// `IORING_OP_RECVMSG_MULTISHOT` — one SQE → many CQEs (kernel ≥ 6.0).
    pub const MULTISHOT_RECV: Self = Self(1 << 0);

    /// `IORING_OP_SEND_ZC` — zero-copy send with two-CQE lifecycle (kernel ≥ 6.0).
    pub const SEND_ZC: Self = Self(1 << 1);

    /// `IORING_REGISTER_PBUF_RING` — provided buffer rings (kernel ≥ 5.19).
    pub const BUF_RING: Self = Self(1 << 2);

    /// `IORING_REGISTER_FILES_SPARSE` — lazy-alloc file slots (kernel ≥ 5.19).
    pub const FIXED_FILE_SPARSE: Self = Self(1 << 3);

    /// `IORING_ACCEPT_MULTISHOT` — multishot accept flag (kernel ≥ 5.19).
    pub const MULTISHOT_ACCEPT: Self = Self(1 << 4);

    /// `IORING_SETUP_SQPOLL` — kernel-side SQ poll thread (kernel ≥ 5.11).
    pub const SQPOLL: Self = Self(1 << 5);

    /// Returns an empty feature set (no features detected).
    #[must_use]
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Returns the raw bitmask value.
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }

    /// Returns `true` if `self` contains all bits in `other`.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Inserts `other` into `self`.
    pub fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }

    /// Returns `true` if no features are set.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Returns the union of `self` and `other` (for `a | b` syntax).
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Detects io_uring features from the running kernel.
    ///
    /// `ring` is accepted for future use (reading `io_uring_params.features`
    /// directly); the current implementation uses `uname(2)` version parsing.
    #[must_use]
    pub fn detect(_ring: &IoUring) -> Self {
        let (major, minor) = kernel_version();
        let mut f = Self::empty();

        // IORING_OP_RECVMSG_MULTISHOT + IORING_OP_SEND_ZC: kernel ≥ 6.0
        if kernel_ge(major, minor, 6, 0) {
            f.insert(Self::MULTISHOT_RECV);
            f.insert(Self::SEND_ZC);
        }

        // PBUF_RING + FILES_SPARSE + ACCEPT_MULTISHOT: kernel ≥ 5.19
        if kernel_ge(major, minor, 5, 19) {
            f.insert(Self::BUF_RING);
            f.insert(Self::FIXED_FILE_SPARSE);
            f.insert(Self::MULTISHOT_ACCEPT);
        }

        // SQPOLL: kernel ≥ 5.11 (still requires CAP_SYS_NICE / root)
        if kernel_ge(major, minor, 5, 11) {
            f.insert(Self::SQPOLL);
        }

        f
    }

    // ── Query helpers ──────────────────────────────────────────────

    /// `RECVMSG_MULTISHOT` is available.
    #[must_use]
    pub fn supports_multishot_recv(self) -> bool {
        self.contains(Self::MULTISHOT_RECV)
    }

    /// `SEND_ZC` is available.
    #[must_use]
    pub fn supports_send_zc(self) -> bool {
        self.contains(Self::SEND_ZC)
    }

    /// `REGISTER_PBUF_RING` is available.
    #[must_use]
    pub fn supports_buf_ring(self) -> bool {
        self.contains(Self::BUF_RING)
    }

    /// Sparse file registration is available.
    #[must_use]
    pub fn supports_fixed_file_sparse(self) -> bool {
        self.contains(Self::FIXED_FILE_SPARSE)
    }

    /// `ACCEPT_MULTISHOT` is available.
    #[must_use]
    pub fn supports_multishot_accept(self) -> bool {
        self.contains(Self::MULTISHOT_ACCEPT)
    }

    /// `SQPOLL` is available (does not imply the caller has privileges).
    #[must_use]
    pub fn supports_sqpoll(self) -> bool {
        self.contains(Self::SQPOLL)
    }
}

impl std::ops::BitOr for IoFeatures {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for IoFeatures {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl std::fmt::Display for IoFeatures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        let emit =
            |f: &mut std::fmt::Formatter<'_>, name: &str, first: &mut bool| -> std::fmt::Result {
                if !*first {
                    f.write_str(" | ")?;
                }
                *first = false;
                f.write_str(name)
            };
        if self.contains(Self::MULTISHOT_RECV) {
            emit(f, "MULTISHOT_RECV", &mut first)?;
        }
        if self.contains(Self::SEND_ZC) {
            emit(f, "SEND_ZC", &mut first)?;
        }
        if self.contains(Self::BUF_RING) {
            emit(f, "BUF_RING", &mut first)?;
        }
        if self.contains(Self::FIXED_FILE_SPARSE) {
            emit(f, "FIXED_FILE_SPARSE", &mut first)?;
        }
        if self.contains(Self::MULTISHOT_ACCEPT) {
            emit(f, "MULTISHOT_ACCEPT", &mut first)?;
        }
        if self.contains(Self::SQPOLL) {
            emit(f, "SQPOLL", &mut first)?;
        }
        if first {
            f.write_str("(empty)")?;
        }
        Ok(())
    }
}

// ─── Kernel version parsing ────────────────────────────────────────

/// Returns `(major, minor)` from `uname(2)`. Returns `(0, 0)` on failure.
fn kernel_version() -> (u32, u32) {
    // SAFETY: `utsname` is a plain struct of fixed-size char arrays.
    // `uname(2)` fills it atomically.
    let mut uts: libc::utsname = unsafe { std::mem::zeroed() };
    if unsafe { libc::uname(&mut uts) } != 0 {
        return (0, 0);
    }
    let release: Vec<u8> = uts
        .release
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    let s = String::from_utf8_lossy(&release);
    let mut parts = s.split('.');
    let major = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    (major, minor)
}

/// Returns `true` if the kernel version is ≥ `(req_major, req_minor)`.
const fn kernel_ge(major: u32, minor: u32, req_major: u32, req_minor: u32) -> bool {
    major > req_major || (major == req_major && minor >= req_minor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_returns_nonempty_on_modern_kernel() {
        // The development host runs ≥ 5.19 (required for PBUF_RING).
        let ring = IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_coop_taskrun()
            .build(4)
            .expect("ring");
        let features = IoFeatures::detect(&ring);
        assert!(
            features.supports_buf_ring(),
            "expected BUF_RING support on this kernel"
        );
    }

    #[test]
    fn contains_and_insert_round_trip() {
        let mut f = IoFeatures::empty();
        assert!(!f.contains(IoFeatures::SEND_ZC));
        f.insert(IoFeatures::SEND_ZC | IoFeatures::MULTISHOT_RECV);
        assert!(f.contains(IoFeatures::SEND_ZC));
        assert!(f.contains(IoFeatures::MULTISHOT_RECV));
        assert!(!f.contains(IoFeatures::SQPOLL));
    }

    #[test]
    fn display_lists_active_features() {
        let f = IoFeatures::SEND_ZC | IoFeatures::BUF_RING;
        let s = f.to_string();
        assert!(s.contains("SEND_ZC"));
        assert!(s.contains("BUF_RING"));
    }

    #[test]
    fn empty_display_shows_empty() {
        assert_eq!(IoFeatures::empty().to_string(), "(empty)");
    }

    #[test]
    fn kernel_ge_compares_correctly() {
        assert!(kernel_ge(6, 8, 6, 0));
        assert!(kernel_ge(6, 0, 6, 0));
        assert!(kernel_ge(7, 0, 6, 0));
        assert!(!kernel_ge(5, 19, 6, 0));
        assert!(!kernel_ge(6, 0, 6, 1));
    }
}

//! Per-task metadata (Layer 6).
//!
//! [`TaskMetadata`] holds the cold fields that the executor and
//! scheduler inspect on enqueue / dequeue but not on every cache line
//! of the hot execute path.

/// Per-task metadata stored alongside the task descriptor.
#[derive(Clone, Copy, Debug, Default)]
pub struct TaskMetadata {
    /// Absolute deadline (monotonic nanos). `0` means no deadline.
    pub deadline_ns: u64,
    /// Spawn timestamp (monotonic nanos) — for queue-wait metric.
    pub spawned_at_ns: u64,
    /// Bitset of flags (cancellation requested, retry requested, etc.).
    pub flags: TaskFlags,
}

impl TaskMetadata {
    /// Creates metadata for a task with no deadline, spawn time `now_ns`.
    #[inline]
    #[must_use]
    pub const fn new(now_ns: u64) -> Self {
        Self {
            deadline_ns: 0,
            spawned_at_ns: now_ns,
            flags: TaskFlags::empty(),
        }
    }

    /// Sets the absolute deadline and returns `self` (builder-style).
    #[inline]
    #[must_use]
    pub const fn with_deadline(mut self, deadline_ns: u64) -> Self {
        self.deadline_ns = deadline_ns;
        self
    }

    /// Returns `true` if the deadline has passed at time `now_ns`.
    /// Tasks with `deadline_ns == 0` never expire.
    #[inline]
    #[must_use]
    pub fn is_expired(self, now_ns: u64) -> bool {
        self.deadline_ns != 0 && now_ns >= self.deadline_ns
    }

    /// Queue wait time in nanoseconds (now minus spawn).
    #[inline]
    #[must_use]
    pub fn wait_ns(self, now_ns: u64) -> u64 {
        now_ns.saturating_sub(self.spawned_at_ns)
    }
}

/// Bitset of per-task flags.
///
/// Manual implementation rather than the `bitflags` crate — the
/// dependency is small but unnecessary for two flags, and a manual
/// impl keeps the crate's dependency surface tight.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TaskFlags(u8);

impl TaskFlags {
    /// Cancellation has been requested via [`TaskHandle::cancel`].
    pub const CANCEL_REQUESTED: Self = Self(1 << 0);

    /// Task requested re-scheduling and will run again.
    pub const RETRY_REQUESTED: Self = Self(1 << 1);

    /// Empty flagset (no flags set).
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Returns `true` if no flags are set.
    #[inline]
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Returns `true` if `self` contains all bits in `other`.
    #[inline]
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Inserts the bits of `other` into `self`.
    #[inline]
    pub fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }

    /// Removes the bits of `other` from `self`.
    #[inline]
    pub fn remove(&mut self, other: Self) {
        self.0 &= !other.0;
    }

    /// Returns the union of `self` and `other`.
    #[inline]
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Raw bits accessor (for tests and serialization).
    #[inline]
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }
}

impl std::ops::BitOr for TaskFlags {
    type Output = Self;
    #[inline]
    fn bitor(self, rhs: Self) -> Self {
        self.union(rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_expiry() {
        let m = TaskMetadata::new(100).with_deadline(500);
        assert!(!m.is_expired(400));
        assert!(m.is_expired(500));
        assert!(m.is_expired(600));
    }

    #[test]
    fn metadata_no_deadline_never_expires() {
        let m = TaskMetadata::new(100);
        assert!(!m.is_expired(u64::MAX));
    }

    #[test]
    fn wait_time() {
        let m = TaskMetadata::new(1_000);
        assert_eq!(m.wait_ns(1_500), 500);
        assert_eq!(m.wait_ns(500), 0); // saturating
    }

    #[test]
    fn flags_set_clear() {
        let mut f = TaskFlags::empty();
        assert!(f.is_empty());
        f.insert(TaskFlags::CANCEL_REQUESTED);
        assert!(f.contains(TaskFlags::CANCEL_REQUESTED));
        assert!(!f.contains(TaskFlags::RETRY_REQUESTED));
        f.remove(TaskFlags::CANCEL_REQUESTED);
        assert!(!f.contains(TaskFlags::CANCEL_REQUESTED));
    }

    #[test]
    fn flags_union_bitor() {
        let f = TaskFlags::CANCEL_REQUESTED | TaskFlags::RETRY_REQUESTED;
        assert!(f.contains(TaskFlags::CANCEL_REQUESTED));
        assert!(f.contains(TaskFlags::RETRY_REQUESTED));
    }
}

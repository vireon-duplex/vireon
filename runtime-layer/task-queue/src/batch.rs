//! TaskBatch — reusable scratch buffer for batch collection (Layer 17).

/// Scratch buffer used by the scheduler/executor to collect a batch
/// of slot indices for one tick's execution.
///
/// Owned by the [`TaskExecutor`](crate::executor::TaskExecutor) and
/// reused every tick — zero allocation on the steady-state path.
pub struct TaskBatch {
    buf: Vec<u32>,
    len: usize,
}

impl TaskBatch {
    /// Creates a scratch buffer sized for `capacity` slot indices.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: vec![0; capacity.max(1)],
            len: 0,
        }
    }

    /// Resets the batch to empty (does not deallocate).
    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Pushes a slot index. Returns `false` if the buffer is full.
    #[inline]
    pub fn push(&mut self, slot_idx: u32) -> bool {
        if self.len >= self.buf.len() {
            return false;
        }
        self.buf[self.len] = slot_idx;
        self.len += 1;
        true
    }

    /// Returns the collected slot indices as a slice.
    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u32] {
        &self.buf[..self.len]
    }

    /// Number of slot indices currently in the batch.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the batch is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Capacity of the scratch buffer.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }
}

impl std::fmt::Debug for TaskBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskBatch")
            .field("len", &self.len)
            .field("capacity", &self.buf.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_and_clear() {
        let mut b = TaskBatch::new(4);
        assert!(b.is_empty());
        assert!(b.push(10));
        assert!(b.push(20));
        assert_eq!(b.len(), 2);
        assert_eq!(b.as_slice(), &[10, 20]);
        b.clear();
        assert!(b.is_empty());
    }

    #[test]
    fn overflow_returns_false() {
        let mut b = TaskBatch::new(2);
        assert!(b.push(1));
        assert!(b.push(2));
        assert!(!b.push(3));
        assert_eq!(b.len(), 2);
    }
}

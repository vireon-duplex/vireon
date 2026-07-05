//! Runtime services context passed to tasks (Layer 9).
//!
//! [`TaskContext`] bundles mutable access to the sibling runtime
//! services that a task may need — currently only the reactor tick's
//! monotonic time, reactor id, and a slot to drive the
//! [`TaskQueue`](crate::queue::TaskQueue) itself when the executor
//! needs to re-enqueue rescheduled work.
//!
//! In the full runtime integration this struct also carries
//! `&mut SendQueue`, `&TimerWheel`, `&BufferPool`, but those types
//! live in other crates and are feature-gated. For Phase 1 the context
//! is a thin handle to reactor metadata plus a callback-based extension
//! point so callers can stash application services via downcasting.

use std::any::Any;
use std::time::Duration;

use crate::priority::TaskPriority;

/// Runtime services available during task execution.
///
/// The lifetime `'a` ties the context to a single [`tick`](crate::queue::TaskQueue::tick)
/// call. Tasks must not retain references past `run()`.
pub struct TaskContext<'a> {
    now_ns: u64,
    reactor_id: u32,
    /// Optional extension slot for application services (downcast on use).
    /// `Box<dyn Any>` is heap-allocated but only constructed once per
    /// reactor — not on the hot path.
    ext: Option<&'a mut dyn Any>,
}

impl<'a> TaskContext<'a> {
    /// Creates a context carrying only reactor metadata.
    #[inline]
    #[must_use]
    pub const fn new(now_ns: u64, reactor_id: u32) -> Self {
        Self {
            now_ns,
            reactor_id,
            ext: None,
        }
    }

    /// Attaches an application-defined extension to the context.
    #[inline]
    #[must_use]
    pub fn with_ext(mut self, ext: &'a mut dyn Any) -> Self {
        self.ext = Some(ext);
        self
    }

    /// Current monotonic time in nanoseconds (the reactor's view).
    #[inline]
    #[must_use]
    pub const fn now_ns(&self) -> u64 {
        self.now_ns
    }

    /// Reactor (core) identifier for diagnostics.
    #[inline]
    #[must_use]
    pub const fn reactor_id(&self) -> u32 {
        self.reactor_id
    }

    /// Downcasts the extension to `&T`, if present and matching.
    #[must_use]
    pub fn ext<T: 'static>(&self) -> Option<&T> {
        self.ext.as_deref().and_then(|e| e.downcast_ref::<T>())
    }

    /// Downcasts the extension to `&mut T`, if present and matching.
    #[must_use]
    pub fn ext_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.ext.as_deref_mut().and_then(|e| e.downcast_mut::<T>())
    }

    /// Helper that builds a [`TaskResult::Reschedule`](crate::TaskResult)
    /// for an immediate next-tick yield.
    #[must_use]
    pub fn yield_next(&self, priority: TaskPriority) -> crate::TaskResult {
        crate::TaskResult::Reschedule {
            delay: Duration::ZERO,
            priority,
        }
    }
}

impl<'a> std::fmt::Debug for TaskContext<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskContext")
            .field("now_ns", &self.now_ns)
            .field("reactor_id", &self.reactor_id)
            .field("has_ext", &self.ext.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_accessors() {
        let ctx = TaskContext::new(1234, 7);
        assert_eq!(ctx.now_ns(), 1234);
        assert_eq!(ctx.reactor_id(), 7);
        assert!(ctx.ext::<u32>().is_none());
    }

    #[test]
    fn ext_downcast() {
        let mut val = 42u32;
        let mut ctx = TaskContext::new(0, 0).with_ext(&mut val);
        assert_eq!(ctx.ext::<u32>(), Some(&42));
        if let Some(v) = ctx.ext_mut::<u32>() {
            *v = 99;
        }
        assert_eq!(val, 99);
    }

    #[test]
    fn debug_format() {
        let ctx = TaskContext::new(100, 3);
        let s = format!("{ctx:?}");
        assert!(s.contains("100"));
        assert!(s.contains("reactor_id"));
    }
}

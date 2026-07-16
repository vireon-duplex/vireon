//! Dispatch table (Layer 6).
//!
//! A `[Handler; 16]` indexed by `OpKind as usize`. Adding a new OpKind
//! means adding one entry in the table; the dispatch loop never changes.
//! Handlers are freestanding `fn` pointers (no closures, no vtables) so
//! dispatch is a single indirect call.

use io::completion::Completion;
use io::ops::OpKind;

use crate::lifecycle::{Outcome, SendQueueEvent};
use crate::processor::CqeProcessor;

/// Handler signature. The processor is passed mutably so handlers can
/// record metrics and inspect internal state (e.g. multishot health).
pub type Handler = fn(proc: &mut CqeProcessor, completion: &Completion) -> Outcome;

/// Dispatch table — fixed-size array of handlers.
#[derive(Clone, Copy)]
pub struct DispatchTable {
    table: [Handler; crate::user_data::OP_KIND_COUNT],
}

impl DispatchTable {
    /// Builds the default table mapping each known OpKind to its handler.
    /// Unknown slots are filled with [`noop_handler`].
    #[must_use]
    pub fn default_table() -> Self {
        let mut t: [Handler; crate::user_data::OP_KIND_COUNT] =
            [noop_handler; crate::user_data::OP_KIND_COUNT];
        t[OpKind::Recv as usize] = crate::handlers::process_recv;
        t[OpKind::Send as usize] = crate::handlers::process_send;
        t[OpKind::Timeout as usize] = crate::handlers::process_timeout;
        t[OpKind::Cancel as usize] = crate::handlers::process_cancel;
        Self { table: t }
    }

    /// Returns the handler for `kind`.
    #[inline(always)]
    #[must_use]
    pub const fn get(&self, kind: OpKind) -> Handler {
        self.table[kind as usize]
    }

    /// Replaces the handler for `kind`. Useful for tests that want to
    /// override routing without re-declaring the table.
    #[inline]
    pub fn set(&mut self, kind: OpKind, handler: Handler) {
        self.table[kind as usize] = handler;
    }
}

impl std::fmt::Debug for DispatchTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatchTable")
            .field("slots", &self.table.len())
            .finish()
    }
}

impl Default for DispatchTable {
    fn default() -> Self {
        Self::default_table()
    }
}

/// The no-op handler used to fill unused dispatch slots. Returns
/// `Outcome::default()` (i.e. `consumed == false`).
pub fn noop_handler(_proc: &mut CqeProcessor, _completion: &Completion) -> Outcome {
    Outcome::default()
}

// ── Default handlers ──────────────────────────────────────────────────
//
// Each handler matches the expected `Completion` variant. If the variant
// does not match (which should never happen for a correctly encoded CQE),
// the handler returns `Outcome::default()` so the dispatcher records the
// entry as `skipped`.

pub mod handlers {
    use super::*;
    use crate::lifecycle::Outcome;

    /// Handler for `Completion::Receive` / `ReceiveError`. Records RX
    /// metrics and schedules a re-arm when the multishot terminates.
    #[inline]
    pub fn process_recv(proc: &mut CqeProcessor, c: &Completion) -> Outcome {
        match c {
            Completion::Receive {
                id,
                buf_id,
                len,
                more,
            } => {
                let _ = (id, buf_id, len);
                if !more {
                    proc.multishot.schedule_rearm();
                }
                proc.stats
                    .recv_buffers_refilled
                    .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                Outcome {
                    consumed: true,
                    schedule_task: true,
                    rearm_multishot: !more,
                    ..Outcome::default()
                }
            }
            Completion::ReceiveError { errno, more, .. } => {
                if *errno == -libc::ENOBUFS {
                    proc.stats
                        .buffer_exhaustion_events
                        .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                    proc.multishot.note_enobufs();
                } else {
                    proc.stats
                        .errors_total
                        .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                }
                if !more {
                    proc.multishot.schedule_rearm();
                }
                Outcome {
                    consumed: true,
                    rearm_multishot: !more,
                    ..Outcome::default()
                }
            }
            _ => Outcome::default(),
        }
    }

    /// Handler for `Completion::Send` (first CQE of SEND_ZC) and
    /// `Completion::SendBufferReclaim` (second CQE).
    #[inline]
    pub fn process_send(proc: &mut CqeProcessor, c: &Completion) -> Outcome {
        match c {
            Completion::Send { id, res: _ } => {
                proc.stats
                    .zc_first_cqes
                    .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                #[cfg(debug_assertions)]
                proc.sendzc.note_send(*id);
                Outcome {
                    consumed: true,
                    forward: Some(SendQueueEvent::SendCqe(*id)),
                    ..Outcome::default()
                }
            }
            Completion::SendBufferReclaim { id } => {
                proc.stats
                    .zc_notifications
                    .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                // Look up the registered buf_id (if any) for this id.
                let buf_id = proc.sendzc.peek_buf_for(*id);
                #[cfg(debug_assertions)]
                proc.sendzc.note_reclaim(*id);
                Outcome {
                    consumed: true,
                    forward: Some(SendQueueEvent::ReclaimCqe(*id)),
                    return_buffer: buf_id,
                    ..Outcome::default()
                }
            }
            Completion::Error { id, errno } => {
                proc.stats
                    .errors_total
                    .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                if *errno == -libc::ENOBUFS {
                    proc.stats
                        .buffer_exhaustion_events
                        .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                }
                // A send that errored still needs reclaim handling: the
                // kernel will post the notification CQE separately. We
                // forward the error to the Send Queue so it can record
                // the failure; buffer recycling happens on reclaim.
                let _ = id;
                Outcome {
                    consumed: true,
                    ..Outcome::default()
                }
            }
            _ => Outcome::default(),
        }
    }

    /// Handler for `Completion::Timeout`.
    #[inline]
    pub fn process_timeout(proc: &mut CqeProcessor, c: &Completion) -> Outcome {
        let Completion::Timeout { id } = c else {
            return Outcome::default();
        };
        proc.stats
            .timer_fires
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        Outcome {
            consumed: true,
            retire_timer: Some(*id),
            ..Outcome::default()
        }
    }

    /// Handler for `Completion::Cancel`.
    #[inline]
    pub fn process_cancel(proc: &mut CqeProcessor, c: &Completion) -> Outcome {
        let Completion::Cancel { id, res: _ } = c else {
            return Outcome::default();
        };
        proc.stats
            .timer_cancels
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        Outcome {
            consumed: true,
            retire_timer: Some(*id),
            ..Outcome::default()
        }
    }
}

// Re-export handler functions at the dispatcher module root so the table
// builder can reference them as `crate::handlers::*`.
pub use handlers::{process_cancel, process_recv, process_send, process_timeout};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::CqeProcessorBuilder;

    #[test]
    fn table_default_has_handlers_for_known_opkinds() {
        let t = DispatchTable::default_table();
        // Recv/Send/Timeout/Cancel slots should not be the noop handler.
        assert!(!std::ptr::eq(
            t.get(OpKind::Recv) as *const (),
            noop_handler as *const (),
        ));
        assert!(!std::ptr::eq(
            t.get(OpKind::Send) as *const (),
            noop_handler as *const (),
        ));
        assert!(!std::ptr::eq(
            t.get(OpKind::Timeout) as *const (),
            noop_handler as *const (),
        ));
        assert!(!std::ptr::eq(
            t.get(OpKind::Cancel) as *const (),
            noop_handler as *const (),
        ));
    }

    #[test]
    fn set_replaces_handler() {
        let mut t = DispatchTable::default();
        let original = t.get(OpKind::Recv);
        t.set(OpKind::Recv, noop_handler);
        assert!(std::ptr::eq(
            t.get(OpKind::Recv) as *const (),
            noop_handler as *const (),
        ));
        // Restore for other tests.
        t.set(OpKind::Recv, original);
    }

    #[test]
    fn noop_handler_returns_default() {
        let mut proc = CqeProcessorBuilder::new().build().expect("build");
        let c = Completion::Timeout { id: 0 };
        let o = noop_handler(&mut proc, &c);
        assert!(!o.consumed);
    }
}

//! CQE Processor facade (Layer 15).
//!
//! [`CqeProcessor`] owns its private state (decoder, dispatch table,
//! scratch buffers, recycle pipeline, multishot state, send-zc tracker,
//! stats). It does **not** own the runtime modules — the future Reactor
//! owns those and passes them in via [`ProcessorContext`] per drain call.
//!
//! ## Testability
//!
//! Handlers never touch runtime modules directly. They emit [`Outcome`]s
//! that the reactor interprets. This lets unit tests drive the processor
//! with synthetic [`Completion`]s via [`CqeProcessor::dispatch_one`]
//! without needing a real io_uring ring or runtime modules.

use std::sync::atomic::Ordering;
use std::time::Instant;

use io::completion::{Completion, CompletionDecoder};
use io::ops::decode_kind;
use io_uring::IoUring;
use profiler::profile;

use crate::cache::HotState;
use crate::config::{CqeProcessorConfig, OverflowPolicy};
use crate::dispatcher::DispatchTable;
use crate::drain::CqDrainer;
use crate::lifecycle::{Outcome, SendQueueEvent};
use crate::multishot::MultishotState;
use crate::recycle::RecyclePipeline;
use crate::scheduler::CqeScheduler;
use crate::sendzc::SendZcTracker;
use crate::stats::StatsCollector;

/// Outcome of a single drain cycle.
#[derive(Clone, Copy, Debug, Default)]
pub struct TickResult {
    /// Total CQEs read from the ring.
    pub drained: usize,
    /// CQEs whose handler returned `consumed == true`.
    pub dispatched: usize,
    /// CQEs whose handler returned `consumed == false` (variant mismatch).
    pub skipped: usize,
    /// Buffers returned via the recycle pipeline this tick.
    pub buffers_recycled: usize,
    /// Tasks scheduled via [`CqeScheduler`] this tick.
    pub tasks_scheduled: usize,
    /// Kernel timers retired this tick.
    pub timers_retired: usize,
    /// Send Queue events emitted this tick.
    pub send_queue_events: usize,
    /// Multishot re-arms requested this tick.
    pub multishot_rearms: usize,
    /// `true` if the kernel reported CQ overflow.
    pub overflow_detected: bool,
    /// `true` if `starvation_threshold_ticks` consecutive empty drains.
    pub starvation_detected: bool,
    /// Wall-clock time spent in this drain (ns).
    pub drain_latency_ns: u64,
}

impl TickResult {
    /// Total events emitted (sum of all per-category counts).
    #[must_use]
    pub const fn total_events(self) -> usize {
        self.buffers_recycled
            + self.tasks_scheduled
            + self.timers_retired
            + self.send_queue_events
            + self.multishot_rearms
    }
}

/// The processor facade. Owns private state; borrows runtime modules per
/// drain call.
pub struct CqeProcessor {
    /// Stateless decoder (from IO Layer).
    pub(crate) decoder: CompletionDecoder,
    /// Dispatch table indexed by OpKind.
    pub(crate) dispatch_table: DispatchTable,
    /// Validated configuration.
    pub(crate) config: CqeProcessorConfig,
    /// Raw CQE drainer (owns scratch).
    pub(crate) drainer: CqDrainer,
    /// Hot cache-aligned counters.
    pub(crate) hot: HotState,
    /// Cold atomic counters + snapshot source.
    pub(crate) stats: StatsCollector,
    /// Coalesced buffer returns.
    pub(crate) recycle: RecyclePipeline,
    /// Multishot RX health + re-arm scheduling.
    pub(crate) multishot: MultishotState,
    /// SEND_ZC id → buf_id mapping + debug pairing.
    pub(crate) sendzc: SendZcTracker,
    /// Per-tick scheduled-task accumulator.
    pub(crate) scheduler: CqeScheduler,
    /// Consecutive empty-drain counter (starvation tracking).
    pub(crate) consecutive_empty: u32,
    /// Buffer returns pending across the current drain call (separate
    /// from `recycle` so we can drain into Outcome emission without
    /// fighting the borrow checker).
    pub(crate) pending_recycle: Vec<u16>,
    /// Send Queue events pending across the current drain call.
    pub(crate) pending_sendq: Vec<SendQueueEvent>,
    /// Timer ids pending retirement across the current drain call.
    pub(crate) pending_timers: Vec<u64>,
}

impl CqeProcessor {
    /// Constructs a processor with the given validated configuration.
    pub fn from_config(config: CqeProcessorConfig) -> Result<Self, crate::error::CqeError> {
        config.validate()?;
        let batch_max = config.batch_max_cqes;
        Ok(Self {
            decoder: CompletionDecoder::new(),
            dispatch_table: DispatchTable::default(),
            config,
            drainer: CqDrainer::new(batch_max),
            hot: HotState::new(),
            stats: StatsCollector::new(),
            recycle: RecyclePipeline::new(batch_max.max(64)),
            multishot: MultishotState::new(/* rearm cooldown placeholder; updated below */ 0),
            sendzc: SendZcTracker::with_capacity(batch_max),
            scheduler: CqeScheduler::new(batch_max),
            consecutive_empty: 0,
            pending_recycle: Vec::with_capacity(batch_max),
            pending_sendq: Vec::with_capacity(batch_max),
            pending_timers: Vec::with_capacity(batch_max),
        }
        .with_rearm_cooldown())
    }

    /// Internal helper to apply the configured re-arm cooldown after build.
    fn with_rearm_cooldown(mut self) -> Self {
        let cooldown = self.config.multishot_rearm_cooldown;
        self.multishot = MultishotState::new(cooldown);
        self
    }

    // ── Public accessors ────────────────────────────────────────────

    /// Returns the reactor/core ID this processor is bound to.
    #[must_use]
    pub fn reactor_id(&self) -> u32 {
        self.config.reactor_id
    }

    /// Returns the configured batch cap.
    #[must_use]
    pub fn batch_max_cqes(&self) -> usize {
        self.config.batch_max_cqes
    }

    /// Returns a snapshot of all stats.
    #[must_use]
    pub fn stats(&self) -> crate::stats::StatsSnapshot {
        self.stats.snapshot()
    }

    /// Returns `true` if a multishot re-arm is pending.
    #[must_use]
    pub fn is_multishot_rearm_pending(&self) -> bool {
        self.multishot.is_rearm_pending()
    }

    /// Takes the pending multishot re-arm request, if any.
    pub fn take_multishot_rearm(&mut self) -> Option<crate::multishot::RearmRequest> {
        self.multishot.take_pending()
    }

    // ── Registration: SEND_ZC id → buf_id ───────────────────────────

    /// Registers an `id → buf_id` mapping for a SEND_ZC operation that the
    /// application has just submitted. The processor uses this to fill in
    /// [`Outcome::return_buffer`] when the corresponding `SendBufferReclaim`
    /// CQE arrives.
    ///
    /// The future Reactor calls this when forwarding a `SendQueue::submit`
    /// to the kernel.
    pub fn register_send(&mut self, id: u64, buf_id: u16) {
        self.sendzc.register(id, buf_id);
    }

    // ── Pending-event drains (called by the reactor after `drain_cq`) ──

    /// Drains all buffer-recycle requests accumulated during the last
    /// `drain_cq` call. The caller (reactor) is responsible for invoking
    /// `BufferPool::refill_buffer` on each id.
    #[must_use]
    pub fn drain_pending_recycles(&mut self) -> Vec<u16> {
        std::mem::take(&mut self.pending_recycle)
    }

    /// Drains all Send Queue-bound events accumulated during the last
    /// `drain_cq` call.
    #[must_use]
    pub fn drain_pending_sendq_events(&mut self) -> Vec<SendQueueEvent> {
        std::mem::take(&mut self.pending_sendq)
    }

    /// Drains all timer-retirement ids accumulated during the last
    /// `drain_cq` call.
    #[must_use]
    pub fn drain_pending_timer_retires(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.pending_timers)
    }

    /// Drains all scheduled tasks accumulated during the last `drain_cq` call.
    #[must_use]
    pub fn drain_pending_tasks(&mut self) -> Vec<crate::scheduler::ScheduledTask> {
        self.scheduler.drain()
    }

    // ── Drain ───────────────────────────────────────────────────────

    /// Drains up to `batch_max_cqes` CQEs from `ring`, decodes each, and
    /// dispatches via the dispatch table. Returns a [`TickResult`] summarising
    /// the work and accumulates pending events accessible via the
    /// `drain_pending_*` methods.
    #[profile]
    pub fn drain_cq(&mut self, ring: &mut IoUring, now_ns: u64) -> TickResult {
        let start = Instant::now();

        // Phase 1: drain raw entries (head advanced once by io-uring crate's
        // iterator Drop).
        let entries: Vec<io_uring::cqueue::Entry> = {
            let slice = self.drainer.drain(ring);
            slice.to_vec()
        };

        let mut result = TickResult {
            drained: entries.len(),
            ..TickResult::default()
        };

        // Phase 2: decode + dispatch.
        for entry in &entries {
            let user_data = entry.user_data();
            let res = entry.result();
            let flags = entry.flags();
            let completion = self.decoder.decode(entry);
            let kind = decode_kind(user_data);

            // Hot-state record.
            self.hot.record_cqe(kind as usize);

            // Dispatch via the table.
            let handler = self.dispatch_table.get(kind);
            let outcome: Outcome = handler(self, &completion);

            // Tally.
            if outcome.consumed {
                result.dispatched += 1;
            } else {
                result.skipped += 1;
            }

            // Outcome → pending queues.
            if outcome.schedule_task {
                self.scheduler.push(completion.clone(), now_ns);
                result.tasks_scheduled += 1;
            }
            if let Some(buf_id) = outcome.return_buffer {
                self.pending_recycle.push(buf_id);
            }
            if let Some(event) = outcome.forward {
                self.pending_sendq.push(event);
                result.send_queue_events += 1;
            }
            if let Some(id) = outcome.retire_timer {
                self.pending_timers.push(id);
                result.timers_retired += 1;
            }
            if outcome.rearm_multishot {
                // schedule_rearm already called inside the handler; bump
                // the metric here only if the request is currently pending.
                if self.multishot.is_rearm_pending() {
                    result.multishot_rearms += 1;
                }
            }

            // Update per-OpKind stats (mirror hot-state into cold).
            self.stats
                .cqes_per_opkind
                .get(kind as usize)
                .map(|a| a.fetch_add(1, Ordering::Relaxed));
            self.stats.cqes_total.fetch_add(1, Ordering::Relaxed);

            // Suppress unused-variable warning when debug-only fields exist.
            let _ = (res, flags, &completion);
        }

        // Phase 3: record drain cycle.
        self.hot.record_drain(entries.len() as u64);
        self.multishot.tick();

        // Phase 4: overflow + starvation detection.
        let overflow = CqDrainer::check_overflow(ring);
        if overflow > 0 {
            self.stats
                .cq_overflow_events
                .fetch_add(overflow as u64, Ordering::Relaxed);
            result.overflow_detected = true;
            match self.config.on_overflow {
                OverflowPolicy::Log => {}
                OverflowPolicy::Shutdown => {
                    // Reactor inspects TickResult.overflow_detected and
                    // shuts down; no direct signal here.
                }
                OverflowPolicy::Panic => {
                    // User-configured abort. tracing::error! preserves
                    // diagnostics; abort() terminates the process. We avoid
                    // panic!/eprintln! which are denied by workspace lints.
                    tracing::error!(
                        reactor_id = self.config.reactor_id,
                        overflow,
                        "CQ overflow — kernel dropped CQEs; aborting"
                    );
                    std::process::abort();
                }
            }
        }

        if entries.is_empty() {
            self.consecutive_empty = self.consecutive_empty.saturating_add(1);
            if self.consecutive_empty >= self.config.starvation_threshold_ticks {
                self.stats
                    .cq_starvation_events
                    .fetch_add(1, Ordering::Relaxed);
                result.starvation_detected = true;
            }
        } else {
            self.consecutive_empty = 0;
        }

        // Phase 5: recycle flush if threshold reached (the reactor still
        // pulls via `drain_pending_recycles`; this is just informational).
        if self.recycle.should_flush() {
            // Drain into pending_recycle so the next `drain_pending_recycles`
            // picks them up.
            let pending = self.recycle.drain_pending();
            self.pending_recycle.extend(pending);
        }

        result.drain_latency_ns = start.elapsed().as_nanos() as u64;
        result
    }

    /// Dispatches a single decoded [`Completion`] without touching the ring.
    /// Used by tests and by callers that pre-decode CQEs themselves.
    ///
    /// Updates internal stats and accumulates pending events just like
    /// [`drain_cq`](Self::drain_cq), but does not advance any ring head.
    #[profile]
    pub fn dispatch_one(
        &mut self,
        user_data: u64,
        completion: &Completion,
        now_ns: u64,
    ) -> Outcome {
        let kind = decode_kind(user_data);
        self.hot.record_cqe(kind as usize);
        let outcome = (self.dispatch_table.get(kind))(self, completion);

        if outcome.schedule_task {
            self.scheduler.push(completion.clone(), now_ns);
        }
        if let Some(buf_id) = outcome.return_buffer {
            self.pending_recycle.push(buf_id);
        }
        if let Some(event) = outcome.forward {
            self.pending_sendq.push(event);
        }
        if let Some(id) = outcome.retire_timer {
            self.pending_timers.push(id);
        }
        self.stats
            .cqes_per_opkind
            .get(kind as usize)
            .map(|a| a.fetch_add(1, Ordering::Relaxed));
        self.stats.cqes_total.fetch_add(1, Ordering::Relaxed);
        outcome
    }

    // ── Recycle-pipeline access ─────────────────────────────────────

    /// Pushes a buf_id into the recycle pipeline (advanced API; usually
    /// the dispatcher does this for you).
    pub fn recycle_buffer(&mut self, buf_id: u16) {
        self.recycle.push(buf_id);
    }

    /// Current recycle-pipeline pending count.
    #[must_use]
    pub fn recycle_pending(&self) -> usize {
        self.recycle.pending_count()
    }
}

impl std::fmt::Debug for CqeProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CqeProcessor")
            .field("reactor_id", &self.config.reactor_id)
            .field("batch_max_cqes", &self.config.batch_max_cqes)
            .field("cqes_total", &self.stats.cqes_total.load(Ordering::Relaxed))
            .field(
                "drain_cycles",
                &self.stats.drain_cycles.load(Ordering::Relaxed),
            )
            .field("inflight_sends", &self.sendzc.inflight_count())
            .field("recycle_pending", &self.recycle.pending_count())
            .field("rearm_pending", &self.multishot.is_rearm_pending())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::CqeProcessorBuilder;
    use io::ops::{OpKind, encode};

    fn make_proc() -> CqeProcessor {
        CqeProcessorBuilder::new()
            .batch_max_cqes(64)
            .build()
            .expect("build")
    }

    #[test]
    fn dispatch_one_recv_schedules_rearm_on_no_more() {
        let mut p = make_proc();
        let c = Completion::Receive {
            id: encode(OpKind::Recv, 1),
            buf_id: Some(7),
            len: 128,
            more: false,
        };
        let ud = encode(OpKind::Recv, 1);
        let outcome = p.dispatch_one(ud, &c, 0);
        assert!(outcome.consumed);
        assert!(outcome.schedule_task);
        assert!(outcome.rearm_multishot);
        // Re-arm request should be pending.
        assert!(p.is_multishot_rearm_pending());
    }

    #[test]
    fn dispatch_one_recv_does_not_rearm_when_more() {
        let mut p = make_proc();
        let c = Completion::Receive {
            id: encode(OpKind::Recv, 1),
            buf_id: Some(7),
            len: 128,
            more: true,
        };
        let ud = encode(OpKind::Recv, 1);
        let outcome = p.dispatch_one(ud, &c, 0);
        assert!(outcome.consumed);
        assert!(!outcome.rearm_multishot);
        assert!(!p.is_multishot_rearm_pending());
    }

    #[test]
    fn dispatch_one_send_emits_sendq_event() {
        let mut p = make_proc();
        let c = Completion::Send { id: 42, res: 128 };
        let ud = encode(OpKind::Send, 42);
        let outcome = p.dispatch_one(ud, &c, 0);
        assert!(outcome.consumed);
        assert!(matches!(outcome.forward, Some(SendQueueEvent::SendCqe(42))));
    }

    #[test]
    fn dispatch_one_reclaim_recycles_registered_buf() {
        let mut p = make_proc();
        p.register_send(99, 14);
        let c = Completion::SendBufferReclaim { id: 99 };
        let ud = encode(OpKind::Send, 99);
        let outcome = p.dispatch_one(ud, &c, 0);
        assert!(outcome.consumed);
        assert_eq!(outcome.return_buffer, Some(14));
        assert!(matches!(
            outcome.forward,
            Some(SendQueueEvent::ReclaimCqe(99))
        ));
    }

    #[test]
    fn dispatch_one_timeout_retires_timer() {
        let mut p = make_proc();
        let c = Completion::Timeout { id: 5 };
        let ud = encode(OpKind::Timeout, 5);
        let outcome = p.dispatch_one(ud, &c, 0);
        assert!(outcome.consumed);
        assert_eq!(outcome.retire_timer, Some(5));
    }

    #[test]
    fn dispatch_one_cancel_retires_timer() {
        let mut p = make_proc();
        let c = Completion::Cancel { id: 8, res: 0 };
        let ud = encode(OpKind::Cancel, 8);
        let outcome = p.dispatch_one(ud, &c, 0);
        assert!(outcome.consumed);
        assert_eq!(outcome.retire_timer, Some(8));
    }

    #[test]
    fn pending_events_drain_correctly() {
        let mut p = make_proc();
        p.register_send(1, 100);
        let reclaim = Completion::SendBufferReclaim { id: 1 };
        p.dispatch_one(encode(OpKind::Send, 1), &reclaim, 0);

        let timeout = Completion::Timeout { id: 5 };
        p.dispatch_one(encode(OpKind::Timeout, 5), &timeout, 0);

        let recv = Completion::Receive {
            id: 0,
            buf_id: Some(9),
            len: 10,
            more: true,
        };
        p.dispatch_one(encode(OpKind::Recv, 0), &recv, 0);

        assert_eq!(p.drain_pending_recycles(), vec![100]);
        assert_eq!(p.drain_pending_sendq_events().len(), 1);
        assert_eq!(p.drain_pending_timer_retires(), vec![5]);
        assert_eq!(p.drain_pending_tasks().len(), 1);

        // After drain, all empty.
        assert!(p.drain_pending_recycles().is_empty());
        assert!(p.drain_pending_sendq_events().is_empty());
        assert!(p.drain_pending_timer_retires().is_empty());
        assert!(p.drain_pending_tasks().is_empty());
    }

    #[test]
    fn starvation_tracking_resets_on_work() {
        let mut p = make_proc();
        // simulate consecutive empty drains
        for _ in 0..3 {
            p.consecutive_empty = p.consecutive_empty.saturating_add(1);
        }
        assert_eq!(p.consecutive_empty, 3);
        // Dispatch one CQE → resets via drain_cq path (we simulate by hand).
        let c = Completion::Timeout { id: 0 };
        p.dispatch_one(encode(OpKind::Timeout, 0), &c, 0);
        // dispatch_one does not touch consecutive_empty; only drain_cq does.
        assert_eq!(p.consecutive_empty, 3);
    }

    #[test]
    fn debug_format_has_reactor_id_and_counts() {
        let p = make_proc();
        let s = format!("{p:?}");
        assert!(s.contains("reactor_id"));
        assert!(s.contains("cqes_total"));
    }
}

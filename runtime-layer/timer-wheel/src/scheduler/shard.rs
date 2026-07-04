//! Per-core share-nothing shard (Layer 8).
//!
//! A [`Shard`] is the unit of work for one worker thread. It owns its slab,
//! hierarchical wheel, command queue, expired-out queue, and the cancellation
//! set. Only the owning worker touches the slab and wheel; the queues are
//! lock-free and shared with the facade.

use std::collections::HashSet;
use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::concurrent::mpsc::BoundedMpsc;
use crate::core::allocator::SlabAllocator;
use crate::core::timer_entry::TimerEntry;
use crate::core::wheel::HierarchicalWheel;
use crate::error::ShardId;
use crate::scheduler::clock::Clock;

/// Application-supplied timer identifier.
pub type TaskId = u64;

/// Optional inline hook invoked on the worker thread when a timer fires.
///
/// Implementations must be non-blocking and `Send + Sync`. Fired `task_id`s
/// are always also routed to the drain queue regardless of whether a handler
/// is registered.
pub trait TimerHandler: Send + Sync {
    /// Called for each fired timer, on the owning worker thread.
    fn on_fire(&self, task_id: TaskId);
}

/// A command pushed by the facade and drained by the worker.
#[derive(Debug)]
pub enum TimerCmd {
    /// Schedule `task_id` to fire at absolute `deadline_ticks`.
    Start {
        /// Caller-owned identifier returned via `drain_expired`.
        task_id: TaskId,
        /// Absolute expiry tick (derived from `Clock::now_ticks + duration`).
        deadline_ticks: u64,
    },
    /// Best-effort cancel; honoured even if the `Start` has not been processed.
    Cancel {
        /// Identifier to suppress at fire time.
        task_id: TaskId,
    },
}

/// Cross-thread handles into one shard (held by the facade).
///
/// Cloning these `Arc`s is cheap; the worker keeps its own clones inside the
/// [`Shard`] it owns.
#[derive(Clone)]
pub struct ShardChannels {
    /// Producer side of the command queue (facade pushes).
    pub cmd_tx: Arc<BoundedMpsc<TimerCmd>>,
    /// Consumer side of the expired queue (facade drains).
    pub expired_rx: Arc<BoundedMpsc<TaskId>>,
    /// Approximate count of outstanding timers (for synchronous capacity check).
    pub outstanding: Arc<AtomicU64>,
    /// Slab capacity (bound for `outstanding`).
    pub capacity: usize,
}

/// A share-nothing shard driven by a single worker.
pub struct Shard {
    shard_id: ShardId,
    slab: SlabAllocator<TimerEntry>,
    wheel: HierarchicalWheel,
    cmd_rx: Arc<BoundedMpsc<TimerCmd>>,
    expired_out: Arc<BoundedMpsc<TaskId>>,
    outstanding: Arc<AtomicU64>,
    clock: Arc<Clock>,
    /// Task ids cancelled before their fire time; drained at fire.
    cancelled: HashSet<TaskId>,
    /// Fired task ids that did not fit in `expired_out`; retried each tick.
    pending_expired: VecDeque<TaskId>,
    /// Scratch buffer reused by `advance`.
    reclaim_buf: Vec<u32>,
    /// Scratch buffer reused by command drain.
    cmd_buf: Vec<TimerCmd>,
    /// Optional inline hook called on each fire.
    handler: Option<Arc<dyn TimerHandler>>,
}

impl Shard {
    /// Builds a shard and its cross-thread channels.
    ///
    /// Returns `(Shard, ShardChannels)` so the worker can take ownership of
    /// the `Shard` while the facade keeps the `ShardChannels`.
    #[must_use]
    pub fn new(
        shard_id: ShardId,
        bits_per_level: u32,
        levels: u32,
        capacity: usize,
        cmd_capacity: usize,
        expired_capacity: usize,
        clock: Arc<Clock>,
    ) -> Option<(Self, ShardChannels)> {
        let slab = SlabAllocator::new(capacity)?;
        let wheel = HierarchicalWheel::new(bits_per_level, levels, clock.now_ticks());
        let cmd_rx = Arc::new(BoundedMpsc::new(cmd_capacity)?);
        let expired_out = Arc::new(BoundedMpsc::new(expired_capacity)?);
        let outstanding = Arc::new(AtomicU64::new(0));
        let channels = ShardChannels {
            cmd_tx: Arc::clone(&cmd_rx),
            expired_rx: Arc::clone(&expired_out),
            outstanding: Arc::clone(&outstanding),
            capacity,
        };
        let shard = Self {
            shard_id,
            slab,
            wheel,
            cmd_rx,
            expired_out,
            outstanding,
            clock,
            cancelled: HashSet::new(),
            pending_expired: VecDeque::new(),
            reclaim_buf: Vec::new(),
            cmd_buf: Vec::new(),
            handler: None,
        };
        Some((shard, channels))
    }

    /// This shard's id.
    #[inline]
    #[must_use]
    pub fn id(&self) -> ShardId {
        self.shard_id
    }

    /// Live timer count (approximate; updated atomically by start/fire).
    #[inline]
    #[must_use]
    pub fn outstanding(&self) -> u64 {
        self.outstanding.load(Ordering::Relaxed)
    }

    /// Number of timers currently linked in the wheel (worker-side view).
    #[inline]
    #[must_use]
    pub fn linked(&self) -> u64 {
        self.wheel.count()
    }

    /// Installs an optional inline fire handler (invoked on the worker thread).
    pub fn set_handler(&mut self, handler: Arc<dyn TimerHandler>) {
        self.handler = Some(handler);
    }

    /// One iteration of the event loop: flush pending expirations, drain
    /// commands, advance the wheel, and route fired/cancelled timers.
    ///
    /// This is the method the worker calls each spin; it is also safe to call
    /// directly for caller-driven integration (no worker thread).
    pub fn tick(&mut self) {
        // 1. Flush any expired ids that did not fit last tick.
        self.flush_pending_expired();

        // 2. Drain pending commands and apply them.
        self.cmd_rx.drain_into(&mut self.cmd_buf);
        let cmds = std::mem::take(&mut self.cmd_buf);
        for cmd in cmds {
            self.apply_cmd(cmd);
        }

        // 3. Advance the wheel to the current tick.
        let now = self.clock.now_ticks();
        self.wheel
            .advance(&mut self.slab, now, &mut self.reclaim_buf);

        // 4. Reclaim slots and route task ids.
        let slots = std::mem::take(&mut self.reclaim_buf);
        for slot in slots {
            let task_id = self.slab.get(slot).task_id;
            if self.cancelled.remove(&task_id) {
                // cancelled — do not fire, just free the slot.
            } else {
                self.deliver_expired(task_id);
            }
            if let Some(idx) = NonZeroU32::new(slot) {
                self.slab.free(idx);
            }
            self.outstanding.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Duration the worker should park before re-checking.
    ///
    /// Returns the time until the next deadline, capped at `idle_interval` so
    /// that command-ingestion latency stays bounded. When the wheel is empty,
    /// parks for the full `idle_interval`.
    #[must_use]
    pub fn park_duration(&self, idle_interval: std::time::Duration) -> std::time::Duration {
        match self.wheel.next_deadline() {
            None => idle_interval,
            Some(target_ticks) => self.clock.sleep_until(target_ticks).min(idle_interval),
        }
    }

    /// Drains every expired `task_id` currently buffered (facade-level use).
    #[must_use]
    pub fn drain_expired(&self) -> Vec<TaskId> {
        let mut out = Vec::new();
        self.expired_out.drain_into(&mut out);
        out
    }

    fn apply_cmd(&mut self, cmd: TimerCmd) {
        match cmd {
            TimerCmd::Start {
                task_id,
                deadline_ticks,
            } => {
                if self.cancelled.remove(&task_id) {
                    // Cancelled before start — drop without scheduling.
                    self.outstanding.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
                let Some(slot) = self.slab.allocate() else {
                    // Should not happen: the facade bounds `outstanding` to
                    // capacity. Treat as overflow and drop defensively.
                    tracing::warn!(
                        shard = self.shard_id,
                        task_id,
                        "shard slab exhausted despite outstanding counter; dropping timer"
                    );
                    self.outstanding.fetch_sub(1, Ordering::Relaxed);
                    return;
                };
                self.slab
                    .write(slot, TimerEntry::new(task_id, deadline_ticks));
                if deadline_ticks <= self.wheel.last_tick() {
                    // Already due — fire immediately.
                    self.deliver_expired(task_id);
                    self.slab.free(slot);
                    self.outstanding.fetch_sub(1, Ordering::Relaxed);
                } else {
                    // Disjoint field borrows: wheel and slab are distinct.
                    self.wheel.add(&mut self.slab, slot.get(), deadline_ticks);
                }
            }
            TimerCmd::Cancel { task_id } => {
                self.cancelled.insert(task_id);
            }
        }
    }

    /// Pushes a fired `task_id` to the expired queue, or spills into the
    /// pending buffer if the queue is full (retried next tick). Also invokes
    /// the inline handler if one is registered.
    fn deliver_expired(&mut self, task_id: TaskId) {
        if let Some(handler) = &self.handler {
            handler.on_fire(task_id);
        }
        if self.expired_out.push(task_id).is_err() {
            self.pending_expired.push_back(task_id);
        }
    }

    fn flush_pending_expired(&mut self) {
        while let Some(&tid) = self.pending_expired.front() {
            if self.expired_out.push(tid).is_ok() {
                self.pending_expired.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TimerWheelConfig;
    use std::time::Duration;

    fn make_shard(cfg: TimerWheelConfig) -> (Shard, ShardChannels) {
        let clock = Clock::shared(cfg.tick_ns);
        Shard::new(
            0,
            cfg.bits_per_level,
            cfg.levels,
            cfg.capacity_per_shard,
            cfg.cmd_queue_capacity.next_power_of_two(),
            cfg.cmd_queue_capacity.next_power_of_two(),
            clock,
        )
        .expect("shard")
    }

    fn valid_cfg() -> TimerWheelConfig {
        TimerWheelConfig {
            bits_per_level: 6,
            levels: 3,
            tick_ns: 1_000_000,
            capacity_per_shard: 1024,
            shards: 1,
            cmd_queue_capacity: 64,
            idle_interval_ns: 1_000_000,
        }
    }

    #[test]
    fn start_then_tick_fires_after_duration() {
        let (mut shard, _channels) = make_shard(valid_cfg());
        // schedule 5 ticks out
        let now = shard.clock.now_ticks();
        let _ = shard.cmd_rx.push(TimerCmd::Start {
            task_id: 1,
            deadline_ticks: now + 5,
        });
        // tick once to ingest + add
        shard.tick();
        assert_eq!(shard.linked(), 1);
        // wait for the deadline
        std::thread::sleep(Duration::from_millis(8));
        shard.tick();
        let fired = shard.drain_expired();
        assert_eq!(fired, vec![1]);
        assert_eq!(shard.linked(), 0);
    }

    #[test]
    fn cancel_before_fire_suppresses_delivery() {
        let (mut shard, _channels) = make_shard(valid_cfg());
        let now = shard.clock.now_ticks();
        let _ = shard.cmd_rx.push(TimerCmd::Start {
            task_id: 42,
            deadline_ticks: now + 3,
        });
        let _ = shard.cmd_rx.push(TimerCmd::Cancel { task_id: 42 });
        std::thread::sleep(Duration::from_millis(6));
        shard.tick();
        let fired = shard.drain_expired();
        assert!(fired.is_empty(), "cancelled timer fired: {fired:?}");
        assert_eq!(shard.linked(), 0);
    }

    #[test]
    fn cancel_before_start_is_honoured() {
        let (mut shard, _channels) = make_shard(valid_cfg());
        let now = shard.clock.now_ticks();
        // outstanding was pre-incremented by the facade in real usage; emulate.
        shard.outstanding.fetch_add(2, Ordering::Relaxed);
        let _ = shard.cmd_rx.push(TimerCmd::Cancel { task_id: 7 });
        let _ = shard.cmd_rx.push(TimerCmd::Start {
            task_id: 7,
            deadline_ticks: now + 2,
        });
        std::thread::sleep(Duration::from_millis(5));
        shard.tick();
        assert!(shard.drain_expired().is_empty());
        assert_eq!(shard.linked(), 0);
    }

    #[test]
    fn already_due_timer_fires_immediately() {
        let (mut shard, _channels) = make_shard(valid_cfg());
        let now = shard.clock.now_ticks();
        // deadline in the past
        let _ = shard.cmd_rx.push(TimerCmd::Start {
            task_id: 9,
            deadline_ticks: now,
        });
        shard.tick();
        assert_eq!(shard.drain_expired(), vec![9]);
    }

    #[test]
    fn expired_queue_spills_into_pending_when_full() {
        let (mut shard, _channels) = make_shard(valid_cfg());
        // saturate the expired queue directly
        for tid in 0..shard.expired_out.capacity() as u64 {
            let _ = shard.expired_out.push(tid);
        }
        // now a fire should spill into pending
        shard.deliver_expired(9999);
        assert_eq!(shard.pending_expired.len(), 1);
        // draining the queue then ticking flushes pending
        let _ = shard.drain_expired();
        shard.flush_pending_expired();
        assert!(shard.pending_expired.is_empty());
        assert_eq!(shard.expired_out.pop(), Some(9999));
    }
}

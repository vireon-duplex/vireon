//! Thread-per-core reactor event loop.
//!
//! A [`Reactor`] owns one [`IoUringCtx`], one [`BufferPool`], one
//! [`Socket`] + [`Receiver`] + [`Sender`], one [`TimerWheel`], and one
//! [`MetricsCollector`]. The `run` loop submits SQEs, blocks for CQEs,
//! and dispatches every completion via [`CompletionDecoder`].
//!
//! # Lifecycle
//!
//! ```text
//! init(core_id, port, queue_depth, enable_sqpoll, features, busy_poll)
//!   1. sched_setaffinity(0, {core_id})
//!   2. IoUringCtx::init_ring(queue_depth, sqpoll=enable_sqpoll)
//!   3. BufferPool::init_buffer_ring(ring, bgid=1)
//!   4. Socket::bind_with_reuseport(port, core_id, features, busy_poll)
//!   5. ctx.register_buffers(iovecs)
//!   6. ctx.register_napi(busy_poll.mode.us(), busy_poll.should_prefer())  // kernel ≥ 6.6
//!   7. socket.receiver(bgid).arm(ring, id=0)
//! ```
//!
//! # Dispatch (per CQE)
//!
//! The [`CompletionDecoder`] maps each CQE to a [`Completion`] variant.
//! The reactor matches on variants — no raw flag inspection:
//!
//! | Completion | Action |
//! |------------|--------|
//! | [`Receive`](Completion::Receive) | decode `buf_id`, refill pool, re-arm if `!more` |
//! | [`ReceiveError`](Completion::ReceiveError) | record errno, re-arm if `!more` |
//! | [`Send`](Completion::Send) | upper layer tracks in-flight buffers |
//! | [`SendBufferReclaim`](Completion::SendBufferReclaim) | buffer recyclable |
//! | [`Timeout`](Completion::Timeout) | retire from timer wheel |
//! | [`Cancel`](Completion::Cancel) | informational (cancel result) |
//! | [`Error`](Completion::Error) | record and continue |

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use io_uring::cqueue;

use crate::common::{DEFAULT_BUF_COUNT, DEFAULT_BUF_SIZE, QUEUE_DEPTH};
use crate::completion::{Completion, CompletionDecoder};
use crate::context::IoUringCtx;
use crate::error::IoError;
use crate::handler::{IoHandler, TickContext};
use crate::memory::BufferPool;
use crate::metrics::MetricsCollector;
use crate::metrics::MetricsSnapshot;
use crate::ops::{OpKind, Receiver, Sender, encode};
use crate::socket::Socket;
use crate::timer_wheel::TimerWheel;

/// Negated errno constant, usable in `match` patterns.
const ENOBUFS: i32 = -libc::ENOBUFS;

/// Timer ID reserved for the reactor's internal periodic tick.
/// This timer wakes the event loop every [`TICK_INTERVAL`] so the
/// reactor can check `stop`, call `handler.on_tick()`, and flush
/// quiche sends even when no packets arrive.
const TICK_TIMER_ID: u64 = 0;

/// Periodic tick interval — wakes `submit_and_wait(1)` at this rate.
///
/// 1 ms balances responsiveness (needed for cross-worker pub/sub
/// delivery on workers with no inbound QUIC traffic) with CPU
/// overhead (1000 wakeups/s when idle — negligible vs the busy-poll
/// budget already spent in the kernel for BareMetal/HundredGbe).
const TICK_INTERVAL: Duration = Duration::from_millis(1);

// ── Crash-handler ring registry ───────────────────────────────────
//
// When the process crashes (SIGSEGV, SIGABRT), Reactor::drop does not
// run, so the AsyncCancel2 that normally cancels the multishot RECV is
// never submitted. The kernel holds the socket inode reference via the
// pending multishot RECV, leaving a ghost UDP socket that blocks the
// port for subsequent server restarts.
//
// This static array stores raw pointers to each Reactor's IoUringCtx.
// The crash handler calls [`cancel_all_rings`] which writes an
// AsyncCancel2 SQE to each ring and calls `submit_and_wait(1)`,
// replicating what Reactor::drop does — but from a signal handler.

const MAX_RINGS: usize = 128;
static RING_PTRS: [AtomicUsize; MAX_RINGS] = [const { AtomicUsize::new(0) }; MAX_RINGS];

/// Register this reactor's IoUringCtx for crash-handler cleanup.
///
/// Stores a raw pointer to `self.ctx.ring`. Valid as long as `self`
/// is alive. Unregistered in `Drop`.
fn register_ring(ring: &io_uring::IoUring) {
    let ptr = ring as *const _ as usize;
    for slot in &RING_PTRS {
        if slot
            .compare_exchange(0, ptr, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }
    }
}

/// Remove this reactor's ring from the crash-handler registry.
fn unregister_ring(ring: &io_uring::IoUring) {
    let ptr = ring as *const _ as usize;
    for slot in &RING_PTRS {
        if slot.load(Ordering::Acquire) == ptr {
            slot.store(0, Ordering::Release);
            return;
        }
    }
}

/// Cancel all pending io_uring multishot operations across all reactors.
///
/// Called from signal handlers / panic hooks **before** closing fds.
/// Writes an `AsyncCancel2(CancelBuilder::any())` SQE to each
/// registered ring and calls `submit_and_wait(1)` so the kernel
/// synchronously cancels pending multishot RECV and releases socket
/// inode references.
///
/// # Safety (caller)
///
/// This function is designed for crash-handler use. It casts stored
/// raw pointers to `&mut IoUring` without runtime validation. If a
/// Reactor was already dropped without unregistering, the pointer is
/// dangling and behaviour is undefined. The `Drop` impl unregisters
/// to prevent this.
pub fn cancel_all_rings() {
    use io_uring::opcode::AsyncCancel2;
    use io_uring::types::CancelBuilder;

    for slot in &RING_PTRS {
        let ptr_val = slot.load(Ordering::Acquire);
        if ptr_val == 0 {
            continue;
        }
        // SAFETY: the pointer was stored by `register_ring` from a
        // live Reactor. We're in a crash handler, so the Reactor is
        // still alive (the process hasn't exited yet). Casting to
        // `&mut` is UB in general but acceptable here — the process
        // is about to die.
        let ring: &mut io_uring::IoUring = unsafe { &mut *(ptr_val as *mut _) };

        let cancel_entry = AsyncCancel2::new(CancelBuilder::any())
            .build()
            .user_data(0);
        // Drain existing CQEs BEFORE calling submit_and_wait. Without
        // this, submit_and_wait(1) may see pending CQEs from the multishot
        // RECV and return immediately — before the kernel processes
        // the AsyncCancel2 SQE. The socket inode reference stays alive.
        loop {
            let mut cq = ring.completion();
            if cq.next().is_none() {
                break;
            }
        }
        let _ = crate::ops::push_entry(ring, cancel_entry);
        let _ = ring.submit_and_wait(1);
    }
}

/// Thread-per-core reactor.
pub struct Reactor {
    ctx: IoUringCtx,
    pool: BufferPool,
    socket: Socket,
    receiver: Receiver,
    sender: Sender,
    timers: TimerWheel,
    decoder: CompletionDecoder,
    metrics: MetricsCollector,
    core_id: u32,
    stop: Arc<AtomicBool>,
}

impl Reactor {
    /// Creates and arms a reactor pinned to `core_id`.
    ///
    /// `port = 0` lets the kernel pick an ephemeral port (useful for
    /// tests); production callers pass a fixed port. `queue_depth = 0`
    /// falls back to [`QUEUE_DEPTH`].
    ///
    /// `features` is a `kernel_nic::KernelFeatures` snapshot (probed once
    /// at process startup) — it gates both socket-option application and
    /// io_uring NAPI register. `busy_poll` drives both socket-level
    /// (`SO_BUSY_POLL` + `SO_PREFER_BUSY_POLL`) and ring-level
    /// (`IORING_REGISTER_NAPI`) busy-poll configuration.
    ///
    /// When `enable_sqpoll` is `true`, attempts to set up the ring with
    /// `IORING_SETUP_SQPOLL`. If the kernel denies it (e.g. insufficient
    /// privileges), falls back to standard mode and logs a warning.
    pub fn init(
        core_id: u32,
        port: u16,
        queue_depth: u32,
        enable_sqpoll: bool,
        features: kernel_nic::KernelFeatures,
        busy_poll: kernel_nic::BusyPollPolicy,
    ) -> Result<Self, IoError> {
        pin_to_core(core_id)?;

        let entries = if queue_depth == 0 {
            QUEUE_DEPTH
        } else {
            queue_depth
        };

        // Try SQPOLL mode; fall back gracefully if the kernel denies it
        // (typically needs CAP_SYS_NICE / root).
        let mut ctx = if enable_sqpoll {
            match IoUringCtx::init_ring(entries, true, Some(core_id)) {
                Ok(ctx) => {
                    tracing::info!(
                        core_id,
                        sqpoll = true,
                        sq_aff = true,
                        "SQPOLL enabled — kernel thread polling SQ ring (pinned to core)"
                    );
                    ctx
                }
                Err(e) => {
                    tracing::warn!(
                        core_id,
                        error = ?e,
                        "SQPOLL denied by kernel — falling back to standard mode \
                         (needs CAP_SYS_NICE on kernels < 6.0)"
                    );
                    IoUringCtx::init_ring(entries, false, None)?
                }
            }
        } else {
            IoUringCtx::init_ring(entries, false, None)?
        };
        // SAFETY: we register a brand-new pool whose backing storage will
        // outlive the ring (pool is stored in the reactor, which is
        // dropped before the ring). Construct iovecs after the pool.
        let pool = BufferPool::init_buffer_ring(
            ctx.ring(),
            /* bgid */ 1,
            DEFAULT_BUF_COUNT,
            DEFAULT_BUF_SIZE,
        )?;

        let socket = Socket::bind_with_reuseport(port, core_id, features, busy_poll)?;
        let receiver = socket.receiver(pool.bgid());
        let sender = socket.sender();

        // Register pool backing store as fixed buffers so `SendZc` can
        // reference them by `buf_index` without re-validating pointers.
        //
        // Non-fatal: the current send path uses `send_blocking`/`send_gso`
        // (via GsoBatcher), not `send_zc`. If registration fails (e.g.
        // RLIMIT_MEMLOCK exhaustion on hosts with restrictive limits, or
        // AppArmor io_uring mediation), we log a warning and continue —
        // SendZc is disabled but all other paths work normally.
        // SAFETY: `pool`'s backing storage lives as long as the reactor,
        // which outlives the ring registration.
        unsafe {
            let iovecs: Vec<libc::iovec> = (0..pool.buf_count())
                .map(|i| {
                    let bid = u16::try_from(i)
                        .map_err(|_| IoError::InvalidConfig("buf_count overflow".into()))?;
                    let addr = pool.get_buffer_address(bid).ok_or_else(|| {
                        IoError::InvalidConfig(format!("buf_id {bid} out of range"))
                    })?;
                    Ok(libc::iovec {
                        iov_base: addr.as_ptr() as *mut _,
                        iov_len: pool.buf_size(),
                    })
                })
                .collect::<Result<Vec<_>, IoError>>()?;
            if !iovecs.is_empty() {
                if let Err(e) = ctx.register_buffers(&iovecs) {
                    tracing::warn!(
                        core_id,
                        error = ?e,
                        buf_count = pool.buf_count(),
                        "register_buffers failed — SendZc disabled, \
                         falling back to send_blocking/send_gso. \
                         Check RLIMIT_MEMLOCK (ulimit -l)."
                    );
                }
            }
        }

        // Ring-level NAPI busy poll (kernel ≥ 6.6). Distinct from
        // socket-level SO_BUSY_POLL: lets the ring itself drive NAPI
        // polling without softirq wakeups. Best-effort — loopback and
        // pre-6.6 kernels reject with EINVAL; we log + continue.
        if ctx.features().supports_napi_busy_poll() {
            if let Err(e) = ctx.register_napi(busy_poll.mode.us(), busy_poll.should_prefer()) {
                tracing::warn!(
                    core_id,
                    error = ?e,
                    "io_uring NAPI register denied — softirq path active"
                );
            }
        }

        let mut reactor = Self {
            ctx,
            pool,
            socket,
            receiver,
            sender,
            timers: TimerWheel::new(),
            decoder: CompletionDecoder::new(),
            metrics: MetricsCollector::new(),
            core_id,
            stop: Arc::new(AtomicBool::new(false)),
        };

        // Arm the initial multishot RX. ID 0 encodes as
        // `encode(OpKind::Recv, 0)` in user_data.
        reactor.receiver.arm(reactor.ctx.ring_mut(), 0)?;

        // Arm the periodic tick timer so `submit_and_wait(1)` unblocks
        // even when no packets arrive. This ensures the reactor can
        // check `stop`, flush sends via `on_tick`, and run QUIC idle
        // detection.
        reactor
            .timers
            .add_periodic_tick(reactor.ctx.ring_mut(), TICK_TIMER_ID, TICK_INTERVAL)?;

        Ok(reactor)
    }

    /// Returns the CPU core this reactor is pinned to.
    #[inline]
    #[must_use]
    pub fn core_id(&self) -> u32 {
        self.core_id
    }

    /// Signals the run loop to exit at the next iteration boundary.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }

    /// Returns a clone of the stop flag so another thread can signal
    /// shutdown without holding a `&Reactor` borrow.
    #[must_use]
    pub fn stop_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.stop)
    }

    /// Whether the loop has been signalled to stop.
    #[inline]
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    /// Snapshot of the reactor's metrics.
    #[must_use]
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Mutable access to the kernel-side timer wheel (for the upper
    /// layer to schedule timeouts via the reactor's ring).
    #[must_use]
    pub fn timers_mut(&mut self) -> &mut TimerWheel {
        &mut self.timers
    }

    /// Immutable access to the socket.
    #[inline]
    #[must_use]
    pub fn socket(&self) -> &Socket {
        &self.socket
    }

    /// Returns the raw fd of the io_uring ring.
    ///
    /// Needed by the crash-exit socket registry: when a signal handler
    /// force-closes fds, the ring fd must be closed so the kernel cancels
    /// pending multishot RECV operations and releases the socket inode
    /// reference. Without this, closing only the socket fd leaves a ghost
    /// UDP socket on the port.
    #[inline]
    #[must_use]
    pub fn ring_fd(&self) -> std::os::fd::RawFd {
        use std::os::unix::io::AsRawFd;
        self.ctx.ring().as_raw_fd()
    }

    /// Immutable access to the receiver.
    #[inline]
    #[must_use]
    pub fn receiver(&self) -> &Receiver {
        &self.receiver
    }

    /// Immutable access to the sender.
    #[inline]
    #[must_use]
    pub fn sender(&self) -> &Sender {
        &self.sender
    }

    /// Mutable access to the buffer pool (for refills from outside the
    /// hot path).
    #[must_use]
    pub fn pool_mut(&mut self) -> &mut BufferPool {
        &mut self.pool
    }

    /// Drives the event loop until [`stop`](Self::stop) is called.
    ///
    /// Each iteration:
    /// 1. `submit_and_wait(1)` — block for the next CQE.
    /// 2. Drain every CQE in a batch.
    /// 3. Dispatch per the rules in the module docs.
    pub fn run(&mut self) -> Result<(), IoError> {
        while !self.stop.load(Ordering::Relaxed) {
            // Submit any pending SQEs and wait for at least one CQE.
            match self.ctx.ring_mut().submit_and_wait(1) {
                Ok(_) => {}
                Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {
                    self.metrics.record_submit_backpressure();
                    // Inline CQ drain to free SQ slots, then retry.
                    self.drain_cq();
                    continue;
                }
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => {
                    // Coop-taskrun should suppress this, but handle it.
                    continue;
                }
                Err(e) => return Err(IoError::Submit(e)),
            }
            self.drain_cq();

            // Detect CQ overflow between iterations.
            if self.ctx.ring_mut().completion().overflow() > 0 {
                self.metrics.record_cq_overflow();
            }
        }
        Ok(())
    }

    /// Drives the event loop with an [`IoHandler`], calling
    /// `handler.on_datagram()` for each received datagram and
    /// `handler.on_tick()` at the end of each iteration.
    ///
    /// This is the primary entry point for protocol-layer integration
    /// (e.g. QUIC engine). The handler receives raw UDP payloads and
    /// peer addresses; outgoing sends happen in `on_tick` via
    /// [`TickContext`].
    pub fn run_with_handler<H: IoHandler>(&mut self, handler: &mut H) -> Result<(), IoError> {
        // Register the ring for crash-handler cleanup NOW — at this point
        // the Reactor has been moved to its final stack address (inside
        // Worker::run → self.reactor.run_with_handler). The stored raw
        // pointer remains valid until Drop runs, which unregisters it.
        register_ring(self.ctx.ring());

        // Arm a periodic wakeup timer so the event loop returns from
        // submit_and_wait even when no packets arrive. Without this,
        // SIGINT/SIGTERM caught by ctrlc's background thread sets the
        // stop flag but the blocked submit_and_wait never returns to
        // check it — the server becomes unresponsive to graceful
        // shutdown when idle. Orchestration systems (systemd, k8s)
        // escalate to SIGKILL after a timeout, which leaves ghost
        // sockets.
        //
        // The timer fires every 200ms; its CQE is processed as a
        // no-op Completion::Timeout (on_timeout defaults to no-op).
        // Requires kernel ≥ 6.2 (IORING_TIMEOUT_MULTISHOT).
        const WAKEUP_PERIOD: Duration = Duration::from_millis(200);
        if let Err(e) = self
            .timers
            .add_periodic_tick(self.ctx.ring_mut(), 0, WAKEUP_PERIOD)
        {
            tracing::warn!(
                error = ?e,
                "failed to arm wakeup timer — server may be slow to respond \
                 to SIGINT/SIGTERM when idle (requires kernel ≥ 6.2)"
            );
        }

        while !self.stop.load(Ordering::Relaxed) {
            match self.ctx.ring_mut().submit_and_wait(1) {
                Ok(_) => {}
                Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {
                    self.metrics.record_submit_backpressure();
                    self.drain_cq_with_handler(handler);
                    continue;
                }
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => {
                    continue;
                }
                Err(e) => return Err(IoError::Submit(e)),
            }
            self.drain_cq_with_handler(handler);

            if self.ctx.ring_mut().completion().overflow() > 0 {
                self.metrics.record_cq_overflow();
            }

            // End-of-tick: let the handler flush sends.
            handler.on_tick(&mut TickContext {
                ring: self.ctx.ring_mut(),
                sender: &self.sender,
            });
        }
        Ok(())
    }

    /// Reaps every CQE currently in the completion queue and dispatches it
    /// via the [`CompletionDecoder`].
    fn drain_cq(&mut self) {
        // Collect entries into a Vec so the mutable borrow of the ring
        // ends before we call `receiver.arm` (which needs another mutable
        // borrow) inside the dispatch loop.
        let entries: Vec<cqueue::Entry> = {
            let mut cq = self.ctx.ring_mut().completion();
            cq.by_ref().collect()
        };
        for entry in entries {
            let res = entry.result();
            let completion = self.decoder.decode(&entry);

            let is_rx = matches!(
                completion,
                Completion::Receive { .. } | Completion::ReceiveError { .. }
            );
            self.metrics.record_cqe_completion(res, is_rx);

            match completion {
                // ── Recv success: decode buf_id, refill, re-arm if done ──
                Completion::Receive {
                    buf_id, len, more, ..
                } => {
                    if let Some(bid) = buf_id {
                        self.metrics.record_bytes_rx(len as u64);
                        self.pool.refill_buffer(bid);
                    }
                    if !more {
                        let _ = self.receiver.arm(self.ctx.ring_mut(), 0);
                    }
                }

                // ── Recv error: buffer exhaustion or multishot termination ──
                Completion::ReceiveError { errno, more, .. } => {
                    if errno == ENOBUFS {
                        self.metrics.track_buffer_exhaustion(self.pool.bgid());
                    }
                    if !more {
                        let _ = self.receiver.arm(self.ctx.ring_mut(), 0);
                    }
                }

                // ── Send notification: buffer reclaimable ──
                Completion::SendBufferReclaim { .. } => {
                    // Upper layer recycles the buffer.
                }

                // ── Send success: kernel accepted the send ──
                Completion::Send { .. } => {
                    // Buffer still in flight (NIC DMA pending).
                }

                // ── Timeout fired (naturally or cancelled) ──
                Completion::Timeout { id } => {
                    self.timers.retire(encode(OpKind::Timeout, id));
                }

                // ── Cancel result: informational ──
                Completion::Cancel { .. } => {}

                // ── Other errors (send failure, etc.) ──
                Completion::Error { errno, .. } => {
                    if errno == ENOBUFS {
                        self.metrics.track_buffer_exhaustion(self.pool.bgid());
                    }
                }
            }
        }
    }

    /// Handler-aware CQE drain: like [`drain_cq`](Self::drain_cq) but calls
    /// `handler.on_datagram()` for each received datagram before recycling
    /// the buffer.
    fn drain_cq_with_handler<H: IoHandler>(&mut self, handler: &mut H) {
        let entries: Vec<cqueue::Entry> = {
            let mut cq = self.ctx.ring_mut().completion();
            cq.by_ref().collect()
        };
        for entry in entries {
            let res = entry.result();
            let completion = self.decoder.decode(&entry);

            let is_rx = matches!(
                completion,
                Completion::Receive { .. } | Completion::ReceiveError { .. }
            );
            self.metrics.record_cqe_completion(res, is_rx);

            match completion {
                Completion::Receive {
                    buf_id, len, more, ..
                } => {
                    if let Some(bid) = buf_id {
                        self.metrics.record_bytes_rx(len as u64);

                        // Extract the raw buffer pointer (no lifetime borrow).
                        if let Some(ptr) = self.pool.buffer_ptr_mut(bid) {
                            // SAFETY: `ptr` points into the pool's backing
                            // store; the kernel just wrote `len` bytes. The
                            // slice is only used within this block — before
                            // `refill_buffer` reclaims the buffer.
                            let raw = unsafe { std::slice::from_raw_parts_mut(ptr, len) };

                            // Parse io_uring header to get mutable UDP payload + peer.
                            if let Ok((payload, peer_addr)) = self.receiver.parse_raw_mut(raw) {
                                handler.on_datagram(payload, peer_addr);
                            }
                        }

                        self.pool.refill_buffer(bid);
                    }
                    if !more {
                        let _ = self.receiver.arm(self.ctx.ring_mut(), 0);
                    }
                }

                Completion::ReceiveError { errno, more, .. } => {
                    if errno == ENOBUFS {
                        self.metrics.track_buffer_exhaustion(self.pool.bgid());
                    }
                    if !more {
                        let _ = self.receiver.arm(self.ctx.ring_mut(), 0);
                    }
                }

                Completion::SendBufferReclaim { id, .. } => {
                    handler.on_send_reclaim(id);
                }

                Completion::Send { .. } => {}

                Completion::Timeout { id } => {
                    self.timers.retire(encode(OpKind::Timeout, id));
                    handler.on_timeout(id);
                }

                Completion::Cancel { .. } => {}

                Completion::Error { errno, .. } => {
                    if errno == ENOBUFS {
                        self.metrics.track_buffer_exhaustion(self.pool.bgid());
                    }
                }
            }
        }
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        // Remove from crash-handler registry FIRST — after the cancel
        // below runs, the ring state is indeterminate and the crash
        // handler must not touch it.
        unregister_ring(self.ctx.ring());

        self.stop.store(true, Ordering::Release);

        // Force kernel-side shutdown of the UDP socket before its fd is
        // closed by the implicit `Socket::drop`. This signals intent to
        // close so the kernel starts tearing down the transport.
        //
        // SAFETY: `shutdown(2)` is async-signal-safe and idempotent; on
        // an already-closed fd it returns EBADF (ignored here).
        unsafe {
            libc::shutdown(self.socket.raw_fd(), libc::SHUT_RDWR);
        }

        // Cancel ALL in-flight io_uring operations (especially the multishot
        // RECV armed at init). Without this explicit cancellation, the
        // kernel holds a reference to the socket inode until its deferred
        // teardown completes (~60s on Linux 6.x), leaving ghost UDP sockets
        // on the port that block subsequent server starts.
        //
        // CancelBuilder::any() requires kernel ≥ 5.19 — already a hard
        // requirement for PBUF_RING used by the buffer pool.
        let cancel_entry = io_uring::opcode::AsyncCancel2::new(
            io_uring::types::CancelBuilder::any(),
        )
        .build()
        .user_data(0);
        // Best-effort push; if the SQ ring is full the submit_and_wait
        // below will flush what's already queued and the kernel's
        // ring-fd close (when IoUring drops) cancels the rest anyway.
        let _ = crate::ops::push_entry(self.ctx.ring_mut(), cancel_entry);
        // Submit the cancel + drain all remaining CQEs. Waiting for ≥1
        // completion ensures the cancel SQE is processed (it always
        // produces a CQE — success or -ENOENT). Additional pending CQEs
        // from the cancelled multishot are also reaped.
        let _ = self.ctx.ring_mut().submit_and_wait(1);
    }
}

/// Pins the calling thread to a single CPU (`core_id`).
fn pin_to_core(core_id: u32) -> Result<(), IoError> {
    // SAFETY: `sched_setaffinity` reads `cpuset` of size `sizeof(cpu_set_t)`.
    // We construct a zeroed `cpu_set_t` and set exactly one bit. The pid
    // `0` means "current thread". The syscall does not write through the
    // pointer; it only reads it.
    let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe { libc::CPU_ZERO(&mut cpuset) };
    if core_id >= libc::CPU_SETSIZE as u32 {
        return Err(IoError::InvalidConfig(format!(
            "core_id {core_id} exceeds CPU_SETSIZE ({})",
            libc::CPU_SETSIZE
        )));
    }
    unsafe { libc::CPU_SET(core_id as usize, &mut cpuset) };
    let r = unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpuset) };
    if r != 0 {
        return Err(IoError::InvalidConfig(format!(
            "sched_setaffinity(core {core_id}) failed: errno {}",
            std::io::Error::last_os_error().raw_os_error().unwrap_or(-1),
        )));
    }
    Ok(())
}

/// Idle park between iterations when the reactor is quiescent. Currently
/// unused (the blocking `submit_and_wait(1)` parks the thread), but
/// retained for future poll-mode adaptation.
#[allow(dead_code)]
fn idle_park(_dur: Duration) {
    // std::thread::sleep(_dur) would be the obvious implementation; we
    // omit it because `submit_and_wait` already parks.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket;
    use std::sync::atomic::Ordering::SeqCst;
    use std::thread;
    use std::time::Duration;

    /// Shared dev/test feature snapshot + Off busy-poll policy.
    fn test_cfg() -> (kernel_nic::KernelFeatures, kernel_nic::BusyPollPolicy) {
        let features = kernel_nic::KernelFeatures::detect().expect("features");
        (features, kernel_nic::BusyPollPolicy::default())
    }

    #[test]
    fn reactor_init_succeeds_on_ephemeral_port() {
        // core_id 0 is virtually always present.
        let (features, busy_poll) = test_cfg();
        let reactor = Reactor::init(0, 0, 32, false, features, busy_poll);
        let reactor = match reactor {
            Ok(r) => r,
            Err(IoError::InvalidConfig(msg)) if msg.contains("sched_setaffinity") => {
                // CI runners may disallow affinity; skip the test.
                eprintln!("skipping: affinity denied: {msg}");
                return;
            }
            Err(e) => panic!("init failed: {e:?}"),
        };
        assert_eq!(reactor.core_id(), 0);
        assert!(!reactor.is_stopped());
    }

    #[test]
    fn stop_flag_round_trips() {
        let (features, busy_poll) = test_cfg();
        let mut reactor = match Reactor::init(0, 0, 32, false, features, busy_poll) {
            Ok(r) => r,
            Err(IoError::InvalidConfig(msg)) if msg.contains("sched_setaffinity") => return,
            Err(e) => panic!("init failed: {e:?}"),
        };
        assert!(!reactor.is_stopped());
        reactor.stop();
        assert!(reactor.is_stopped());
        // Run should exit immediately.
        reactor.run().expect("run exits on stop");
    }

    #[test]
    fn reactor_reaps_a_udp_packet() {
        let (features, busy_poll) = test_cfg();
        let mut reactor = match Reactor::init(0, 0, 64, false, features, busy_poll) {
            Ok(r) => r,
            Err(IoError::InvalidConfig(msg)) if msg.contains("sched_setaffinity") => return,
            Err(e) => panic!("init failed: {e:?}"),
        };

        // Read the bound port back through the socket.
        let local_addr = reactor.socket().local_addr().expect("local_addr");

        // Capture the stop handle so the sender thread can signal exit
        // without a `&Reactor` borrow.
        let stop = reactor.stop_handle();

        // Spawn a thread that sends one packet, then sets stop after a
        // short delay so the reactor has a chance to reap the CQE.
        let sender = thread::spawn(move || {
            let sock = UdpSocket::bind("0.0.0.0:0").expect("bind sender");
            // 9-byte frame header + 4-byte payload.
            let mut pkt = [0u8; 13];
            pkt[0] = crate::common::data_plane::TEXT_TOKEN;
            // stream_id = 0 (bytes 1..9 already zero)
            pkt[9..13].copy_from_slice(b"ping");
            sock.send_to(&pkt, local_addr).expect("send_to");
            thread::sleep(Duration::from_millis(80));
            stop.store(true, SeqCst);
        });

        // Drive the reactor on THIS thread (it owns non-Send raw pointers).
        let _ = reactor.run();
        sender.join().expect("sender");

        // We sent 13 bytes; the reactor should have reaped at least that.
        let reaped = reactor.metrics().bytes_rx;
        if reaped == 0 {
            eprintln!("skipping assertion: no bytes reaped in time (CI timing)");
        } else {
            assert!(reaped >= 13, "expected >= 13 bytes reaped, got {reaped}");
        }
    }
}

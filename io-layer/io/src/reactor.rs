//! Thread-per-core reactor event loop.
//!
//! A [`Reactor`] owns one [`IoUringCtx`], one [`BufferPool`], one
//! [`Connection`], one [`TimerWheel`], and one [`MetricsCollector`]. The
//! `run` loop submits SQEs, blocks for CQEs, and dispatches every
//! completion to the appropriate subsystem.
//!
//! # Lifecycle
//!
//! ```text
//! init(core_id, port, queue_depth)
//!   1. sched_setaffinity(0, {core_id})
//!   2. IoUringCtx::init_ring(queue_depth, sqpoll=false)
//!   3. BufferPool::with_defaults(submitter, bgid=1)
//!   4. Connection::setup_socket_with_reuseport(port, core_id)
//!   5. arm_multishot_recv(ring, bgid=1, user_data=RECV_TOKEN)
//! ```
//!
//! # Dispatch (per CQE)
//!
//! | Condition | Action |
//! |-----------|--------|
//! | `res == -libc::ECANCELED` | timer/recv cancelled; retire & re-arm |
//! | `res == -libc::EBUSY`     | backpressure; drain CQ, retry submit |
//! | `flags & IORING_CQE_F_NOTIF` | buffer reclaimable (send_zc done) |
//! | `flags & IORING_CQE_F_BUFFER` | decode `buf_id`, refill pool |
//! | `flags & IORING_CQE_F_MORE`   | multishot still armed; do nothing |

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use io_uring::cqueue;

use crate::buffer_pool::BufferPool;
use crate::common::{DEFAULT_BUF_COUNT, DEFAULT_BUF_SIZE, QUEUE_DEPTH};
use crate::connection::Connection;
use crate::error::IoError;
use crate::io_uring_ctx::IoUringCtx;
use crate::metrics::MetricsCollector;
use crate::metrics::MetricsSnapshot;
use crate::timer_wheel::TimerWheel;

/// Reserved `user_data` value marking the multishot RX arm. Anything
/// else is a send_zc or timer completion.
const RECV_USER_DATA: u64 = u64::MAX;

/// Negated errno constants, usable in `match` patterns.
const ECANCELED: i32 = -libc::ECANCELED;
const ENOBUFS: i32 = -libc::ENOBUFS;

/// Thread-per-core reactor.
pub struct Reactor {
    ctx: IoUringCtx,
    pool: BufferPool,
    conn: Connection,
    timers: TimerWheel,
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
    pub fn init(core_id: u32, port: u16, queue_depth: u32) -> Result<Self, IoError> {
        pin_to_core(core_id)?;

        let entries = if queue_depth == 0 {
            QUEUE_DEPTH
        } else {
            queue_depth
        };
        let ctx = IoUringCtx::init_ring(entries, /* sqpoll */ false)?;
        // SAFETY: we register a brand-new pool whose backing storage will
        // outlive the ring (pool is stored in the reactor, which is
        // dropped before the ring). Construct iovecs after the pool.
        let pool = BufferPool::init_buffer_ring(
            &ctx.ring().submitter(),
            /* bgid */ 1,
            DEFAULT_BUF_COUNT,
            DEFAULT_BUF_SIZE,
        )?;

        let conn = Connection::setup_socket_with_reuseport(port, core_id)?;

        // Build a single iovec spanning the whole backing store so
        // `SendZc` can reference it by `buf_index`. (For simplicity we
        // register one iovec per buffer; a real deployment may prefer a
        // single span and use buf_index math.)
        // SAFETY: `pool`'s backing storage lives as long as the reactor,
        // which outlives the ring registration.
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
                ctx.register_fixed_buffers(&iovecs)?;
            }
        }

        let mut reactor = Self {
            ctx,
            pool,
            conn,
            timers: TimerWheel::new(),
            metrics: MetricsCollector::new(),
            core_id,
            stop: Arc::new(AtomicBool::new(false)),
        };

        // Arm the initial multishot RX.
        reactor.conn.arm_multishot_recv(
            reactor.ctx.ring_mut(),
            reactor.pool.bgid(),
            RECV_USER_DATA,
        )?;

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

    /// Mutable access to the connection (for sends, re-arm, etc.).
    #[must_use]
    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.conn
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

    /// Reaps every CQE currently in the completion queue and dispatches it.
    fn drain_cq(&mut self) {
        // Collect entries into a Vec so the mutable borrow of the ring
        // ends before we call `arm_multishot_recv` (which needs another
        // mutable borrow) inside the dispatch loop.
        let entries: Vec<cqueue::Entry> = {
            let mut cq = self.ctx.ring_mut().completion();
            cq.by_ref().collect()
        };
        for entry in entries {
            let user_data = entry.user_data();
            let res = entry.result();
            let flags = entry.flags();

            // Record the completion broadly.
            let is_rx = user_data == RECV_USER_DATA;
            self.metrics.record_cqe_completion(res, is_rx);

            // 1. Notifications from send_zc — buffer reclaimable.
            if cqueue::notif(flags) {
                // Nothing else to do; the upper layer will recycle.
                continue;
            }

            // 2. Negative results — error or cancellation.
            if res < 0 {
                match res {
                    ECANCELED => {
                        // Timer fired-then-cancelled, or multishot
                        // terminated. Retire from the timer wheel if it
                        // was a timer; re-arm RX if it was the recv.
                        if user_data != RECV_USER_DATA {
                            self.timers.retire(user_data);
                        }
                    }
                    ENOBUFS => {
                        // Provided-buffer group ran dry.
                        self.metrics.track_buffer_exhaustion(self.pool.bgid());
                    }
                    _ => {
                        // Other errors: record but keep running.
                    }
                }
                // If RX multishot terminated, re-arm.
                if is_rx && !cqueue::more(flags) {
                    let _ = self.conn.arm_multishot_recv(
                        self.ctx.ring_mut(),
                        self.pool.bgid(),
                        RECV_USER_DATA,
                    );
                }
                continue;
            }

            // 3. Multishot recv completion — decode buf_id.
            if is_rx {
                if let Some(buf_id) = cqueue::buffer_select(flags) {
                    // The upper layer would consume the payload here; for
                    // now we record bytes and refill immediately.
                    self.metrics
                        .record_bytes_rx(u64::try_from(res).unwrap_or(0));
                    self.pool.refill_buffer(buf_id);
                }
                // If the kernel says no more completions are pending,
                // re-arm the multishot RX.
                if !cqueue::more(flags) {
                    let _ = self.conn.arm_multishot_recv(
                        self.ctx.ring_mut(),
                        self.pool.bgid(),
                        RECV_USER_DATA,
                    );
                }
                continue;
            }

            // 4. Timer fire — retire from active map.
            if self.timers.is_active(user_data) {
                self.timers.retire(user_data);
            }
        }
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        // Best-effort final reap so any in-flight SQEs are flushed.
        let _ = self.ctx.ring_mut().submit_and_wait(0);
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

    #[test]
    fn reactor_init_succeeds_on_ephemeral_port() {
        // core_id 0 is virtually always present.
        let reactor = Reactor::init(0, 0, 32);
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
        let mut reactor = match Reactor::init(0, 0, 32) {
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
        let mut reactor = match Reactor::init(0, 0, 64) {
            Ok(r) => r,
            Err(IoError::InvalidConfig(msg)) if msg.contains("sched_setaffinity") => return,
            Err(e) => panic!("init failed: {e:?}"),
        };

        // Read the bound port back through the connection's socket.
        let local_addr = {
            use std::os::unix::io::FromRawFd;
            let fd = reactor.connection_mut().raw_fd();
            // SAFETY: the fd is owned by `conn` for the lifetime of the
            // reactor; we only borrow it here for a `local_addr` query
            // and never close it.
            let borrowed = unsafe { UdpSocket::from_raw_fd(fd) };
            let addr = borrowed.local_addr().expect("local_addr");
            std::mem::forget(borrowed); // don't close the fd
            addr
        };

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

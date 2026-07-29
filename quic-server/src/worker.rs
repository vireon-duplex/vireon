//! Worker — owns one `io::Reactor` + one `QuicEngine` and drives the event loop.
//!
//! In single-worker mode the worker runs on the main thread.
//! In multi-worker mode each worker runs on its own pinned thread.

use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;

use io::Reactor;
use quic_engine::{ForwardMesh, ForwardedPacket, QuicEngineBuilder, QuicPreset};

use crate::cluster::ClusterWorkerHandle;
use crate::config::ServerConfig;
use crate::error::ServerError;
use crate::stack::VireonStack;

/// A QUIC worker — one Reactor + one VireonStack (engine + layers).
///
/// Not `Send` (io_uring ring + buffer pool hold raw pointers).
/// Each worker must stay on the thread that created it.
pub struct Worker {
    core_id: u32,
    reactor: Reactor,
    handler: VireonStack,
}

impl Worker {
    /// Creates a new worker pinned to `core_id`, bound to `config.port`.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] if reactor or engine initialization fails.
    pub fn create(
        config: &ServerConfig,
        core_id: u32,
        num_workers: u32,
        inbox_rx: mpsc::Receiver<ForwardedPacket>,
        outboxes: Vec<mpsc::SyncSender<ForwardedPacket>>,
        worker_inbox: Option<mpsc::Receiver<crate::layers::InterWorkerMsg>>,
        worker_outboxes: Vec<mpsc::SyncSender<crate::layers::InterWorkerMsg>>,
        cluster_handle: Option<ClusterWorkerHandle>,
        shutdown_requested: Arc<AtomicBool>,
    ) -> Result<Self, ServerError> {
        tracing::info!(core_id, port = config.port, "initializing reactor");

        let features = kernel_nic::KernelFeatures::detect()
            .map_err(|e| ServerError::ReactorInit(format!("feature detect: {e:?}")))?;
        let busy_poll = busy_poll_policy_from_preset(config.preset, features);

        let reactor = Reactor::init(
            core_id,
            config.port,
            config.queue_depth,
            config.sqpoll,
            features,
            busy_poll,
        )
        .map_err(|e| ServerError::ReactorInit(format!("{e:?}")))?;

        let local_addr = reactor
            .socket()
            .local_addr()
            .map_err(|e| ServerError::ReactorInit(format!("local_addr: {e:?}")))?;

        tracing::info!(core_id, %local_addr, "reactor bound");

        // Auto-size the buffer pool based on cgroup memory limits.
        // Falls back to the default 256 MiB on bare metal.
        let pool_budget = buffer_pool::maintenance::cgroup::recommended_pool_budget();
        let cgroup_limit = buffer_pool::maintenance::cgroup::cgroup_memory_limit();
        tracing::info!(
            core_id,
            cgroup_limit_bytes = cgroup_limit.unwrap_or(0),
            pool_budget_bytes = pool_budget,
            "buffer pool sized for container memory"
        );

        // Derive the local NUMA node from the now-pinned reactor thread and
        // bind arena slab memory to it. On single-NUMA hosts this is node 0
        // and `mbind` is a cheap kernel no-op; on multi-socket systems it
        // keeps each per-core pool's backing memory local to its reactor.
        let numa_node = buffer_pool::numa::current_node();
        tracing::info!(core_id, numa_node = numa_node.0, "buffer pool NUMA node");

        let mesh = ForwardMesh::new(core_id, num_workers, inbox_rx, outboxes);

        let engine = QuicEngineBuilder::from_config(config.quic_config.clone())
            .reactor_id(core_id)
            .local_addr(local_addr)
            .cert_path(&config.cert_path)
            .key_path(&config.key_path)
            .buffer_pool(
                buffer_pool::BufferPoolBuilder::new()
                    .max_memory(pool_budget)
                    .huge_threshold(config.tuning.buffer_huge_threshold)
                    .numa_node(numa_node)
                    .build()
                    .map_err(|e| ServerError::EngineInit(format!("buffer pool: {e:?}")))?,
            )
            .forward_mesh(mesh)
            .num_workers(num_workers)
            .build()
            .map_err(|e| ServerError::EngineInit(format!("{e:?}")))?;

        let cluster =
            cluster_handle.and_then(|h| config.cluster.as_ref().map(|cfg| (&cfg.cluster_cfg, h)));

        let mut handler = VireonStack::new(
            engine,
            core_id,
            config.echo,
            config.acl_table.clone(),
            config.wal_root.as_deref(),
            config.wal_retention,
            config.wal_sync,
            config.max_topics,
            config.max_subscribers_per_topic,
            cluster,
            worker_inbox,
            worker_outboxes,
            shutdown_requested,
            config.audit_log.as_deref(),
            &config.tuning,
        )?;
        handler.metrics = config.metrics.clone();

        Ok(Self {
            core_id,
            reactor,
            handler,
        })
    }

    /// Runs the event loop until the reactor's stop flag is set.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] if the io_uring loop fails.
    pub fn run(mut self) -> Result<(), ServerError> {
        tracing::info!(core_id = self.core_id, "worker started");
        // Wire the reactor's stop handle into the handler so it can
        // signal reactor exit after the graceful-drain period.
        self.handler.set_reactor_stop(self.reactor.stop_handle());
        self.reactor
            .run_with_handler(&mut self.handler)
            .map_err(|e| ServerError::EventLoop(format!("{e:?}")))?;
        tracing::info!(core_id = self.core_id, "worker stopped");
        Ok(())
    }

    /// Returns a stop handle so an external thread can signal shutdown.
    #[must_use]
    pub fn stop_handle(&self) -> Arc<AtomicBool> {
        self.reactor.stop_handle()
    }

    /// Returns the core id this worker is pinned to.
    #[must_use]
    #[allow(dead_code)]
    pub const fn core_id(&self) -> u32 {
        self.core_id
    }

    /// Returns the reactor's UDP socket fd (for eBPF REUSEPORT steering).
    #[must_use]
    pub fn socket_fd(&self) -> RawFd {
        self.reactor.socket().raw_fd()
    }

    /// Spawns a worker on a dedicated thread pinned to `core_id`.
    ///
    /// Returns the `JoinHandle` plus a receiver that yields a tuple of
    /// `(stop_handle, socket_fd)` once the reactor is initialised inside
    /// the thread. The socket fd is needed to attach the eBPF REUSEPORT
    /// steering program after all workers are bound.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] if the thread cannot be spawned.
    pub fn spawn(
        config: ServerConfig,
        core_id: u32,
        num_workers: u32,
        inbox_rx: mpsc::Receiver<ForwardedPacket>,
        outboxes: Vec<mpsc::SyncSender<ForwardedPacket>>,
        worker_inbox: Option<mpsc::Receiver<crate::layers::InterWorkerMsg>>,
        worker_outboxes: Vec<mpsc::SyncSender<crate::layers::InterWorkerMsg>>,
        cluster_handle: Option<ClusterWorkerHandle>,
        shutdown_requested: Arc<AtomicBool>,
    ) -> Result<
        (
            thread::JoinHandle<Result<(), ServerError>>,
            mpsc::Receiver<(Arc<AtomicBool>, RawFd)>,
        ),
        ServerError,
    > {
        let (tx, rx) = mpsc::channel();
        let builder = thread::Builder::new().name(format!("vireon-reactor-{core_id}"));
        let handle = builder
            .spawn(move || {
                let worker = match Worker::create(
                    &config,
                    core_id,
                    num_workers,
                    inbox_rx,
                    outboxes,
                    worker_inbox,
                    worker_outboxes,
                    cluster_handle,
                    shutdown_requested,
                ) {
                    Ok(w) => w,
                    Err(e) => {
                        tracing::error!(core_id, error = %e, "failed to create worker");
                        return Err(e);
                    }
                };
                // Send stop handle + socket fd back before entering run loop.
                let fd = worker.reactor.socket().raw_fd();
                let _ = tx.send((worker.stop_handle(), fd));
                // Register the io_uring ring fd FIRST (lower slot index) so
                // close_all() closes it before the socket fd. Closing the ring
                // triggers kernel cancellation of pending multishot RECV,
                // releasing the socket inode reference. Without this, closing
                // only the socket fd leaves a ghost UDP socket on the port.
                crate::socket_registry::REGISTRY.register(worker.reactor.ring_fd());
                // Register the socket fd so the signal handler / panic hook
                // can force-close it on a crash exit.
                crate::socket_registry::REGISTRY.register(fd);
                // NOTE: ring pointer registration for crash-handler
                // AsyncCancel2 happens inside run_with_handler, where the
                // Reactor is at its final stack address.
                worker.run()
            })
            .map_err(|e| ServerError::ThreadSpawn(format!("{e}")))?;
        Ok((handle, rx))
    }
}

/// Maps a [`QuicPreset`] + detected [`kernel_nic::KernelFeatures`] to a
/// [`kernel_nic::BusyPollPolicy`].
///
/// This is the **config-driven** busy-poll decision (phase 5): instead of
/// hard-coding `SO_BUSY_POLL=50µs`, the preset selects the mode + µs
/// budget. `SO_PREFER_BUSY_POLL` is applied only when both the mode
/// requests it (`Latency`/`Throughput`/`Adaptive`) and the kernel reports
/// the capability.
///
/// | Preset          | Mode              | `us` | `prefer` |
/// |-----------------|-------------------|------|----------|
/// | `Cloud`         | `Off`             | 0    | false    |
/// | `BareMetal`     | `Latency`         | 50   | if cap.  |
/// | `HundredGbe`    | `Throughput`      | 200  | if cap.  |
/// | `LowLatency`    | `Latency`         | 50   | if cap.  |
/// | `MaxThroughput` | `Throughput`      | 200  | if cap.  |
fn busy_poll_policy_from_preset(
    preset: QuicPreset,
    features: kernel_nic::KernelFeatures,
) -> kernel_nic::BusyPollPolicy {
    use kernel_nic::{BusyPollMode, BusyPollPolicy};
    let mode = match preset {
        QuicPreset::Cloud => BusyPollMode::Off,
        QuicPreset::BareMetal | QuicPreset::LowLatency => BusyPollMode::Latency { us: 50 },
        QuicPreset::HundredGbe | QuicPreset::MaxThroughput => BusyPollMode::Throughput { us: 200 },
        // `QuicPreset` is `#[non_exhaustive]`; unknown future variants
        // fall back to the conservative Off policy.
        _ => BusyPollMode::Off,
    };
    BusyPollPolicy::new(mode, features.supports_prefer_busy_poll())
}

/// Installs a Ctrl-C handler with graceful-to-forced escalation.
///
/// **First SIGINT/SIGTERM**: sets `shutdown_requested`, triggering the
/// graceful drain. Connections receive CONNECTION_CLOSE frames, and each
/// reactor exits after [`DRAIN_TIMEOUT`](crate::stack::DRAIN_TIMEOUT).
///
/// **Second SIGINT/SIGTERM**: forces immediate process exit (code 130 =
/// 128 + SIGINT). This gives the operator an escape hatch if the drain
/// takes too long or is stuck.
///
/// # Errors
///
/// Returns an error if the signal handler cannot be installed.
pub fn install_signal_handler(shutdown_requested: Arc<AtomicBool>) -> Result<(), ServerError> {
    use std::sync::atomic::AtomicU8;
    let signal_count = Arc::new(AtomicU8::new(0));
    ctrlc::set_handler(move || {
        let prev = signal_count.fetch_add(1, Ordering::AcqRel);
        if prev == 0 {
            tracing::info!(
                "received SIGINT/SIGTERM — initiating graceful shutdown (press Ctrl-C again to force exit)"
            );
            shutdown_requested.store(true, Ordering::Release);
        } else {
            tracing::warn!("received second SIGINT/SIGTERM — forcing immediate exit");
            #[cfg(feature = "profiling")]
            {
                crate::profiler_force_dump();
            }
            // Cancel io_uring multishot RECV ops, then shutdown+close fds.
            io::cancel_all_rings();
            crate::socket_registry::REGISTRY.close_all();
            std::process::exit(130);
        }
    })
    .map_err(|e| ServerError::SignalHandler(format!("{e}")))
}

/// Install one-shot handlers for SIGSEGV/SIGABRT/SIGBUS/SIGFPE/SIGILL.
///
/// Each handler force-closes every registered socket fd, then resets its
/// own disposition to `SIG_DFL` and re-raises the signal so the default
/// core-dump / process-termination behavior still fires.
///
/// # Why
///
/// Without this, a crash from unsafe code or a `libc::abort` from a C
/// dependency leaves SO_REUSEPORT sockets in `UNCONN` state on the bound
/// port. Subsequent server restarts route handshakes to those zombie
/// sockets and time out.
///
/// # Safety
///
/// The handlers use only async-signal-safe primitives: atomic swaps and
/// `libc::close`. `SA_RESETHAND` ensures the handler auto-resets on entry
/// so a fault inside the handler terminates immediately instead of
/// recursing.
pub fn install_crash_handlers() {
    /// Signal-handler body. Must be async-signal-safe.
    extern "C" fn crash_handler(signum: libc::c_int) {
        unsafe {
            // Cancel io_uring multishot operations on all registered rings.
            // For the current thread's ring, this is race-free (the signal
            // interrupted the event loop). For other threads' rings, the
            // submit may fail due to concurrent SQ access — close_all's
            // shutdown() provides the fallback for those.
            io::cancel_all_rings();
            crate::socket_registry::REGISTRY.close_all();
            // Restore default disposition so re-raise produces the normal
            // core-dump / termination behavior. SA_RESETHAND already did
            // this on entry, but be explicit in case of SA_NODEFER.
            let mut dfl: libc::sigaction = std::mem::zeroed();
            dfl.sa_sigaction = libc::SIG_DFL;
            libc::sigaction(signum, &dfl, std::ptr::null_mut());
            libc::raise(signum);
        }
    }

    // Crash signals we want to clean up before. Not SIGKILL (can't be
    // caught) and not SIGSTOP (same).
    let crash_signals: [libc::c_int; 5] = [
        libc::SIGSEGV,
        libc::SIGABRT,
        libc::SIGBUS,
        libc::SIGFPE,
        libc::SIGILL,
    ];

    for &sig in &crash_signals {
        unsafe {
            let mut act: libc::sigaction = std::mem::zeroed();
            act.sa_sigaction = crash_handler as usize;
            act.sa_flags = libc::SA_RESETHAND | libc::SA_NODEFER;
            libc::sigaction(sig, &act, std::ptr::null_mut());
        }
    }
}

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
                    .numa_node(numa_node)
                    .build()
                    .map_err(|e| ServerError::EngineInit(format!("buffer pool: {e:?}")))?,
            )
            .forward_mesh(mesh)
            .num_workers(num_workers)
            .build()
            .map_err(|e| ServerError::EngineInit(format!("{e:?}")))?;

        let cluster = cluster_handle.and_then(|h| {
            config
                .cluster
                .as_ref()
                .map(|cfg| (&cfg.cluster_cfg, h))
        });

        let handler = VireonStack::new(
            engine,
            core_id,
            config.echo,
            config.acl_table.clone(),
            config.wal_root.as_deref(),
            cluster,
            shutdown_requested,
        )?;

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
            std::process::exit(130);
        }
    })
    .map_err(|e| ServerError::SignalHandler(format!("{e}")))
}

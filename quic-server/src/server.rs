//! [`QuicServer`] — top-level orchestrator that boots workers.

use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::Duration;

use crate::config::{ServerConfig, WorkerMode};
use crate::error::ServerError;
use crate::worker::{Worker, install_crash_handlers, install_signal_handler};

/// Top-level QUIC server.
pub struct QuicServer {
    config: ServerConfig,
}

impl QuicServer {
    /// Creates a new server from resolved configuration.
    #[must_use]
    pub const fn new(config: ServerConfig) -> Self {
        Self { config }
    }

    /// Runs the server in the configured mode until shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] on fatal failure.
    pub fn run(&self) -> Result<(), ServerError> {
        let nic_manager = kernel_nic::NicManagerBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("kernel-nic: {e:?}")))?;

        // Start the Prometheus /metrics endpoint if configured.
        if let (Some(port), Some(metrics)) = (self.config.metrics_port, &self.config.metrics) {
            crate::metrics::start_metrics_endpoint(metrics.clone(), port)
                .map_err(|e| ServerError::Config(format!("metrics endpoint: {e}")))?;
        }

        tracing::info!(
            cpus = nic_manager.topology().thread_count(),
            cores = nic_manager.topology().core_count(),
            nics = nic_manager.nics().len(),
            features = %nic_manager.features(),
            "kernel-nic ready"
        );

        match self.config.mode {
            WorkerMode::Single => self.run_single(),
            WorkerMode::Multi => self.run_multi(),
            WorkerMode::Distributed => self.run_distributed(),
        }
    }

    // ── Single Worker ──────────────────────────────────────────────

    fn run_single(&self) -> Result<(), ServerError> {
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        // Single-worker mode: create a dummy mesh (never used — no other
        // workers to forward to or receive from).
        let (dummy_tx, dummy_rx) = std::sync::mpsc::sync_channel::<quic_engine::ForwardedPacket>(1);

        // P6: launch cluster transport if configured.
        let (cluster_handles, cluster_join) = self.launch_cluster_transport(1)?;
        let cluster_handle = cluster_handles.and_then(|mut h| h.pop());

        let worker = Worker::create(
            &self.config,
            0,
            1,
            dummy_rx,
            vec![dummy_tx],
            None,
            Vec::new(),
            cluster_handle,
            shutdown_requested.clone(),
        )?;
        install_signal_handler(shutdown_requested)?;
        install_crash_handlers();

        // Detect ghost sockets and attach eBPF steering if any are present.
        // Single-worker without ghosts: no steering needed (only one socket
        // receives all packets). Single-worker WITH ghosts: attach steering
        // with an offset so packets bypass dead sockets.
        let total_on_port = kernel_nic::count_udp_sockets_on_port(self.config.port).unwrap_or(1);
        let ghost_offset = total_on_port.saturating_sub(1) as u32;

        if ghost_offset > 0 {
            tracing::warn!(
                total_sockets = total_on_port,
                ghost_offset,
                "detected {ghost_offset} ghost UDP socket(s) on port {} — \
                 attaching offset eBPF steering",
                self.config.port,
            );
            match kernel_nic::attach_quic_reuseport_steering(worker.socket_fd(), 1, ghost_offset) {
                Ok(()) => {
                    tracing::info!(
                        ghost_offset,
                        "eBPF steering attached for single worker (ghost bypass)"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "eBPF REUSEPORT steering unavailable — ghost sockets may cause \
                         packet loss. To enable: sudo setcap cap_bpf,cap_net_admin+ep \
                         ./target/x86_64-unknown-linux-gnu/release/quic-server"
                    );
                }
            }
        }

        tracing::info!("starting single-worker server (core 0)");
        let result = worker.run();

        // Join the cluster transport thread for a clean shutdown.
        if let Some(join) = cluster_join {
            if join.join().is_err() {
                tracing::warn!("cluster transport thread panicked during shutdown");
            }
        }
        result
    }

    // ── Multi Worker (thread-per-core) ─────────────────────────────

    fn run_multi(&self) -> Result<(), ServerError> {
        let num_workers = self.config.num_workers();
        tracing::info!(workers = num_workers, "starting multi-worker server");

        // One shared shutdown flag for all workers + signal handler.
        let shutdown_requested = Arc::new(AtomicBool::new(false));

        // P6: launch cluster transport if configured. Returns one
        // ClusterWorkerHandle per worker (in core_id order).
        let (cluster_handles_opt, cluster_join) = self.launch_cluster_transport(num_workers)?;
        let mut cluster_handles = cluster_handles_opt.map(std::collections::VecDeque::from);

        // Build the inter-worker forwarding mesh. Each worker gets one
        // inbox receiver + the shared sender vector. The mesh provides a
        // migration safety net: when eBPF REUSEPORT steering is
        // unavailable, the kernel hash may route a migrated client's
        // packets to the wrong worker. That worker reads the target
        // reactor_id from DCID[0..4] and forwards the packet.
        let mesh_channels = quic_engine::ForwardMesh::build_channels(num_workers as u32);

        // Build inter-worker pub/sub channels. Each worker broadcasts
        // publishes to all others so subscribers on any worker receive
        // the message regardless of which worker the publisher landed on.
        let pubsub_channels = build_inter_worker_pubsub_channels(num_workers);

        let mut handles = Vec::with_capacity(num_workers);
        let mut stop_rx_list = Vec::with_capacity(num_workers);

        for (core_id, ((inbox_rx, outboxes), (ps_inbox, ps_outboxes))) in mesh_channels
            .into_iter()
            .zip(pubsub_channels.into_iter())
            .enumerate()
        {
            let core_id = core_id as u32;
            // Pop the next cluster handle in order (core_id matches).
            let cluster_handle = cluster_handles.as_mut().and_then(|q| q.pop_front());
            let (handle, stop_rx) = Worker::spawn(
                self.config.clone(),
                core_id,
                num_workers as u32,
                inbox_rx,
                outboxes,
                Some(ps_inbox),
                ps_outboxes,
                cluster_handle,
                shutdown_requested.clone(),
            )?;
            handles.push(handle);
            stop_rx_list.push(stop_rx);
        }

        // Collect each worker's stop handle (reactor's internal flag) and
        // socket fd (for eBPF REUSEPORT steering attachment).
        let mut stop_handles: Vec<Arc<AtomicBool>> = Vec::with_capacity(num_workers);
        let mut socket_fds: Vec<RawFd> = Vec::with_capacity(num_workers);
        for rx in &stop_rx_list {
            if let Ok((stop_handle, fd)) = rx.recv_timeout(Duration::from_secs(5)) {
                stop_handles.push(stop_handle);
                socket_fds.push(fd);
            }
        }

        if stop_handles.is_empty() {
            return Err(ServerError::ReactorInit(
                "no worker responded with a stop handle".into(),
            ));
        }

        tracing::info!(
            workers_ready = stop_handles.len(),
            "all workers initialised, installing signal handler"
        );

        // Attach eBPF QUIC DCID steering program to the REUSEPORT group.
        // Detects ghost sockets from previous (pre-fix) runs and applies
        // a steering offset so packets bypass dead sockets.
        //
        // The kernel's REUSEPORT socket array is ordered by bind time:
        // ghost sockets (bound first, never closed) occupy indices
        // [0, ghost_count), and live worker sockets occupy
        // [ghost_count, ghost_count + num_workers). The eBPF program
        // does `(DCID % num_workers) + ghost_offset` to return indices
        // that fall entirely within the live range.
        let total_on_port =
            kernel_nic::count_udp_sockets_on_port(self.config.port).unwrap_or(socket_fds.len());
        let num_live = socket_fds.len();
        let ghost_offset = total_on_port.saturating_sub(num_live) as u32;

        if ghost_offset > 0 {
            tracing::warn!(
                total_sockets = total_on_port,
                live_workers = num_live,
                ghost_offset,
                "detected {ghost_offset} ghost UDP socket(s) on port {} — \
                 using offset eBPF steering to bypass them. \
                 These are permanent on kernel 6.8.0-134 (reboot to clear).",
                self.config.port,
            );
        }

        // Attach when multi-worker OR when ghost sockets are detected
        // (single-worker with ghosts needs steering to avoid routing
        // packets to dead sockets).
        if num_live > 1 || ghost_offset > 0 {
            let Some(&primary_fd) = socket_fds.first() else {
                tracing::error!("no worker sockets available — skipping eBPF attach");
                return join_all(handles, &shutdown_requested);
            };
            match kernel_nic::attach_quic_reuseport_steering(
                primary_fd,
                num_live as u32,
                ghost_offset,
            ) {
                Ok(()) => {
                    tracing::info!(
                        workers = num_live,
                        ghost_offset,
                        "eBPF QUIC DCID reuseport steering attached"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "eBPF REUSEPORT steering unavailable. Connection distribution uses \
                         kernel (src_ip,src_port) hash — uniform but no migration support. \
                         Inter-worker forwarding mesh is ACTIVE as migration safety net. \
                         To enable optimal DCID-based routing: \
                         sudo setcap cap_bpf,cap_net_admin+ep \
                         ./target/x86_64-unknown-linux-gnu/release/quic-server"
                    );
                }
            }
        }

        // Signal handler: first Ctrl-C initiates graceful drain, second
        // forces immediate exit. Each worker's VireonStack detects the
        // flag in on_tick and drives its own CONNECTION_CLOSE drain.
        install_signal_handler(shutdown_requested.clone())?;
        install_crash_handlers();

        let result = join_all(handles, &shutdown_requested);

        // Join the cluster transport thread for a clean shutdown.
        if let Some(join) = cluster_join {
            if join.join().is_err() {
                tracing::warn!("cluster transport thread panicked during shutdown");
            }
        }

        result
    }

    // ── Distributed (stub) ─────────────────────────────────────────

    fn run_distributed(&self) -> Result<(), ServerError> {
        tracing::info!("distributed mode — coordination layer stub");

        let self_node = session_registry::NodeId::new(1);

        let _registry_cfg = session_registry::builder::RegistryBuilder::new(self_node)
            .map_err(|e| ServerError::Config(format!("registry: {e:?}")))?
            .build()
            .map_err(|e| ServerError::Config(format!("registry: {e:?}")))?;

        let _router = session_router::builder::RouterBuilder::new()
            .build_resolver()
            .map_err(|e| ServerError::Config(format!("router: {e:?}")))?;

        let _migration_cfg = session_migration_manager::builder::MigrationServiceBuilder::new()
            .build()
            .map_err(|e| ServerError::Config(format!("migration: {e:?}")))?;

        let _replication_cfg =
            session_replication_service::builder::ReplicationServiceBuilder::new()
                .build()
                .map_err(|e| ServerError::Config(format!("replication: {e:?}")))?;

        tracing::info!("session cluster services initialized");

        self.run_multi()
    }

    // ── P6: cluster transport launch helper ────────────────────────

    /// Launches the cluster transport thread if `config.cluster` is
    /// set, returning one [`ClusterWorkerHandle`] per worker. Returns
    /// `None` in single-node mode.
    ///
    /// # Errors
    /// Returns [`ServerError::Config`] if the transport thread cannot
    /// bind its UDP socket or spawn.
    fn launch_cluster_transport(
        &self,
        num_workers: usize,
    ) -> Result<
        (
            Option<Vec<crate::cluster::ClusterWorkerHandle>>,
            Option<thread::JoinHandle<()>>,
        ),
        ServerError,
    > {
        let Some(cfg) = self.config.cluster.as_ref() else {
            return Ok((None, None));
        };
        tracing::info!(
            self_id = cfg.cluster_cfg.self_id.raw(),
            peers = cfg.peer_addrs.len(),
            workers = num_workers,
            "starting cluster transport"
        );
        let (handles, join) = crate::cluster::launch_cluster(cfg, num_workers)
            .map_err(|e| ServerError::Config(format!("cluster transport: {e}")))?;
        Ok((Some(handles), Some(join)))
    }
}

/// Joins all worker threads.
///
/// If any worker panics, sets `shutdown_requested` so the surviving workers
/// begin their own graceful-drain sequence — otherwise they would outlive
/// the failed worker indefinitely, holding their socket fds open.
fn join_all(
    handles: Vec<thread::JoinHandle<Result<(), ServerError>>>,
    shutdown_requested: &std::sync::atomic::AtomicBool,
) -> Result<(), ServerError> {
    use std::sync::atomic::Ordering;
    let mut last_err = None;

    for handle in handles {
        let name = handle.thread().name().unwrap_or("unnamed").to_string();
        match handle.join() {
            Ok(Ok(())) => {
                tracing::info!(thread = %name, "worker exited cleanly");
            }
            Ok(Err(e)) => {
                tracing::error!(thread = %name, error = %e, "worker exited with error");
                // Signal survivors so they don't wait forever for the dead
                // worker's share of the work.
                shutdown_requested.store(true, Ordering::Release);
                last_err = Some(e);
            }
            Err(_) => {
                tracing::error!(thread = %name, "worker thread panicked");
                // The panic hook in main.rs already closed the panicking
                // worker's fd via the global registry; signal the others.
                shutdown_requested.store(true, Ordering::Release);
                last_err = Some(ServerError::WorkerPanic(name));
            }
        }
    }

    last_err.map_or(Ok(()), Err)
}

/// Builds the inter-worker pub/sub channel mesh.
///
/// Returns one `(inbox, outboxes)` pair per worker — same topology as
/// `ForwardMesh::build_channels`. Each worker's `outboxes` is a Vec of
/// cloned senders (one per worker, including itself). The worker skips
/// its own index during broadcast.
///
/// Bounded at 65536 messages per inbox. `try_send` drops on full rather
/// than blocking the reactor thread. 65536 absorbs sustained bursts
/// from a worker publishing ~50k 8 KiB frames/sec; below this the
/// inbox fills and ReliableOrdered frames get dropped.
fn build_inter_worker_pubsub_channels(
    n: usize,
) -> Vec<(
    std::sync::mpsc::Receiver<crate::layers::application::InterWorkerMsg>,
    Vec<std::sync::mpsc::SyncSender<crate::layers::application::InterWorkerMsg>>,
)> {
    let cap = 262_144;
    let channels: Vec<_> = (0..n)
        .map(|_| {
            std::sync::mpsc::sync_channel::<crate::layers::application::InterWorkerMsg>(cap)
        })
        .collect();
    let senders: Vec<_> = channels.iter().map(|(s, _)| s.clone()).collect();
    channels
        .into_iter()
        .map(|(_, rx)| (rx, senders.clone()))
        .collect()
}

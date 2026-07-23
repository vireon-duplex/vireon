//! [`QuicServer`] — top-level orchestrator that boots workers.

use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::Duration;

use crate::config::{ServerConfig, WorkerMode};
use crate::error::ServerError;
use crate::worker::{Worker, install_signal_handler};

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
        let cluster_handle = self
            .launch_cluster_transport(1)?
            .and_then(|mut h| h.pop());

        let worker = Worker::create(
            &self.config,
            0,
            1,
            dummy_rx,
            vec![dummy_tx],
            cluster_handle,
            shutdown_requested.clone(),
        )?;
        install_signal_handler(shutdown_requested)?;

        tracing::info!("starting single-worker server (core 0)");
        worker.run()
    }

    // ── Multi Worker (thread-per-core) ─────────────────────────────

    fn run_multi(&self) -> Result<(), ServerError> {
        let num_workers = self.config.num_workers();
        tracing::info!(workers = num_workers, "starting multi-worker server");

        // One shared shutdown flag for all workers + signal handler.
        let shutdown_requested = Arc::new(AtomicBool::new(false));

        // P6: launch cluster transport if configured. Returns one
        // ClusterWorkerHandle per worker (in core_id order).
        let mut cluster_handles = self
            .launch_cluster_transport(num_workers)?
            .map(std::collections::VecDeque::from);

        // Build the inter-worker forwarding mesh. Each worker gets one
        // inbox receiver + the shared sender vector. The mesh provides a
        // migration safety net: when eBPF REUSEPORT steering is
        // unavailable, the kernel hash may route a migrated client's
        // packets to the wrong worker. That worker reads the target
        // reactor_id from DCID[0..4] and forwards the packet.
        let mesh_channels = quic_engine::ForwardMesh::build_channels(num_workers as u32);

        let mut handles = Vec::with_capacity(num_workers);
        let mut stop_rx_list = Vec::with_capacity(num_workers);

        for (core_id, (inbox_rx, outboxes)) in mesh_channels.into_iter().enumerate() {
            let core_id = core_id as u32;
            // Pop the next cluster handle in order (core_id matches).
            let cluster_handle = cluster_handles.as_mut().and_then(|q| q.pop_front());
            let (handle, stop_rx) = Worker::spawn(
                self.config.clone(),
                core_id,
                num_workers as u32,
                inbox_rx,
                outboxes,
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
        // Only meaningful with >1 worker (single worker trivially receives
        // all packets). Must run AFTER all sockets are bound.
        if socket_fds.len() > 1 {
            match kernel_nic::attach_quic_reuseport_steering(socket_fds[0], socket_fds.len() as u32)
            {
                Ok(()) => {
                    tracing::info!(
                        workers = socket_fds.len(),
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
        install_signal_handler(shutdown_requested)?;

        join_all(handles)
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
    ) -> Result<Option<Vec<crate::cluster::ClusterWorkerHandle>>, ServerError> {
        let Some(cfg) = self.config.cluster.as_ref() else {
            return Ok(None);
        };
        tracing::info!(
            self_id = cfg.cluster_cfg.self_id.raw(),
            peers = cfg.peer_addrs.len(),
            workers = num_workers,
            "starting cluster transport"
        );
        let (handles, _join) = crate::cluster::launch_cluster(cfg, num_workers)
            .map_err(|e| ServerError::Config(format!("cluster transport: {e}")))?;
        // NOTE: `_join` is intentionally dropped here — the transport
        // thread exits naturally when all worker senders drop. A future
        // enhancement could join it at shutdown for a clean exit.
        Ok(Some(handles))
    }
}

/// Joins all worker threads.
fn join_all(handles: Vec<thread::JoinHandle<Result<(), ServerError>>>) -> Result<(), ServerError> {
    let mut last_err = None;

    for handle in handles {
        let name = handle.thread().name().unwrap_or("unnamed").to_string();
        match handle.join() {
            Ok(Ok(())) => {
                tracing::info!(thread = %name, "worker exited cleanly");
            }
            Ok(Err(e)) => {
                tracing::error!(thread = %name, error = %e, "worker exited with error");
                last_err = Some(e);
            }
            Err(_) => {
                tracing::error!(thread = %name, "worker thread panicked");
            }
        }
    }

    last_err.map_or(Ok(()), Err)
}

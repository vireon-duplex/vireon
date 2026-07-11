//! [`QuicServer`] — top-level orchestrator that boots workers.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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
        let worker = Worker::create(&self.config, 0, shutdown_requested.clone())?;
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

        let mut handles = Vec::with_capacity(num_workers);
        let mut stop_rx_list = Vec::with_capacity(num_workers);

        for core_id in 0..num_workers as u32 {
            let (handle, stop_rx) =
                Worker::spawn(self.config.clone(), core_id, shutdown_requested.clone())?;
            handles.push(handle);
            stop_rx_list.push(stop_rx);
        }

        // Collect each worker's stop handle (reactor's internal flag).
        let stop_handles: Vec<Arc<AtomicBool>> = stop_rx_list
            .iter()
            .filter_map(|rx| rx.recv_timeout(Duration::from_secs(5)).ok())
            .collect();

        if stop_handles.is_empty() {
            return Err(ServerError::ReactorInit(
                "no worker responded with a stop handle".into(),
            ));
        }

        tracing::info!(
            workers_ready = stop_handles.len(),
            "all workers initialised, installing signal handler"
        );

        // Signal handler sets the shared shutdown flag. Each worker's
        // VireonStack detects it in on_tick and drives graceful drain.
        let shutdown_for_handler = shutdown_requested;
        ctrlc::set_handler(move || {
            tracing::info!("received SIGINT/SIGTERM, initiating graceful shutdown...");
            shutdown_for_handler.store(true, Ordering::Release);
        })
        .map_err(|e| ServerError::SignalHandler(format!("{e}")))?;

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

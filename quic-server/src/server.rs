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
        match self.config.mode {
            WorkerMode::Single => self.run_single(),
            WorkerMode::Multi => self.run_multi(),
            WorkerMode::Distributed => self.run_distributed(),
        }
    }

    // ── Single Worker ──────────────────────────────────────────────

    fn run_single(&self) -> Result<(), ServerError> {
        let worker = Worker::create(&self.config, 0)?;
        let stop = worker.stop_handle();
        install_signal_handler(stop)?;

        tracing::info!("starting single-worker server (core 0)");
        worker.run()
    }

    // ── Multi Worker (thread-per-core) ─────────────────────────────

    fn run_multi(&self) -> Result<(), ServerError> {
        let num_workers = self.config.num_workers();
        tracing::info!(workers = num_workers, "starting multi-worker server");

        let mut handles = Vec::with_capacity(num_workers);
        let mut stop_rx_list = Vec::with_capacity(num_workers);

        for core_id in 0..num_workers as u32 {
            let (handle, stop_rx) = Worker::spawn(self.config.clone(), core_id)?;
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

        // Install signal handler that sets ALL reactor stop flags.
        ctrlc::set_handler(move || {
            tracing::info!("received SIGINT/SIGTERM, shutting down...");
            for sh in &stop_handles {
                sh.store(true, Ordering::Release);
            }
        })
        .map_err(|e| ServerError::SignalHandler(format!("{e}")))?;

        join_all(handles)
    }

    // ── Distributed (stub) ─────────────────────────────────────────

    fn run_distributed(&self) -> Result<(), ServerError> {
        tracing::info!("distributed mode — coordination layer stub");
        tracing::info!("running multi-worker base; session-registry will be wired later");
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

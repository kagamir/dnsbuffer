use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use chrono::Utc;
use tokio::sync::mpsc;

use crate::config::Config;

pub struct Runtime {
    http_listener: tokio::net::TcpListener,
    http_state: http::HttpState,
    dns_listen: std::net::SocketAddr,
    pipeline: Arc<crate::pipeline::Pipeline>,
    worker: store::StoreWorker,
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Runtime").finish_non_exhaustive()
    }
}

pub async fn build_runtime(config: &Config) -> Result<Runtime> {
    let store = store::Store::open(&config.dashboard.database_path)
        .context("failed to initialize dashboard database")?;
    store
        .cleanup(
            u64::from(config.dashboard.retention_days),
            Utc::now().timestamp_millis(),
        )
        .context("failed to clean dashboard database")?;
    let worker =
        store::StoreWorker::start(store.clone(), u64::from(config.dashboard.retention_days));
    let built = match crate::build_pipeline(config, worker.recorder()).await {
        Ok(built) => built,
        Err(error) => {
            shutdown_worker(worker).await?;
            return Err(error);
        }
    };
    let http_listener = match tokio::net::TcpListener::bind(config.dashboard.listen).await {
        Ok(listener) => listener,
        Err(error) => {
            shutdown_worker(worker).await?;
            return Err(error).with_context(|| {
                format!(
                    "failed to bind dashboard HTTP server to {}",
                    config.dashboard.listen
                )
            });
        }
    };
    Ok(Runtime {
        http_listener,
        http_state: http::HttpState {
            store: Arc::new(store),
            upstreams: built.upstream_metrics,
            retention_days: u64::from(config.dashboard.retention_days),
        },
        dns_listen: config.server.listen,
        pipeline: built.pipeline,
        worker,
    })
}

impl Runtime {
    pub async fn run(self) -> Result<()> {
        let Runtime {
            http_listener,
            http_state,
            dns_listen,
            pipeline,
            worker,
        } = self;
        tracing::info!(listen = %http_listener.local_addr()?, "dashboard HTTP server starting");
        tracing::info!(listen = %dns_listen, "DNS UDP server starting");
        let result = tokio::select! {
            result = http::serve(http_listener, http_state) => {
                result.context("dashboard HTTP server stopped")
            }
            result = crate::server::run_udp(dns_listen, pipeline) => {
                result.context("DNS UDP server stopped")
            }
        };
        shutdown_worker(worker).await?;
        result
    }
}

async fn shutdown_worker(worker: store::StoreWorker) -> Result<()> {
    tokio::task::spawn_blocking(move || worker.shutdown())
        .await
        .context("dashboard store worker shutdown task failed")
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryEvent {
    pub timestamp_ms: i64,
    pub domain: String,
    pub query_type: String,
    pub response_code: String,
    pub duration_ms: u64,
    pub blocked: bool,
    pub cache_hit: bool,
    pub response_ips: Vec<String>,
}

#[derive(Clone)]
pub struct Recorder {
    sender: mpsc::Sender<QueryEvent>,
    full_logged: Arc<AtomicBool>,
    closed_logged: Arc<AtomicBool>,
}

impl Recorder {
    pub fn channel(capacity: usize) -> (Self, mpsc::Receiver<QueryEvent>) {
        let (sender, receiver) = mpsc::channel(capacity);
        (
            Self {
                sender,
                full_logged: Arc::new(AtomicBool::new(false)),
                closed_logged: Arc::new(AtomicBool::new(false)),
            },
            receiver,
        )
    }

    pub fn disabled() -> Self {
        let (recorder, receiver) = Self::channel(1);
        drop(receiver);
        recorder
    }

    pub fn try_record(&self, event: QueryEvent) {
        match self.sender.try_send(event) {
            Ok(()) => {
                self.full_logged.store(false, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                if !self.full_logged.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        "query history queue is full; dropping events until it recovers"
                    );
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                if !self.closed_logged.swap(true, Ordering::Relaxed) {
                    tracing::warn!("query history recorder is closed; dropping events");
                }
            }
        }
    }
}

pub mod http;
pub mod store;
pub mod upstreams;

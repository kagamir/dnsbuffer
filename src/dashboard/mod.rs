use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use chrono::Utc;
use tokio::sync::mpsc;

use crate::config::Config;

const SERVICE_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(2_500);

pub struct Runtime {
    http_listener: tokio::net::TcpListener,
    http_state: http::HttpState,
    dns_socket: Arc<tokio::net::UdpSocket>,
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
        store::StoreWorker::start(store.clone(), u64::from(config.dashboard.retention_days))?;
    let built = match crate::build_pipeline(config, worker.recorder()).await {
        Ok(built) => built,
        Err(error) => {
            shutdown_worker(worker).await;
            return Err(error);
        }
    };
    let http_listener = match tokio::net::TcpListener::bind(config.dashboard.listen).await {
        Ok(listener) => listener,
        Err(error) => {
            shutdown_worker(worker).await;
            return Err(error).with_context(|| {
                format!(
                    "failed to bind dashboard HTTP server to {}",
                    config.dashboard.listen
                )
            });
        }
    };
    let dns_socket = match tokio::net::UdpSocket::bind(config.server.listen).await {
        Ok(socket) => Arc::new(socket),
        Err(error) => {
            shutdown_worker(worker).await;
            return Err(error).with_context(|| {
                format!("failed to bind DNS UDP server to {}", config.server.listen)
            });
        }
    };
    Ok(Runtime {
        http_listener,
        http_state: http::HttpState {
            store: Arc::new(store),
            upstreams: built.upstream_metrics,
            retention_days: u64::from(config.dashboard.retention_days),
            database_reads: Arc::new(tokio::sync::Semaphore::new(http::DATABASE_READ_CONCURRENCY)),
            cache_sampler: built.cache_sampler,
        },
        dns_socket,
        pipeline: built.pipeline,
        worker,
    })
}

impl Runtime {
    pub fn http_addr(&self) -> std::net::SocketAddr {
        self.http_listener
            .local_addr()
            .expect("bound HTTP listener has a local address")
    }

    pub fn dns_addr(&self) -> std::net::SocketAddr {
        self.dns_socket
            .local_addr()
            .expect("bound DNS socket has a local address")
    }

    pub async fn run(self) -> Result<()> {
        self.run_until(std::future::pending::<Result<()>>()).await
    }

    /// Runs both bound services and performs coordinated shutdown. Callers should
    /// consume a built runtime through `run` or `run_until`, rather than dropping it.
    pub async fn run_until<F>(self, external_shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = Result<()>>,
    {
        let Runtime {
            http_listener,
            http_state,
            dns_socket,
            pipeline,
            worker,
        } = self;
        let http_addr = http_listener.local_addr()?;
        let dns_addr = dns_socket.local_addr()?;
        tracing::warn!(listen = %http_addr, "dashboard HTTP server starting");
        tracing::warn!(listen = %dns_addr, "DNS UDP server starting");
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let http_shutdown = wait_for_shutdown(shutdown_rx.clone());
        let dns_shutdown = wait_for_shutdown(shutdown_rx);
        let mut http = Box::pin(http::serve_until(http_listener, http_state, http_shutdown));
        let mut dns = Box::pin(crate::server::run_udp_socket_until(
            dns_socket,
            pipeline,
            dns_shutdown,
        ));
        tokio::pin!(external_shutdown);
        enum First {
            Http(std::io::Result<()>),
            Dns(Result<()>),
            Shutdown(Result<()>),
        }
        // Service failures win over a simultaneously-ready shutdown request; HTTP
        // wins the deterministic tie if both services finish together.
        let first = tokio::select! {
            biased;
            result = &mut http => First::Http(result),
            result = &mut dns => First::Dns(result),
            result = &mut external_shutdown => First::Shutdown(result),
        };
        let _ = shutdown_tx.send(true);
        let wait_remaining = async {
            match &first {
                First::Http(_) => dns.await.map_err(|error| anyhow::anyhow!(error)),
                First::Dns(_) => http.await.map_err(|error| anyhow::anyhow!(error)),
                First::Shutdown(_) => {
                    let (http_result, dns_result) = tokio::join!(http, dns);
                    http_result?;
                    dns_result
                }
            }
        };
        match tokio::time::timeout(SERVICE_SHUTDOWN_TIMEOUT, wait_remaining).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!("service shutdown failed: {error:#}"),
            Err(_) => tracing::warn!(
                timeout_ms = SERVICE_SHUTDOWN_TIMEOUT.as_millis(),
                "service shutdown did not complete before timeout"
            ),
        }
        shutdown_worker(worker).await;
        match first {
            First::Http(result) => {
                service_result(result.map_err(anyhow::Error::from), "dashboard HTTP server")
            }
            First::Dns(result) => service_result(result, "DNS UDP server"),
            First::Shutdown(result) => result,
        }
    }
}

fn service_result(result: Result<()>, service: &str) -> Result<()> {
    match result {
        Ok(()) => anyhow::bail!("{service} stopped unexpectedly"),
        Err(error) => Err(error).with_context(|| format!("{service} stopped")),
    }
}

async fn wait_for_shutdown(mut shutdown: tokio::sync::watch::Receiver<bool>) {
    if !*shutdown.borrow() {
        let _ = shutdown.changed().await;
    }
}

async fn shutdown_worker(worker: store::StoreWorker) {
    if let Err(error) = tokio::task::spawn_blocking(move || worker.shutdown()).await {
        tracing::warn!("dashboard store worker shutdown task failed: {error}");
    }
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
    /// Name of the upstream that produced the answer, or `None` when the answer
    /// did not come from an upstream (hosts entry, local block, or cache hit).
    pub upstream: Option<String>,
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
pub mod sampler;
pub mod store;
pub mod upstreams;

#[cfg(test)]
mod tests {
    #[test]
    fn service_completion_without_shutdown_is_an_error() {
        let error = super::service_result(Ok(()), "dashboard HTTP server").unwrap_err();
        assert_eq!(
            error.to_string(),
            "dashboard HTTP server stopped unexpectedly"
        );
    }

    #[test]
    fn service_error_keeps_its_original_cause() {
        let error = super::service_result(Err(anyhow::anyhow!("socket failure")), "DNS UDP server")
            .unwrap_err();
        assert_eq!(error.to_string(), "DNS UDP server stopped");
        assert_eq!(error.root_cause().to_string(), "socket failure");
    }
}

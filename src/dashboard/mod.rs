use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

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

pub mod store;
pub mod upstreams;

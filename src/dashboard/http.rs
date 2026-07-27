use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, RawQuery, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use super::store::{QueryPage, QueryRecord, Ranking, Store, TrendBucket, TrendResponse};
use super::upstreams::UpstreamMetrics;

const DATABASE_ERROR: &str = "dashboard database unavailable";
const DATABASE_TIMEOUT: &str = "dashboard database request timed out";
pub const DATABASE_READ_CONCURRENCY: usize = 4;
const DATABASE_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug)]
enum DashboardReadError {
    Timeout,
    Database(anyhow::Error),
    Join(tokio::task::JoinError),
}

#[derive(Clone)]
pub struct HttpState {
    pub store: Arc<Store>,
    pub upstreams: UpstreamMetrics,
    pub retention_days: u64,
    pub database_reads: Arc<tokio::sync::Semaphore>,
}

pub fn router(state: HttpState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/assets/style.css", get(style))
        .route("/assets/chart.js", get(chart))
        .route("/assets/app.js", get(app))
        .route("/style.css", get(style))
        .route("/chart.js", get(chart))
        .route("/app.js", get(app))
        .route("/api/dashboard/trend", get(trend))
        .route("/api/dashboard/queries", get(queries))
        .route("/api/dashboard/rankings", get(rankings))
        .route("/api/dashboard/upstreams", get(upstreams))
        .with_state(state)
}

pub async fn serve(listener: tokio::net::TcpListener, state: HttpState) -> std::io::Result<()> {
    axum::serve(listener, router(state)).await
}

pub async fn serve_until<F>(
    listener: tokio::net::TcpListener,
    state: HttpState,
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await
}

async fn index() -> Response {
    asset(
        "text/html; charset=utf-8",
        include_str!("assets/index.html"),
    )
}

async fn style() -> Response {
    asset("text/css; charset=utf-8", include_str!("assets/style.css"))
}

async fn chart() -> Response {
    asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/chart.js"),
    )
}

async fn app() -> Response {
    asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/app.js"),
    )
}

fn asset(content_type: &'static str, body: &'static str) -> Response {
    ([(header::CONTENT_TYPE, content_type)], body).into_response()
}

async fn trend(State(state): State<HttpState>) -> Result<Json<TrendDto>, ApiError> {
    let retention_days = state.retention_days;
    let value = database_call(state.store, state.database_reads, move |store, deadline| {
        store.trend_before(retention_days, Utc::now().timestamp_millis(), deadline)
    })
    .await?;
    Ok(Json(TrendDto::try_from(value).map_err(ApiError::database)?))
}

async fn queries(
    State(state): State<HttpState>,
    RawQuery(raw_query): RawQuery,
    params: Result<Query<QueryParams>, QueryRejection>,
) -> Result<Json<QueryPageDto>, ApiError> {
    validate_query_encoding(raw_query.as_deref().unwrap_or_default())?;
    let Query(params) = params.map_err(|_| ApiError::bad_request("invalid query parameters"))?;
    let params = params.validate()?;
    let value = database_call(state.store, state.database_reads, move |store, deadline| {
        store.queries_before(
            params.page,
            params.page_size,
            params.search.as_deref(),
            deadline,
        )
    })
    .await?;
    Ok(Json(
        QueryPageDto::try_from(value).map_err(ApiError::database)?,
    ))
}

fn validate_query_encoding(query: &str) -> Result<(), ApiError> {
    let mut decoded = Vec::with_capacity(query.len());
    let bytes = query.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let encoded = bytes
                .get(index + 1..index + 3)
                .ok_or_else(|| ApiError::bad_request("invalid query parameters"))?;
            let value = u8::from_str_radix(
                std::str::from_utf8(encoded)
                    .map_err(|_| ApiError::bad_request("invalid query parameters"))?,
                16,
            )
            .map_err(|_| ApiError::bad_request("invalid query parameters"))?;
            decoded.push(value);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    std::str::from_utf8(&decoded)
        .map(|_| ())
        .map_err(|_| ApiError::bad_request("invalid query parameters"))
}

async fn rankings(State(state): State<HttpState>) -> Result<Json<Vec<Ranking>>, ApiError> {
    Ok(Json(
        database_call(state.store, state.database_reads, |store, deadline| {
            store.rankings_before(deadline)
        })
        .await?,
    ))
}

async fn upstreams(
    State(state): State<HttpState>,
) -> Json<Vec<super::upstreams::UpstreamSnapshot>> {
    Json(state.upstreams.snapshot())
}

async fn database_call<T, F>(
    store: Arc<Store>,
    permits: Arc<tokio::sync::Semaphore>,
    operation: F,
) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&Store, Instant) -> Result<T> + Send + 'static,
{
    database_operation(permits, DATABASE_REQUEST_TIMEOUT, move |deadline| {
        operation(&store, deadline).map_err(|error| {
            if let Some(sqlite) = error.downcast_ref::<rusqlite::Error>() {
                classify_sqlite_error_ref(sqlite, Instant::now() >= deadline)
            } else {
                DashboardReadError::Database(error)
            }
        })
    })
    .await
    .map_err(ApiError::from_read)
}

async fn database_operation<T, F>(
    permits: Arc<tokio::sync::Semaphore>,
    timeout: Duration,
    operation: F,
) -> Result<T, DashboardReadError>
where
    T: Send + 'static,
    F: FnOnce(Instant) -> Result<T, DashboardReadError> + Send + 'static,
{
    let deadline = Instant::now() + timeout;
    let permit = tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        permits.acquire_owned(),
    )
    .await
    .map_err(|_| DashboardReadError::Timeout)?
    .map_err(|_| DashboardReadError::Database(anyhow::anyhow!("database read limiter closed")))?;
    let mut handle = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        operation(deadline)
    });
    match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), &mut handle).await {
        Err(_) => Err(DashboardReadError::Timeout),
        Ok(Err(error)) => Err(DashboardReadError::Join(error)),
        Ok(Ok(result)) => result,
    }
}

#[cfg(test)]
fn classify_sqlite_error(error: rusqlite::Error, deadline_exhausted: bool) -> DashboardReadError {
    classify_sqlite_error_ref(&error, deadline_exhausted)
}

fn classify_sqlite_error_ref(
    error: &rusqlite::Error,
    deadline_exhausted: bool,
) -> DashboardReadError {
    let timed_out = matches!(
        error,
        rusqlite::Error::SqliteFailure(code, _)
            if deadline_exhausted
                && matches!(
                    code.code,
                    rusqlite::ErrorCode::OperationInterrupted
                        | rusqlite::ErrorCode::DatabaseBusy
                        | rusqlite::ErrorCode::DatabaseLocked
                )
    );
    if timed_out {
        DashboardReadError::Timeout
    } else {
        DashboardReadError::Database(anyhow::Error::msg(error.to_string()))
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryParams {
    page: Option<String>,
    page_size: Option<String>,
    search: Option<String>,
}

struct ValidatedQueryParams {
    page: u64,
    page_size: u64,
    search: Option<String>,
}

impl QueryParams {
    fn validate(self) -> Result<ValidatedQueryParams, ApiError> {
        let page = parse_parameter(self.page, "page", 1)?;
        if page < 1 {
            return Err(ApiError::bad_request("page must be at least 1"));
        }
        let page_size = parse_parameter(self.page_size, "page_size", 50)?;
        if !(1..=200).contains(&page_size) {
            return Err(ApiError::bad_request("page_size must be between 1 and 200"));
        }
        page.checked_sub(1)
            .and_then(|value| value.checked_mul(page_size))
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| ApiError::bad_request("page offset is out of range"))?;
        let search = self.search.map(|value| value.trim().to_owned());
        if search
            .as_ref()
            .is_some_and(|value| value.chars().count() > 253)
        {
            return Err(ApiError::bad_request(
                "search must be at most 253 characters",
            ));
        }
        Ok(ValidatedQueryParams {
            page,
            page_size,
            search,
        })
    }
}

fn parse_parameter(value: Option<String>, name: &str, default: u64) -> Result<u64, ApiError> {
    value.map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|_| ApiError::bad_request(format!("{name} must be an integer")))
    })
}

#[derive(Serialize)]
struct QueryPageDto {
    page: u64,
    page_size: u64,
    total: i64,
    records: Vec<QueryRecordDto>,
}

impl TryFrom<QueryPage> for QueryPageDto {
    type Error = anyhow::Error;

    fn try_from(value: QueryPage) -> Result<Self> {
        Ok(Self {
            page: value.page,
            page_size: value.page_size,
            total: value.total,
            records: value
                .records
                .into_iter()
                .map(QueryRecordDto::try_from)
                .collect::<Result<_>>()?,
        })
    }
}

#[derive(Serialize)]
struct QueryRecordDto {
    id: i64,
    timestamp: String,
    domain: String,
    query_type: String,
    response_code: String,
    duration_ms: u64,
    blocked: bool,
    cache_hit: bool,
    response_ips: Vec<String>,
}

impl TryFrom<QueryRecord> for QueryRecordDto {
    type Error = anyhow::Error;

    fn try_from(value: QueryRecord) -> Result<Self> {
        Ok(Self {
            id: value.id,
            timestamp: rfc3339(value.timestamp_ms)?,
            domain: value.domain,
            query_type: value.query_type,
            response_code: value.response_code,
            duration_ms: value.duration_ms,
            blocked: value.blocked,
            cache_hit: value.cache_hit,
            response_ips: value.response_ips,
        })
    }
}

#[derive(Serialize)]
struct TrendDto {
    start: String,
    end: String,
    granularity: String,
    buckets: Vec<TrendBucketDto>,
}

impl TryFrom<TrendResponse> for TrendDto {
    type Error = anyhow::Error;

    fn try_from(value: TrendResponse) -> Result<Self> {
        Ok(Self {
            start: rfc3339(value.start_ms)?,
            end: rfc3339(value.end_ms)?,
            granularity: value.granularity,
            buckets: value
                .buckets
                .into_iter()
                .map(TrendBucketDto::try_from)
                .collect::<Result<_>>()?,
        })
    }
}

#[derive(Serialize)]
struct TrendBucketDto {
    timestamp: String,
    total_queries: i64,
    blocked_queries: i64,
    cache_hits: i64,
}

impl TryFrom<TrendBucket> for TrendBucketDto {
    type Error = anyhow::Error;

    fn try_from(value: TrendBucket) -> Result<Self> {
        Ok(Self {
            timestamp: rfc3339(value.bucket_ms)?,
            total_queries: value.total_queries,
            blocked_queries: value.blocked_queries,
            cache_hits: value.cache_hits,
        })
    }
}

fn rfc3339(timestamp_ms: i64) -> Result<String> {
    Ok(DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
        .context("dashboard timestamp is out of range")?
        .to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    client_message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            client_message: message.into(),
        }
    }

    fn database(error: anyhow::Error) -> Self {
        tracing::error!("dashboard database request failed: {error:#}");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            client_message: DATABASE_ERROR.into(),
        }
    }

    fn database_timeout() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            client_message: DATABASE_TIMEOUT.into(),
        }
    }

    fn from_read(error: DashboardReadError) -> Self {
        match error {
            DashboardReadError::Timeout => Self::database_timeout(),
            DashboardReadError::Database(error) => Self::database(error),
            DashboardReadError::Join(error) => {
                Self::database(anyhow::Error::new(error).context("dashboard database task failed"))
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({ "error": self.client_message })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn database_read_limit_queues_and_times_out_the_fifth_operation() {
        let permits = Arc::new(tokio::sync::Semaphore::new(4));
        let entered = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(std::sync::Barrier::new(5));
        let mut running = Vec::new();
        for _ in 0..4 {
            let permits = permits.clone();
            let entered = entered.clone();
            let release = release.clone();
            running.push(tokio::spawn(async move {
                database_operation(permits, Duration::from_secs(1), move |_deadline| {
                    entered.fetch_add(1, Ordering::SeqCst);
                    release.wait();
                    Ok::<_, DashboardReadError>(())
                })
                .await
            }));
        }
        while entered.load(Ordering::SeqCst) < 4 {
            tokio::task::yield_now().await;
        }

        let fifth = database_operation(permits.clone(), Duration::from_millis(20), |_deadline| {
            Ok::<_, DashboardReadError>(())
        })
        .await
        .unwrap_err();

        assert!(matches!(fifth, DashboardReadError::Timeout));
        assert_eq!(permits.available_permits(), 0);
        release.wait();
        for task in running {
            task.await.unwrap().unwrap();
        }
        assert_eq!(permits.available_permits(), 4);
    }

    #[tokio::test]
    async fn database_operation_returns_at_client_deadline_but_holds_permit_until_work_ends() {
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let release = Arc::new(std::sync::Barrier::new(2));
        let worker_release = release.clone();

        let error = database_operation(permits.clone(), Duration::from_millis(20), move |_| {
            worker_release.wait();
            Ok::<_, DashboardReadError>(())
        })
        .await
        .unwrap_err();

        assert!(matches!(error, DashboardReadError::Timeout));
        assert_eq!(permits.available_permits(), 0);
        release.wait();
        tokio::time::timeout(Duration::from_secs(1), async {
            while permits.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn sqlite_error_classification_only_times_out_deadline_errors() {
        let interrupted = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_INTERRUPT),
            None,
        );
        let corrupt = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CORRUPT),
            None,
        );

        assert!(matches!(
            classify_sqlite_error(interrupted, true),
            DashboardReadError::Timeout
        ));
        assert!(matches!(
            classify_sqlite_error(corrupt, true),
            DashboardReadError::Database(_)
        ));
    }

    #[tokio::test]
    async fn blocking_task_join_failure_maps_to_database_500() {
        let error = database_operation(
            Arc::new(tokio::sync::Semaphore::new(1)),
            Duration::from_secs(1),
            |_| -> Result<(), DashboardReadError> { panic!("join failure") },
        )
        .await
        .unwrap_err();

        let api = ApiError::from_read(error);

        assert_eq!(api.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(api.client_message, DATABASE_ERROR);
    }
}

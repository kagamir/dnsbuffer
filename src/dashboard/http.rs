use std::sync::Arc;

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

#[derive(Clone)]
pub struct HttpState {
    pub store: Arc<Store>,
    pub upstreams: UpstreamMetrics,
    pub retention_days: u64,
}

pub fn router(state: HttpState) -> Router {
    Router::new()
        .route("/", get(index))
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
    let value = database_call(state.store, move |store| {
        store.trend(retention_days, Utc::now().timestamp_millis())
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
    let value = database_call(state.store, move |store| {
        store.queries(params.page, params.page_size, params.search.as_deref())
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
    Ok(Json(database_call(state.store, Store::rankings).await?))
}

async fn upstreams(
    State(state): State<HttpState>,
) -> Json<Vec<super::upstreams::UpstreamSnapshot>> {
    Json(state.upstreams.snapshot())
}

async fn database_call<T, F>(store: Arc<Store>, operation: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&Store) -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(move || operation(&store))
        .await
        .context("dashboard database task failed")
        .and_then(|result| result)
        .map_err(ApiError::database)
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

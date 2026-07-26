use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params, params_from_iter};

use super::QueryEvent;

const SCHEMA_VERSION: i64 = 1;
const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;
const MAX_TREND_BUCKETS: u64 = 10_000;
const MAX_QUERY_PAGE_SIZE: u64 = 200;

#[derive(Debug, serde::Serialize)]
pub struct TrendResponse {
    pub start_ms: i64,
    pub end_ms: i64,
    pub granularity: String,
    pub buckets: Vec<TrendBucket>,
}

#[derive(Debug, serde::Serialize)]
pub struct TrendBucket {
    pub bucket_ms: i64,
    pub total_queries: i64,
    pub blocked_queries: i64,
    pub cache_hits: i64,
}

#[derive(Debug, serde::Serialize)]
pub struct QueryPage {
    pub page: u64,
    pub page_size: u64,
    pub total: i64,
    pub records: Vec<QueryRecord>,
}

#[derive(Debug, serde::Serialize)]
pub struct QueryRecord {
    pub id: i64,
    pub timestamp_ms: i64,
    pub domain: String,
    pub query_type: String,
    pub response_code: String,
    pub duration_ms: u64,
    pub blocked: bool,
    pub cache_hit: bool,
    pub response_ips: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct Ranking {
    pub domain: String,
    pub total_queries: i64,
    pub blocked_queries: i64,
    pub cache_hits: i64,
}

#[derive(Debug)]
pub struct Store {
    path: PathBuf,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let store = Self {
            path: path.to_owned(),
        };
        let mut conn = store.connect()?;
        let version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version > SCHEMA_VERSION {
            bail!("database uses newer schema version {version}");
        }
        if version == 0 {
            let transaction = conn.transaction()?;
            transaction.execute_batch(
                "CREATE TABLE query_logs (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   timestamp_ms INTEGER NOT NULL,
                   domain TEXT NOT NULL,
                   query_type TEXT NOT NULL,
                   response_code TEXT NOT NULL,
                   duration_ms INTEGER NOT NULL CHECK(duration_ms >= 0),
                   blocked INTEGER NOT NULL CHECK(blocked IN (0,1)),
                   cache_hit INTEGER NOT NULL CHECK(cache_hit IN (0,1))
                 );
                 CREATE INDEX query_logs_time_idx ON query_logs(timestamp_ms DESC, id DESC);
                 CREATE INDEX query_logs_domain_idx ON query_logs(domain);
                 CREATE TABLE query_response_ips (
                   query_id INTEGER NOT NULL REFERENCES query_logs(id) ON DELETE CASCADE,
                   ip TEXT NOT NULL,
                   PRIMARY KEY(query_id, ip)
                 );
                 CREATE INDEX query_response_ips_ip_idx ON query_response_ips(ip);
                 CREATE TABLE query_hourly_stats (
                   bucket_ms INTEGER PRIMARY KEY,
                   total_queries INTEGER NOT NULL,
                   blocked_queries INTEGER NOT NULL,
                   cache_hits INTEGER NOT NULL
                 );
                 CREATE TABLE query_daily_stats (
                   bucket_ms INTEGER PRIMARY KEY,
                   total_queries INTEGER NOT NULL,
                   blocked_queries INTEGER NOT NULL,
                   cache_hits INTEGER NOT NULL
                 );
                 PRAGMA user_version = 1;",
            )?;
            transaction.commit()?;
        }
        Ok(store)
    }

    pub fn connect(&self) -> Result<Connection> {
        let conn = Connection::open(&self.path)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.busy_timeout(Duration::from_secs(2))?;
        Ok(conn)
    }

    pub fn insert_events(&self, events: &[QueryEvent]) -> Result<()> {
        let mut conn = self.connect()?;
        let transaction = conn.transaction()?;
        for event in events {
            transaction.execute(
                "INSERT INTO query_logs (
                   timestamp_ms, domain, query_type, response_code, duration_ms, blocked, cache_hit
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    event.timestamp_ms,
                    event.domain,
                    event.query_type,
                    event.response_code,
                    event.duration_ms,
                    event.blocked,
                    event.cache_hit,
                ],
            )?;
            let query_id = transaction.last_insert_rowid();
            for ip in &event.response_ips {
                transaction.execute(
                    "INSERT OR IGNORE INTO query_response_ips (query_id, ip) VALUES (?1, ?2)",
                    params![query_id, ip],
                )?;
            }

            let timestamp = DateTime::<Utc>::from_timestamp_millis(event.timestamp_ms)
                .context("query event timestamp is out of range")?;
            let hourly_bucket = event.timestamp_ms.div_euclid(3_600_000) * 3_600_000;
            let daily_bucket = timestamp
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .expect("midnight is a valid time")
                .and_utc()
                .timestamp_millis();
            for (table, bucket) in [
                ("query_hourly_stats", hourly_bucket),
                ("query_daily_stats", daily_bucket),
            ] {
                transaction.execute(
                    &format!(
                        "INSERT INTO {table} (
                           bucket_ms, total_queries, blocked_queries, cache_hits
                         ) VALUES (?1, 1, ?2, ?3)
                         ON CONFLICT(bucket_ms) DO UPDATE SET
                           total_queries = total_queries + 1,
                           blocked_queries = blocked_queries + excluded.blocked_queries,
                           cache_hits = cache_hits + excluded.cache_hits"
                    ),
                    params![bucket, event.blocked, event.cache_hit],
                )?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn cleanup(&self, retention_days: u64, now_ms: i64) -> Result<()> {
        if retention_days == 0 {
            return Ok(());
        }
        let retention_ms = i64::try_from(retention_days)
            .ok()
            .and_then(|days| days.checked_mul(DAY_MS))
            .context("retention period is out of range")?;
        let cutoff = now_ms
            .checked_sub(retention_ms)
            .context("retention cutoff is out of range")?;
        let mut conn = self.connect()?;
        let transaction = conn.transaction()?;
        transaction.execute("DELETE FROM query_logs WHERE timestamp_ms < ?1", [cutoff])?;
        transaction.execute(
            "DELETE FROM query_hourly_stats WHERE bucket_ms + 3600000 <= ?1",
            [cutoff],
        )?;
        transaction.execute(
            "DELETE FROM query_daily_stats WHERE bucket_ms + 86400000 <= ?1",
            [cutoff],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn trend(&self, retention_days: u64, now_ms: i64) -> Result<TrendResponse> {
        DateTime::<Utc>::from_timestamp_millis(now_ms)
            .context("trend timestamp is out of range")?;
        let (granularity, table, interval_ms, bucket_count) = match retention_days {
            0 => ("day", "query_daily_stats", DAY_MS, 30_u64),
            1..=15 => (
                "hour",
                "query_hourly_stats",
                HOUR_MS,
                retention_days
                    .checked_mul(24)
                    .context("trend range is out of range")?,
            ),
            days => ("day", "query_daily_stats", DAY_MS, days),
        };
        if bucket_count > MAX_TREND_BUCKETS {
            bail!("trend range exceeds maximum of {MAX_TREND_BUCKETS} buckets");
        }
        let end_ms = bucket_start(now_ms, interval_ms)?;
        let intervals =
            i64::try_from(bucket_count.saturating_sub(1)).context("trend range is out of range")?;
        let span_ms = intervals
            .checked_mul(interval_ms)
            .context("trend range is out of range")?;
        let start_ms = end_ms
            .checked_sub(span_ms)
            .context("trend range is out of range")?;
        let capacity = usize::try_from(bucket_count).context("trend range is too large")?;
        let conn = self.connect()?;
        let mut statement = conn.prepare(&format!(
            "SELECT bucket_ms, total_queries, blocked_queries, cache_hits
             FROM {table} WHERE bucket_ms BETWEEN ?1 AND ?2 ORDER BY bucket_ms"
        ))?;
        let mut rows = statement.query(params![start_ms, end_ms])?;
        let mut stored = Vec::new();
        while let Some(row) = rows.next()? {
            stored.push(TrendBucket {
                bucket_ms: row.get(0)?,
                total_queries: row.get(1)?,
                blocked_queries: row.get(2)?,
                cache_hits: row.get(3)?,
            });
        }

        let mut buckets = Vec::with_capacity(capacity);
        let mut stored = stored.into_iter().peekable();
        for index in 0..bucket_count {
            let offset = i64::try_from(index)
                .ok()
                .and_then(|value| value.checked_mul(interval_ms))
                .context("trend bucket is out of range")?;
            let bucket_ms = start_ms
                .checked_add(offset)
                .context("trend bucket is out of range")?;
            if stored
                .peek()
                .is_some_and(|bucket| bucket.bucket_ms == bucket_ms)
            {
                buckets.push(stored.next().expect("peeked bucket exists"));
            } else {
                buckets.push(TrendBucket {
                    bucket_ms,
                    total_queries: 0,
                    blocked_queries: 0,
                    cache_hits: 0,
                });
            }
        }
        Ok(TrendResponse {
            start_ms,
            end_ms,
            granularity: granularity.into(),
            buckets,
        })
    }

    pub fn queries(&self, page: u64, page_size: u64, search: Option<&str>) -> Result<QueryPage> {
        if !(1..=MAX_QUERY_PAGE_SIZE).contains(&page_size) {
            bail!("query page size must be between 1 and {MAX_QUERY_PAGE_SIZE}");
        }
        let page = page.max(1);
        let offset = page
            .checked_sub(1)
            .and_then(|value| value.checked_mul(page_size))
            .and_then(|value| i64::try_from(value).ok())
            .context("query page offset is out of range")?;
        let limit = i64::try_from(page_size).context("query page size is out of range")?;
        let pattern = search.map(|value| format!("%{}%", escape_like(value)));
        let filter = if pattern.is_some() {
            "WHERE domain LIKE ?1 ESCAPE '\\' COLLATE NOCASE
               OR EXISTS (
                   SELECT 1 FROM query_response_ips ip
                   WHERE ip.query_id = query_logs.id
                     AND ip.ip LIKE ?1 ESCAPE '\\' COLLATE NOCASE
               )"
        } else {
            ""
        };
        let conn = self.connect()?;
        let total_sql = format!("SELECT COUNT(*) FROM query_logs {filter}");
        let total: i64 = match &pattern {
            Some(pattern) => conn.query_row(&total_sql, [pattern], |row| row.get(0))?,
            None => conn.query_row(&total_sql, [], |row| row.get(0))?,
        };
        let list_sql = format!(
            "SELECT id, timestamp_ms, domain, query_type, response_code,
                    duration_ms, blocked, cache_hit
             FROM query_logs {filter}
             ORDER BY timestamp_ms DESC, id DESC LIMIT ? OFFSET ?"
        );
        let mut statement = conn.prepare(&list_sql)?;
        let mut records = match &pattern {
            Some(pattern) => statement
                .query_map(params![pattern, limit, offset], query_record)?
                .collect::<rusqlite::Result<Vec<_>>>()?,
            None => statement
                .query_map(params![limit, offset], query_record)?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        };

        if !records.is_empty() {
            let placeholders = (1..=records.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(",");
            let ip_sql = format!(
                "SELECT query_id, ip FROM query_response_ips
                 WHERE query_id IN ({placeholders}) ORDER BY ip"
            );
            let ids = records.iter().map(|record| record.id).collect::<Vec<_>>();
            let mut ip_statement = conn.prepare(&ip_sql)?;
            let ips = ip_statement.query_map(params_from_iter(ids), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?;
            for ip in ips {
                let (query_id, ip) = ip?;
                if let Some(record) = records.iter_mut().find(|record| record.id == query_id) {
                    record.response_ips.push(ip);
                }
            }
        }
        Ok(QueryPage {
            page,
            page_size,
            total,
            records,
        })
    }

    pub fn rankings(&self) -> Result<Vec<Ranking>> {
        let conn = self.connect()?;
        let mut statement = conn.prepare(
            "SELECT domain, COUNT(*), SUM(blocked), SUM(cache_hit)
             FROM query_logs GROUP BY domain
             ORDER BY COUNT(*) DESC, domain ASC LIMIT 20",
        )?;
        Ok(statement
            .query_map([], |row| {
                Ok(Ranking {
                    domain: row.get(0)?,
                    total_queries: row.get(1)?,
                    blocked_queries: row.get(2)?,
                    cache_hits: row.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

fn bucket_start(timestamp_ms: i64, interval_ms: i64) -> Result<i64> {
    timestamp_ms
        .div_euclid(interval_ms)
        .checked_mul(interval_ms)
        .context("timestamp bucket is out of range")
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn query_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<QueryRecord> {
    Ok(QueryRecord {
        id: row.get(0)?,
        timestamp_ms: row.get(1)?,
        domain: row.get(2)?,
        query_type: row.get(3)?,
        response_code: row.get(4)?,
        duration_ms: row.get(5)?,
        blocked: row.get(6)?,
        cache_hit: row.get(7)?,
        response_ips: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use rusqlite::Connection;

    use super::{QueryEvent, Store};

    const HOUR_MS: i64 = 3_600_000;
    const DAY_MS: i64 = 86_400_000;

    struct TestDatabase(PathBuf);

    impl TestDatabase {
        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDatabase {
        fn drop(&mut self) {
            for suffix in ["", "-shm", "-wal"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
            }
        }
    }

    fn test_store(name: &str) -> (TestDatabase, Store) {
        let path = std::env::temp_dir().join(format!(
            "dnsbuffer-{name}-{}-{}.db",
            std::process::id(),
            rand::random::<u64>()
        ));
        let guard = TestDatabase(path);
        let store = Store::open(guard.path()).unwrap();
        (guard, store)
    }

    fn scalar(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |row| row.get(0)).unwrap()
    }

    fn event(timestamp_ms: i64, domain: &str, ips: &[&str]) -> QueryEvent {
        QueryEvent {
            timestamp_ms,
            domain: domain.into(),
            query_type: "A".into(),
            response_code: "NOERROR".into(),
            duration_ms: 12,
            blocked: false,
            cache_hit: false,
            response_ips: ips.iter().map(|ip| (*ip).into()).collect(),
        }
    }

    #[test]
    fn insert_persists_log_ips_and_both_aggregates() {
        let (_guard, store) = test_store("insert");
        let event = QueryEvent {
            timestamp_ms: 1_753_488_000_000,
            domain: "example.com".into(),
            query_type: "A".into(),
            response_code: "NOERROR".into(),
            duration_ms: 12,
            blocked: false,
            cache_hit: true,
            response_ips: vec!["1.1.1.1".into(), "1.1.1.1".into(), "2606:4700::1111".into()],
        };
        store.insert_events(&[event]).unwrap();
        let conn = store.connect().unwrap();
        assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM query_logs"), 1);
        assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM query_response_ips"), 2);
        assert_eq!(
            scalar(&conn, "SELECT total_queries FROM query_hourly_stats"),
            1
        );
        assert_eq!(scalar(&conn, "SELECT cache_hits FROM query_daily_stats"), 1);
    }

    #[test]
    fn rejects_out_of_range_timestamp_without_panicking() {
        let (_guard, store) = test_store("timestamp-range");
        let event = QueryEvent {
            timestamp_ms: i64::MIN,
            domain: "example.com".into(),
            query_type: "A".into(),
            response_code: "NOERROR".into(),
            duration_ms: 12,
            blocked: false,
            cache_hit: false,
            response_ips: Vec::new(),
        };

        assert!(store.insert_events(&[event]).is_err());
    }

    #[test]
    fn persists_across_reopen_and_accumulates_shared_buckets() {
        let (guard, store) = test_store("reopen-aggregate");
        let first = QueryEvent {
            timestamp_ms: 1_753_488_000_000,
            domain: "first.example".into(),
            query_type: "A".into(),
            response_code: "NOERROR".into(),
            duration_ms: 10,
            blocked: true,
            cache_hit: false,
            response_ips: Vec::new(),
        };
        let second = QueryEvent {
            timestamp_ms: first.timestamp_ms + 1_000,
            domain: "second.example".into(),
            query_type: "AAAA".into(),
            response_code: "NOERROR".into(),
            duration_ms: 20,
            blocked: false,
            cache_hit: true,
            response_ips: Vec::new(),
        };
        store.insert_events(&[first]).unwrap();
        drop(store);

        let store = Store::open(guard.path()).unwrap();
        store.insert_events(&[second]).unwrap();
        let conn = store.connect().unwrap();
        assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM query_logs"), 2);
        assert_eq!(
            scalar(&conn, "SELECT total_queries FROM query_hourly_stats"),
            2
        );
        assert_eq!(
            scalar(&conn, "SELECT blocked_queries FROM query_daily_stats"),
            1
        );
        assert_eq!(scalar(&conn, "SELECT cache_hits FROM query_daily_stats"), 1);
    }

    #[test]
    fn rejects_database_with_future_schema_version() {
        let (guard, store) = test_store("future");
        store
            .connect()
            .unwrap()
            .pragma_update(None, "user_version", 999)
            .unwrap();
        drop(store);
        assert!(
            Store::open(guard.path())
                .unwrap_err()
                .to_string()
                .contains("newer schema")
        );
    }

    #[test]
    fn cleanup_removes_expired_logs_and_ips_but_zero_keeps_all() {
        let (_guard, store) = test_store("cleanup");
        let now = 1_753_488_000_000;
        store
            .insert_events(&[
                event(now - 8 * DAY_MS, "old.example", &["192.0.2.1"]),
                event(now, "current.example", &["192.0.2.2"]),
            ])
            .unwrap();

        store.cleanup(7, now).unwrap();
        let conn = store.connect().unwrap();
        assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM query_logs"), 1);
        assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM query_response_ips"), 1);
        assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM query_hourly_stats"), 1);
        assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM query_daily_stats"), 1);
        drop(conn);

        store.cleanup(0, i64::MAX).unwrap();
        assert_eq!(
            scalar(&store.connect().unwrap(), "SELECT COUNT(*) FROM query_logs"),
            1
        );
    }

    #[test]
    fn trend_uses_hours_through_15_days_days_after_and_30_days_for_forever() {
        let (_guard, store) = test_store("trend-granularity");
        let now: i64 = 1_753_531_212_345;

        assert_eq!(store.trend(7, now).unwrap().granularity, "hour");
        assert_eq!(store.trend(15, now).unwrap().granularity, "hour");
        assert_eq!(store.trend(16, now).unwrap().granularity, "day");
        let forever = store.trend(0, now).unwrap();
        assert_eq!(forever.granularity, "day");
        assert_eq!(
            forever.start_ms,
            now.div_euclid(DAY_MS) * DAY_MS - 29 * DAY_MS
        );
        assert_eq!(forever.buckets.len(), 30);
    }

    #[test]
    fn trend_fills_empty_buckets_and_returns_counters() {
        let (_guard, store) = test_store("trend-buckets");
        let now: i64 = 1_753_531_212_345;
        let current_hour = now.div_euclid(HOUR_MS) * HOUR_MS;
        let mut first = event(current_hour - 2 * HOUR_MS, "first.example", &[]);
        first.blocked = true;
        let mut last = event(current_hour, "last.example", &[]);
        last.cache_hit = true;
        store.insert_events(&[first, last]).unwrap();

        let trend = store.trend(1, now).unwrap();
        assert_eq!(trend.end_ms, current_hour);
        let buckets = &trend.buckets[trend.buckets.len() - 3..];
        assert_eq!(buckets[0].total_queries, 1);
        assert_eq!(buckets[0].blocked_queries, 1);
        assert_eq!(buckets[1].total_queries, 0);
        assert_eq!(buckets[2].total_queries, 1);
        assert_eq!(buckets[2].cache_hits, 1);
    }

    #[test]
    fn queries_search_domain_or_ip_without_duplicates_and_escape_wildcards() {
        let (_guard, store) = test_store("query-search");
        let now = 1_753_488_000_000;
        store
            .insert_events(&[
                event(now, "example.com", &["1.1.1.1", "1.0.0.1"]),
                event(now - 1, "literal%.com", &["2001:db8::1", "2001:db8::11"]),
                event(now - 2, "literal_.com", &[]),
                event(now - 3, r"literal\slash.com", &[]),
            ])
            .unwrap();

        let domain = store.queries(1, 50, Some("example")).unwrap();
        assert_eq!(domain.total, 1);
        assert_eq!(domain.records[0].response_ips, ["1.0.0.1", "1.1.1.1"]);
        assert_eq!(store.queries(1, 50, Some("1.1")).unwrap().total, 1);
        assert_eq!(store.queries(1, 50, Some("%")).unwrap().total, 1);
        assert_eq!(store.queries(1, 50, Some("_")).unwrap().total, 1);
        assert_eq!(store.queries(1, 50, Some(r"\")).unwrap().total, 1);
    }

    #[test]
    fn queries_use_identical_filters_for_total_and_paginated_records() {
        let (_guard, store) = test_store("query-pagination");
        let now = 1_753_488_000_000;
        store
            .insert_events(&[
                event(now, "match-one.example", &[]),
                event(now, "match-two.example", &[]),
                event(now, "other.example", &[]),
            ])
            .unwrap();

        let first = store.queries(1, 1, Some("match")).unwrap();
        let second = store.queries(2, 1, Some("match")).unwrap();
        assert_eq!((first.page, first.page_size, first.total), (1, 1, 2));
        assert_eq!(first.records.len(), 1);
        assert_eq!(second.total, 2);
        assert_eq!(second.records.len(), 1);
        assert_ne!(first.records[0].id, second.records[0].id);
    }

    #[test]
    fn queries_reject_zero_or_oversized_page_sizes() {
        let (_guard, store) = test_store("query-page-size");

        assert!(store.queries(1, 0, None).is_err());
        assert!(store.queries(1, 201, None).is_err());
        assert!(store.queries(1, 200, None).is_ok());
    }

    #[test]
    fn rankings_return_top_20_with_stable_ties_and_counters() {
        let (_guard, store) = test_store("rankings");
        let now = 1_753_488_000_000;
        let mut events = Vec::new();
        for index in 0..21 {
            events.push(event(
                now + index,
                &format!("domain-{index:02}.example"),
                &[],
            ));
        }
        let mut blocked = event(now + 100, "domain-20.example", &[]);
        blocked.blocked = true;
        let mut cached = event(now + 101, "domain-20.example", &[]);
        cached.cache_hit = true;
        events.extend([blocked, cached]);
        store.insert_events(&events).unwrap();

        let rankings = store.rankings().unwrap();
        assert_eq!(rankings.len(), 20);
        assert_eq!(rankings[0].domain, "domain-20.example");
        assert_eq!(rankings[0].total_queries, 3);
        assert_eq!(rankings[0].blocked_queries, 1);
        assert_eq!(rankings[0].cache_hits, 1);
        assert_eq!(rankings[1].domain, "domain-00.example");
        assert_eq!(rankings[19].domain, "domain-18.example");
    }

    #[test]
    fn time_boundary_inputs_return_errors_instead_of_panicking() {
        let (_guard, store) = test_store("time-boundaries");

        let cleanup = std::panic::catch_unwind(|| store.cleanup(u64::MAX, i64::MIN));
        assert!(cleanup.is_ok());
        assert!(cleanup.unwrap().is_err());

        let minimum = std::panic::catch_unwind(|| store.trend(1, i64::MIN));
        assert!(minimum.is_ok());
        assert!(minimum.unwrap().is_err());

        let maximum = std::panic::catch_unwind(|| store.trend(1, i64::MAX));
        assert!(maximum.is_ok());
        assert!(maximum.unwrap().is_err());
    }

    #[test]
    fn trend_rejects_huge_but_arithmetically_valid_ranges_without_panicking() {
        let (_guard, store) = test_store("trend-allocation-boundary");

        let result = std::panic::catch_unwind(|| store.trend(100_000, 0));
        assert!(result.is_ok());
        assert!(result.unwrap().is_err());
    }
}

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};

use super::QueryEvent;

const SCHEMA_VERSION: i64 = 1;

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
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use rusqlite::Connection;

    use super::{QueryEvent, Store};

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
}

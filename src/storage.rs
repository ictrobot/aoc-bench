// Storage layer: JSON serialization, SQLite operations, dual-storage coordination

use crate::runner::RunSeries;
use jiff::{tz::Offset, Timestamp};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::fs::TryLockError;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::NamedTempFile;
use thiserror::Error;

const RESULTS_DIR: &str = "results";
const RUNS_DIR: &str = "runs";
const LOCK_FILE: &str = ".lock";
const DB_FILE: &str = "metadata.db";
const HOST_KEY: &str = "host";
const DEFAULT_CONFIG_DIR: &str = "__default__";
const SCHEMA_SQL: &str = include_str!("../schema.sql");

/// Storage facade for run series JSON and SQLite metadata.
#[derive(Debug)]
pub struct Storage {
    data_dir: PathBuf,
    host: String,
    host_dir: PathBuf,
    runs_dir: PathBuf,
    db_path: PathBuf,
}

/// Handle to the global `.lock` file.
#[derive(Debug)]
pub struct StorageLock {
    file: File,
}

/// Row stored in the `results` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultsRow {
    pub bench: String,
    pub config: BTreeMap<String, String>,
    pub stable_series_timestamp: i64,
    pub last_series_timestamp: i64,
    pub suspicious_series_count: i64,
}

/// Storage layer errors.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("invalid host name: {host}")]
    InvalidHost { host: String },
    #[error("invalid benchmark name: {bench}")]
    InvalidBench { bench: String },
    #[error("invalid config key: {key}")]
    InvalidConfigKey { key: String },
    #[error("invalid config value for {key}: {value}")]
    InvalidConfigValue { key: String, value: String },
    #[error("config missing host key")]
    MissingHostKey,
    #[error("config host mismatch (expected {expected}, found {found})")]
    HostMismatch { expected: String, found: String },
    #[error("run statistics field {field} is not finite")]
    InvalidStatistic { field: &'static str },
    #[error("failed to lock {path:?} because another process holds it")]
    LockUnavailable { path: PathBuf },
    #[error("I/O error at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("failed to process JSON at {path:?}: {source}")]
    JsonAtPath {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
}

impl Storage {
    /// Create or open storage rooted at `data_dir` for the provided `host`.
    pub fn new(data_dir: impl AsRef<Path>, host: impl Into<String>) -> Result<Self, StorageError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir).map_err(|e| io_error(&data_dir, e))?;

        let host = host.into();
        validate_value_token(&host)
            .then_some(())
            .ok_or_else(|| StorageError::InvalidHost {
                host: host.clone(),
            })?;

        let results_dir = data_dir.join(RESULTS_DIR);
        fs::create_dir_all(&results_dir).map_err(|e| io_error(&results_dir, e))?;

        let host_dir = results_dir.join(&host);
        fs::create_dir_all(&host_dir).map_err(|e| io_error(&host_dir, e))?;

        let runs_dir = host_dir.join(RUNS_DIR);
        fs::create_dir_all(&runs_dir).map_err(|e| io_error(&runs_dir, e))?;

        let db_path = host_dir.join(DB_FILE);

        Ok(Self {
            data_dir,
            host,
            host_dir,
            runs_dir,
            db_path,
        })
    }

    /// Host identifier tied to this storage.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Acquire the process-level lock stored at `data/.lock`.
    pub fn acquire_lock(&self) -> Result<StorageLock, StorageError> {
        StorageLock::new(self.data_dir.join(LOCK_FILE))
    }

    /// Directory containing JSON run series for this host.
    pub fn runs_dir(&self) -> &Path {
        &self.runs_dir
    }

    /// Host-specific results directory (`data/results/{host}`).
    pub fn host_dir(&self) -> &Path {
        &self.host_dir
    }

    /// Compute the on-disk directory for a benchmark/config pair.
    pub fn run_series_directory(
        &self,
        bench: &str,
        config: &BTreeMap<String, String>,
    ) -> Result<PathBuf, StorageError> {
        let bench_dir = self.bench_dir_path(bench)?;
        let dir_name = encode_config_dir(config)?;
        Ok(bench_dir.join(dir_name))
    }

    /// Write the immutable JSON file for a run series using atomic rename.
    pub fn write_run_series_json(&self, series: &RunSeries) -> Result<PathBuf, StorageError> {
        self.ensure_host_matches(&series.config)?;
        let bench_dir = self.ensure_bench_dir( &series.bench)?;
        let config_dir = self.ensure_config_dir(&bench_dir, &series.config)?;
        let stem = timestamp_file_stem(series.timestamp);
        let path = config_dir.join(format!("{stem}.json"));

        let mut tmp = NamedTempFile::new_in(&config_dir).map_err(|e| io_error(&config_dir, e))?;
        serde_json::to_writer_pretty(tmp.as_file_mut(), series)
            .map_err(|source| StorageError::JsonAtPath {
                path: path.clone(),
                source,
            })?;
        tmp.as_file_mut()
            .sync_all()
            .map_err(|e| io_error(tmp.path(), e))?;
        tmp.persist(&path)
            .map_err(|e| StorageError::Io {
                path: path.clone(),
                source: e.error,
            })?;
        Ok(path)
    }

    /// Read a run series JSON file back into memory.
    pub fn read_run_series_json(path: impl AsRef<Path>) -> Result<RunSeries, StorageError> {
        let path = path.as_ref();
        let data = fs::read_to_string(path).map_err(|e| io_error(path, e))?;
        serde_json::from_str(&data).map_err(|source| StorageError::JsonAtPath {
            path: path.to_path_buf(),
            source,
        })
    }

    /// Execute a closure inside a `BEGIN IMMEDIATE` transaction.
    pub fn with_transaction<F, T>(&self, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T, StorageError>,
    {
        let mut conn = self.open_connection()?;
        let result;
        {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            result = f(&tx)?;
            tx.commit()?;
        }
        Ok(result)
    }

    /// Insert a new row into `run_series` for the given series.
    pub fn insert_run_series_row(
        &self,
        tx: &Transaction<'_>,
        series: &RunSeries,
    ) -> Result<(), StorageError> {
        self.ensure_host_matches(&series.config)?;
        let config_json = config_json_string(&series.config)?;
        let timestamp = series.timestamp.as_second();
        let mean = round_to_i64(series.median_mean_ns_per_iter, "median_mean_ns_per_iter")?;
        let ci = round_to_i64(
            series.median_ci95_half_width_ns,
            "median_ci95_half_width_ns",
        )?;

        tx.execute(
            "INSERT INTO run_series (bench, config, timestamp, mean_ns_per_iter, ci95_half_width_ns)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![&series.bench, config_json, timestamp, mean, ci],
        )?;
        Ok(())
    }

    /// Upsert the summary row in the `results` table.
    pub fn upsert_results(
        &self,
        tx: &Transaction<'_>,
        row: &ResultsRow,
    ) -> Result<(), StorageError> {
        self.ensure_host_matches(&row.config)?;
        let config_json = config_json_string(&row.config)?;
        tx.execute(
            "INSERT INTO results (bench, config, stable_series_timestamp, last_series_timestamp, suspicious_series_count)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(bench, config) DO UPDATE SET
                stable_series_timestamp = excluded.stable_series_timestamp,
                last_series_timestamp = excluded.last_series_timestamp,
                suspicious_series_count = excluded.suspicious_series_count,
                updated_at = unixepoch()",
            params![
                &row.bench,
                config_json,
                row.stable_series_timestamp,
                row.last_series_timestamp,
                row.suspicious_series_count
            ],
        )?;
        Ok(())
    }

    /// Fetch a results row for the provided `(bench, config)` pair.
    pub fn get_results_row(
        &self,
        tx: &Transaction<'_>,
        bench: &str,
        config: &BTreeMap<String, String>,
    ) -> Result<Option<ResultsRow>, StorageError> {
        self.ensure_host_matches(config)?;
        let config_json = config_json_string(config)?;
        let mut stmt = tx.prepare_cached(
            "SELECT stable_series_timestamp, last_series_timestamp, suspicious_series_count
             FROM results WHERE bench = ?1 AND config = ?2",
        )?;
        let row = stmt
            .query_row(params![bench, config_json], |row| {
                Ok(ResultsRow {
                    bench: bench.to_string(),
                    config: config.clone(),
                    stable_series_timestamp: row.get(0)?,
                    last_series_timestamp: row.get(1)?,
                    suspicious_series_count: row.get(2)?,
                })
            })
            .optional()?;
        Ok(row)
    }

    fn ensure_host_matches(&self, config: &BTreeMap<String, String>) -> Result<(), StorageError> {
        match config.get(HOST_KEY) {
            Some(value) if value == &self.host => Ok(()),
            Some(value) => Err(StorageError::HostMismatch {
                expected: self.host.clone(),
                found: value.clone(),
            }),
            None => Err(StorageError::MissingHostKey),
        }
    }

    fn ensure_bench_dir(&self, bench: &str) -> Result<PathBuf, StorageError> {
        let bench_dir = self.bench_dir_path(bench)?;
        fs::create_dir_all(&bench_dir).map_err(|e| io_error(&bench_dir, e))?;
        Ok(bench_dir)
    }

    fn ensure_config_dir(
        &self,
        bench_dir: &Path,
        config: &BTreeMap<String, String>,
    ) -> Result<PathBuf, StorageError> {
        let dir_name = encode_config_dir(config)?;
        let path = bench_dir.join(dir_name);
        fs::create_dir_all(&path).map_err(|e| io_error(&path, e))?;
        Ok(path)
    }

    fn bench_dir_path(&self, bench: &str) -> Result<PathBuf, StorageError> {
        validate_value_token(bench)
            .then_some(self.runs_dir.join(bench))
            .ok_or_else(|| StorageError::InvalidBench {
                bench: bench.to_string(),
            })
    }

    fn open_connection(&self) -> Result<Connection, StorageError> {
        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent).map_err(|e| io_error(parent, e))?;
        }
        let new_db = !self.db_path.exists();
        let conn = Connection::open(&self.db_path)?;
        conn.busy_timeout(Duration::from_secs(30))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", &true)?;
        if new_db {
            conn.execute_batch(SCHEMA_SQL)?;
        }
        Ok(conn)
    }
}

impl StorageLock {
    fn new(path: PathBuf) -> Result<Self, StorageError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| io_error(parent, e))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| io_error(&path, e))?;
        match file.try_lock() {
            Ok(()) => Ok(Self { file }),
            Err(TryLockError::WouldBlock) => Err(StorageError::LockUnavailable { path }),
            Err(TryLockError::Error(e)) => Err(io_error(&path, e)),
        }
    }
}

impl Drop for StorageLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn encode_config_dir(config: &BTreeMap<String, String>) -> Result<String, StorageError> {
    let mut parts = Vec::new();
    for (key, value) in config {
        if key == HOST_KEY {
            continue;
        }
        if !is_valid_config_key(key) {
            return Err(StorageError::InvalidConfigKey { key: key.clone() });
        }
        if !validate_value_token(value) {
            return Err(StorageError::InvalidConfigValue {
                key: key.clone(),
                value: value.clone(),
            });
        }
        parts.push(format!("{key}={value}"));
    }
    if parts.is_empty() {
        Ok(DEFAULT_CONFIG_DIR.to_string())
    } else {
        Ok(parts.join(","))
    }
}

fn config_json_string(config: &BTreeMap<String, String>) -> Result<String, StorageError> {
    if !config.contains_key(HOST_KEY) {
        return Err(StorageError::MissingHostKey);
    }
    for (key, value) in config {
        if !is_valid_config_key(key) {
            return Err(StorageError::InvalidConfigKey { key: key.clone() });
        }
        if !validate_value_token(value) {
            return Err(StorageError::InvalidConfigValue {
                key: key.clone(),
                value: value.clone(),
            });
        }
    }
    serde_json::to_string(config).map_err(StorageError::from)
}

fn round_to_i64(value: f64, field: &'static str) -> Result<i64, StorageError> {
    if !value.is_finite() {
        return Err(StorageError::InvalidStatistic { field });
    }
    if value > i64::MAX as f64 || value < i64::MIN as f64 {
        return Err(StorageError::InvalidStatistic { field });
    }
    Ok(value.round() as i64)
}

fn timestamp_file_stem(timestamp: Timestamp) -> String {
    let dt = Offset::UTC.to_datetime(timestamp);
    format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}-{minute:02}-{second:02}",
        year = dt.year(),
        month = dt.month(),
        day = dt.day(),
        hour = dt.hour(),
        minute = dt.minute(),
        second = dt.second(),
    )
}

fn validate_value_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-'))
}

fn is_valid_config_key(key: &str) -> bool {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    first.is_ascii_lowercase()
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn io_error(path: impl AsRef<Path>, source: io::Error) -> StorageError {
    StorageError::Io {
        path: path.as_ref().to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::RunResult;
    use crate::stats::{EstimationMode, Sample, StatsResult};
    use tempfile::TempDir;

    fn temp_storage(host: &str) -> (TempDir, Storage) {
        let dir = TempDir::new().unwrap();
        let storage = Storage::new(dir.path().join("data"), host).unwrap();
        (dir, storage)
    }

    fn sample_series() -> RunSeries {
        let mut config = BTreeMap::new();
        config.insert("commit".to_string(), "abc1234".to_string());
        config.insert("host".to_string(), "pi5".to_string());
        config.insert("profile".to_string(), "release".to_string());

        RunSeries {
            schema: 1,
            bench: "2015-04".to_string(),
            config,
            timestamp: Timestamp::from_second(1_700_000_000).unwrap(),
            runs: vec![RunResult {
                timestamp: Timestamp::from_second(1_700_000_001).unwrap(),
                stats: StatsResult {
                    mean_ns_per_iter: 30_000.0,
                    ci95_half_width_ns: 300.0,
                    mode: EstimationMode::PerIter,
                    intercept_ns: None,
                    outlier_count: 0,
                    temporal_correlation: 0.0,
                    samples: vec![Sample {
                        iters: 10,
                        total_ns: 300_000,
                    }],
                },
            }],
            median_mean_ns_per_iter: 30_000.0,
            median_ci95_half_width_ns: 300.0,
            checksum: None,
        }
    }

    #[test]
    fn creates_directory_layout() {
        let (_dir, storage) = temp_storage("pi5");
        assert!(storage.runs_dir().ends_with("runs"));
        assert!(storage.runs_dir().exists());
        assert!(storage
            .host_dir()
            .join(DB_FILE)
            .parent()
            .unwrap()
            .exists());
    }

    #[test]
    fn config_dir_encoding_omits_host() {
        let mut config = BTreeMap::new();
        config.insert("threads".into(), "32".into());
        config.insert("commit".into(), "abc123".into());
        config.insert("host".into(), "pi5".into());
        assert_eq!(
            encode_config_dir(&config).unwrap(),
            "commit=abc123,threads=32"
        );

        let mut host_only = BTreeMap::new();
        host_only.insert("host".into(), "pi5".into());
        assert_eq!(encode_config_dir(&host_only).unwrap(), DEFAULT_CONFIG_DIR);
    }

    #[test]
    fn lock_is_exclusive() {
        let (dir, storage) = temp_storage("pi5");
        let lock1 = storage.acquire_lock().unwrap();
        let err = storage.acquire_lock().unwrap_err();
        assert!(matches!(err, StorageError::LockUnavailable { .. }));
        drop(lock1);
        storage.acquire_lock().unwrap();
        drop(dir);
    }

    #[test]
    fn write_and_read_series_json() {
        let (_dir, storage) = temp_storage("pi5");
        let series = sample_series();
        let path = storage.write_run_series_json(&series).unwrap();
        assert!(path.exists());
        let loaded = Storage::read_run_series_json(&path).unwrap();
        assert_eq!(loaded.bench, series.bench);
        assert_eq!(loaded.median_mean_ns_per_iter, series.median_mean_ns_per_iter);
    }

    #[test]
    fn sqlite_insert_and_query() {
        let (_dir, storage) = temp_storage("pi5");
        let series = sample_series();
        let ts = series.timestamp.as_second();
        storage.write_run_series_json(&series).unwrap();
        storage
            .with_transaction(|tx| {
                storage.insert_run_series_row(tx, &series)?;
                storage.upsert_results(
                    tx,
                    &ResultsRow {
                        bench: series.bench.clone(),
                        config: series.config.clone(),
                        stable_series_timestamp: ts,
                        last_series_timestamp: ts,
                        suspicious_series_count: 0,
                    },
                )?;
                let row = storage
                    .get_results_row(tx, &series.bench, &series.config)?
                    .unwrap();
                assert_eq!(row.stable_series_timestamp, ts);
                Ok(())
            })
            .unwrap();
    }
}

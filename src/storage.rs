// Storage layer: JSON serialization, SQLite operations, dual-storage coordination

use crate::config::{BenchmarkId, Config, ConfigFile, KeyValue};
use crate::run::{RunSeries, RunSeriesDef};
use jiff::Timestamp;
use rusqlite::types::Type;
use rusqlite::{params, Connection, Error, OptionalExtension, Transaction, TransactionBehavior};
use std::fs::{self, OpenOptions};
use std::fs::{File, TryLockError};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::NamedTempFile;
use thiserror::Error;

const RESULTS_DIR: &str = "results";
const RUNS_DIR: &str = "runs";
const LOCK_FILE: &str = ".lock";
const DB_FILE: &str = "metadata.db";
const SCHEMA_SQL: &str = include_str!("../schema.sql");
const EMPTY_CONFIG_DIR: &str = "__default__";

/// Storage facade for run series JSON and SQLite metadata.
#[derive(Debug)]
pub struct Storage {
    config_file: ConfigFile,
    host: KeyValue,
    runs_dir: PathBuf,
    lock_path: PathBuf,
    db_path: PathBuf,
}

impl Storage {
    /// Create or open storage rooted at `data_dir` for the provided `host`.
    pub fn new(config_file: ConfigFile, host: &str) -> Result<Self, StorageError> {
        let host = config_file
            .host_key()
            .value_from_name(host)
            .ok_or_else(|| StorageError::UnknownHost(host.to_string()))?;

        let data_dir = config_file.data_dir();
        fs::create_dir_all(data_dir).map_err(|e| io_error(data_dir, e))?;

        let results_dir = data_dir.join(RESULTS_DIR);
        fs::create_dir_all(&results_dir).map_err(|e| io_error(&results_dir, e))?;

        let host_dir = results_dir.join(host.value_name());
        fs::create_dir_all(&host_dir).map_err(|e| io_error(&host_dir, e))?;

        let runs_dir = host_dir.join(RUNS_DIR);
        fs::create_dir_all(&runs_dir).map_err(|e| io_error(&runs_dir, e))?;

        let lock_path = data_dir.join(LOCK_FILE);
        let db_path = host_dir.join(DB_FILE);

        Ok(Self {
            config_file,
            host,
            runs_dir,
            lock_path,
            db_path,
        })
    }

    /// Config file tied to this storage.
    #[must_use]
    pub fn config_file(&self) -> &ConfigFile {
        &self.config_file
    }

    /// Host identifier tied to this storage.
    #[must_use]
    pub fn host(&self) -> &KeyValue {
        &self.host
    }

    /// Write the immutable JSON file for a run series using atomic rename.
    pub fn write_run_series_json(&self, series: &RunSeries) -> Result<PathBuf, StorageError> {
        let series_dir = self.ensure_run_series_dir(&series.bench, &series.config)?;
        let path = self.series_file(&series.bench, &series.config, series.timestamp)?;
        let mut tmp = NamedTempFile::new_in(&series_dir).map_err(|e| io_error(&series_dir, e))?;

        serde_json::to_writer_pretty(tmp.as_file_mut(), series)?;

        tmp.as_file_mut()
            .sync_all()
            .map_err(|e| io_error(tmp.path(), e))?;

        tmp.persist(&path)
            .map_err(|e| io_error(path.clone(), e.error))?;

        Ok(path)
    }

    /// Read a run series JSON file back into memory.
    pub fn read_run_series_json(
        &self,
        bench: &BenchmarkId,
        config: &Config,
        timestamp: Timestamp,
    ) -> Result<RunSeries, StorageError> {
        let path = self.series_file(bench, config, timestamp)?;
        let data = fs::read(&path).map_err(|e| io_error(&path, e))?;

        let series_def: RunSeriesDef =
            serde_json::from_slice(&data).map_err(|source| StorageError::JsonAtPath {
                path: path.clone(),
                source,
            })?;

        let series = series_def
            .try_to_run_series(&self.config_file)
            .map_err(|_| StorageError::JsonContentsMismatch {
                path: path.clone(),
                key: "config",
            })?;

        // Ensure the JSON file contains the expected path keys
        if &series.bench != bench {
            Err(StorageError::JsonContentsMismatch { path, key: "bench" })
        } else if &series.config != config {
            Err(StorageError::JsonContentsMismatch {
                path,
                key: "config",
            })
        } else if series.timestamp != timestamp {
            Err(StorageError::JsonContentsMismatch {
                path,
                key: "timestamp",
            })
        } else {
            Ok(series)
        }
    }

    /// Acquire the process-level lock stored at `data/.lock`.
    pub fn acquire_lock(&self) -> Result<StorageLock, StorageError> {
        StorageLock::new(self.lock_path.clone())
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
        let config_json = serde_json::to_string(&series.config)?;

        let mut stmt = tx.prepare_cached(
            "INSERT INTO run_series (bench, config, timestamp, mean_ns_per_iter, ci95_half_width_ns)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        stmt.execute(params![
            &series.bench.as_str(),
            config_json,
            series.timestamp.as_second(),
            series.median_mean_ns_per_iter,
            series.median_ci95_half_width_ns
        ])?;

        Ok(())
    }

    /// Upsert the summary row in the `results` table.
    pub fn upsert_results_row(
        &self,
        tx: &Transaction<'_>,
        row: &ResultsRow,
    ) -> Result<(), StorageError> {
        self.ensure_host_matches(&row.config)?;
        let config_json = serde_json::to_string(&row.config)?;

        let mut stmt = tx.prepare_cached(
        "INSERT INTO results (bench, config, stable_series_timestamp, last_series_timestamp, suspicious_series_count)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(bench, config) DO UPDATE SET
                stable_series_timestamp = excluded.stable_series_timestamp,
                last_series_timestamp = excluded.last_series_timestamp,
                suspicious_series_count = excluded.suspicious_series_count,
                updated_at = unixepoch()"
        )?;
        stmt.execute(params![
            &row.bench.as_str(),
            config_json,
            row.stable_series_timestamp.as_second(),
            row.last_series_timestamp.as_second(),
            row.suspicious_series_count,
        ])?;
        Ok(())
    }

    /// Fetch a results row for the provided `(bench, config)` pair.
    pub fn get_results_row(
        &self,
        tx: &Transaction<'_>,
        bench: BenchmarkId,
        config: Config,
    ) -> Result<Option<ResultsRow>, StorageError> {
        self.ensure_host_matches(&config)?;
        let config_json = serde_json::to_string(&config)?;

        let mut stmt = tx.prepare_cached(
            "SELECT stable_series_timestamp, last_series_timestamp, suspicious_series_count
             FROM results WHERE bench = ?1 AND config = ?2",
        )?;
        let row = stmt
            .query_row(params![bench.to_string(), config_json], |row| {
                let stable_series_timestamp = Timestamp::from_second(row.get_ref(0)?.as_i64()?)
                    .map_err(|e| Error::FromSqlConversionFailure(0, Type::Integer, e.into()))?;
                let last_series_timestamp = Timestamp::from_second(row.get_ref(1)?.as_i64()?)
                    .map_err(|e| Error::FromSqlConversionFailure(1, Type::Integer, e.into()))?;

                Ok(ResultsRow {
                    bench,
                    config,
                    stable_series_timestamp,
                    last_series_timestamp,
                    suspicious_series_count: row.get(2)?,
                })
            })
            .optional()?;
        Ok(row)
    }

    fn run_series_dir(
        &self,
        bench: &BenchmarkId,
        config: &Config,
    ) -> Result<PathBuf, StorageError> {
        let bench_dir = self.runs_dir.join(bench.as_str());

        let host_key = self.ensure_host_matches(config)?;
        let config = config.without_key(host_key.key());

        Ok(if config.is_empty() {
            bench_dir.join(EMPTY_CONFIG_DIR)
        } else {
            bench_dir.join(config.to_string())
        })
    }

    fn series_file(
        &self,
        bench: &BenchmarkId,
        config: &Config,
        timestamp: Timestamp,
    ) -> Result<PathBuf, StorageError> {
        self.ensure_host_matches(config)?;
        let run_series_dir = self.run_series_dir(bench, config)?;
        let filename = format!("{}.json", timestamp.strftime("%Y-%m-%dT%H-%M-%S"));
        Ok(run_series_dir.join(filename))
    }

    fn ensure_host_matches<'a>(&self, config: &'a Config) -> Result<&'a KeyValue, StorageError> {
        match config.get(self.config_file.host_key()) {
            Some(kv) if kv == &self.host => Ok(kv),
            Some(value) => Err(StorageError::HostMismatch {
                expected: self.host.to_string(),
                found: value.to_string(),
            }),
            None => Err(StorageError::ConfigMissingHostKey(config.to_string())),
        }
    }

    fn ensure_run_series_dir(
        &self,
        bench: &BenchmarkId,
        config: &Config,
    ) -> Result<PathBuf, StorageError> {
        let run_series_dir = self.run_series_dir(bench, config)?;
        fs::create_dir_all(&run_series_dir).map_err(|e| io_error(&run_series_dir, e))?;
        Ok(run_series_dir)
    }

    fn open_connection(&self) -> Result<Connection, StorageError> {
        let conn = Connection::open(&self.db_path)?;
        conn.busy_timeout(Duration::from_secs(30))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", true)?;
        if !conn.table_exists(Some("main"), "results")? {
            conn.execute_batch(SCHEMA_SQL)?;
        }
        Ok(conn)
    }
}

/// Row stored in the `results` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultsRow {
    pub bench: BenchmarkId,
    pub config: Config,
    pub stable_series_timestamp: Timestamp,
    pub last_series_timestamp: Timestamp,
    pub suspicious_series_count: i64,
}

/// Handle to the global `.lock` file.
#[derive(Debug)]
pub struct StorageLock {
    _file: File,
}

impl StorageLock {
    fn new(path: PathBuf) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| io_error(&path, e))?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(StorageError::LockUnavailable { path }),
            Err(TryLockError::Error(e)) => Err(io_error(&path, e)),
        }
    }
}

/// Storage layer errors.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("host '{0}' not in loaded config file")]
    UnknownHost(String),
    #[error("config missing host key: {0}")]
    ConfigMissingHostKey(String),
    #[error("config host mismatch: expected '{expected}', found '{found}'")]
    HostMismatch { expected: String, found: String },
    #[error("failed to lock '{path:?}'")]
    LockUnavailable { path: PathBuf },
    #[error("I/O error at '{path:?}': {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("failed to process JSON at '{path:?}': {source}")]
    JsonAtPath {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("JSON file at '{path:?}' does not contain expected key '{key:?}'")]
    JsonContentsMismatch { path: PathBuf, key: &'static str },
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
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
    use crate::config::{Config, ConfigFile};
    use crate::run::Run;
    use crate::stats::{EstimationMode, Sample, StatsResult};
    use tempfile::TempDir;

    fn temp_storage(host: &str) -> (TempDir, Storage) {
        let dir = TempDir::new().unwrap();
        let results_dir = dir.path().join(RESULTS_DIR);
        fs::create_dir_all(results_dir.join("pi3")).unwrap();
        fs::create_dir_all(results_dir.join("pi4")).unwrap();

        let json = r#"{
            "config_keys": {
                "build": {"values": ["generic", "native"]},
                "commit": {"values": ["abc1234", "def4567"]}
            },
            "benchmarks": []
        }"#;
        let config_file = ConfigFile::from_str(dir.path(), Some(host), json).unwrap();
        let storage = Storage::new(config_file, host).unwrap();
        (dir, storage)
    }

    fn sample_series(config: Config) -> RunSeries {
        RunSeries {
            schema: 1,
            bench: "2015-04".try_into().unwrap(),
            config,
            timestamp: Timestamp::from_second(1_700_000_000).unwrap(),
            runs: vec![Run {
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
    fn without_host_key() {
        let (_dir, storage) = temp_storage("pi5");

        let config = storage
            .config_file()
            .config_from_string("build=native,commit=abc1234")
            .unwrap();
        let series = sample_series(config.clone());
        let err = storage.write_run_series_json(&series).unwrap_err();
        assert!(matches!(err, StorageError::ConfigMissingHostKey(_)));

        let err = storage
            .read_run_series_json(&series.bench, &config, series.timestamp)
            .unwrap_err();
        assert!(matches!(err, StorageError::ConfigMissingHostKey(_)));
    }

    #[test]
    fn mismatched_host_key() {
        let (_dir, storage) = temp_storage("pi5");

        let config = storage
            .config_file()
            .config_from_string("build=native,commit=abc1234,host=pi3")
            .unwrap();
        let series = sample_series(config.clone());
        let err = storage.write_run_series_json(&series).unwrap_err();
        assert!(matches!(err, StorageError::HostMismatch { .. }));

        let err = storage
            .read_run_series_json(&series.bench, &config, series.timestamp)
            .unwrap_err();
        assert!(matches!(err, StorageError::HostMismatch { .. }));
    }

    #[test]
    fn write_and_read_series_json() {
        let (_dir, storage) = temp_storage("pi5");

        let config = storage
            .config_file()
            .config_from_string("build=native,commit=abc1234,host=pi5")
            .unwrap();
        let series = sample_series(config.clone());

        let path = storage.write_run_series_json(&series).unwrap();
        assert!(path.exists());

        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            r#"{
  "schema": 1,
  "bench": "2015-04",
  "config": {
    "build": "native",
    "commit": "abc1234",
    "host": "pi5"
  },
  "timestamp": 1700000000,
  "runs": [
    {
      "timestamp": 1700000001,
      "mean_ns_per_iter": 30000.0,
      "ci95_half_width_ns": 300.0,
      "mode": "per_iter",
      "intercept_ns": null,
      "outlier_count": 0,
      "temporal_correlation": 0.0,
      "samples": [
        {
          "iters": 10,
          "total_ns": 300000
        }
      ]
    }
  ],
  "median_mean_ns_per_iter": 30000.0,
  "median_ci95_half_width_ns": 300.0,
  "checksum": null
}"#
        );

        let loaded = storage
            .read_run_series_json(&series.bench, &config, series.timestamp)
            .unwrap();
        assert_eq!(loaded, series);
    }

    #[test]
    fn sqlite_insert_and_query() {
        let (_dir, storage) = temp_storage("pi5");
        let config = storage
            .config_file()
            .config_from_string("commit=def4567,host=pi5")
            .unwrap();
        let series = sample_series(config.clone());

        storage
            .with_transaction(|tx| {
                storage.insert_run_series_row(tx, &series)?;

                let row = ResultsRow {
                    bench: series.bench.clone(),
                    config: config.clone(),
                    stable_series_timestamp: series.timestamp,
                    last_series_timestamp: series.timestamp,
                    suspicious_series_count: 0,
                };
                storage.upsert_results_row(tx, &row)?;

                let retrieved = storage.get_results_row(tx, series.bench, config.clone())?;
                assert_eq!(retrieved, Some(row));

                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn lock_is_exclusive() {
        let (_dir, storage) = temp_storage("pi5");

        let lock1 = storage.acquire_lock().unwrap();

        let err = storage.acquire_lock().unwrap_err();
        assert!(matches!(err, StorageError::LockUnavailable { .. }));

        drop(lock1);

        storage.acquire_lock().unwrap();
    }

    #[test]
    fn test_storage_new_unknown_host() {
        let (_dir, storage) = temp_storage("pi5");

        // Try to create storage with unknown host
        let err = Storage::new(storage.config_file, "unknown-host").unwrap_err();
        assert!(matches!(err, StorageError::UnknownHost(_)));
        assert_eq!(
            err.to_string(),
            "host 'unknown-host' not in loaded config file"
        );
    }

    #[test]
    fn test_empty_config_directory_naming() {
        let (_dir, storage) = temp_storage("pi5");

        // Config with only host key should use __default__ directory
        let config = storage
            .config_file()
            .config_from_string("host=pi5")
            .unwrap();
        let series = sample_series(config.clone());

        let path = storage.write_run_series_json(&series).unwrap();
        assert!(
            path.to_string_lossy()
                .ends_with("results/pi5/runs/2015-04/__default__/2023-11-14T22-13-20.json")
        );
    }

    #[test]
    fn test_series_file_path_format() {
        let (_dir, storage) = temp_storage("pi5");

        // Check path structure
        let config = storage
            .config_file()
            .config_from_string("build=native,commit=abc1234,host=pi5")
            .unwrap();
        let series = sample_series(config.clone());
        let path = storage.write_run_series_json(&series).unwrap();
        assert!(path.to_string_lossy().ends_with(
            "results/pi5/runs/2015-04/build=native,commit=abc1234/2023-11-14T22-13-20.json"
        ));
    }

    #[test]
    fn test_read_nonexistent_series() {
        let (_dir, storage) = temp_storage("pi5");

        let config = storage
            .config_file()
            .config_from_string("build=native,host=pi5")
            .unwrap();
        let timestamp = Timestamp::from_second(1_600_000_000).unwrap();

        let err = storage
            .read_run_series_json(&"2015-04".try_into().unwrap(), &config, timestamp)
            .unwrap_err();
        assert!(matches!(err, StorageError::Io { .. }));
    }

    #[test]
    fn test_read_series_json_content_mismatch_bench() {
        let (_dir, storage) = temp_storage("pi5");

        let config = storage
            .config_file()
            .config_from_string("build=native,host=pi5")
            .unwrap();
        let series = sample_series(config.clone());

        // Write with one bench name
        let path = storage.write_run_series_json(&series).unwrap();

        // Modify the bench name in the JSON file
        let json = fs::read_to_string(&path).unwrap();
        fs::write(path, json.replace("2015-04", "2016-05").as_bytes()).unwrap();

        // Try to read expecting the original bench name (2015-04)
        let err = storage
            .read_run_series_json(&series.bench, &config, series.timestamp)
            .unwrap_err();
        assert!(matches!(err, StorageError::JsonContentsMismatch { .. }));
    }

    #[test]
    fn test_upsert_results_row_updates_existing() {
        let (_dir, storage) = temp_storage("pi5");
        let config = storage
            .config_file()
            .config_from_string("commit=def4567,host=pi5")
            .unwrap();

        storage
            .with_transaction(|tx| {
                let bench: BenchmarkId = "2015-04".try_into().unwrap();
                let timestamp1 = Timestamp::from_second(1_700_000_000).unwrap();
                let timestamp2 = Timestamp::from_second(1_700_000_100).unwrap();

                // Insert run_series rows first (foreign key requirement)
                let mut series1 = sample_series(config.clone());
                series1.timestamp = timestamp1;
                storage.insert_run_series_row(tx, &series1)?;

                let mut series2 = sample_series(config.clone());
                series2.timestamp = timestamp2;
                storage.insert_run_series_row(tx, &series2)?;

                // Insert initial results row
                let row1 = ResultsRow {
                    bench: bench.clone(),
                    config: config.clone(),
                    stable_series_timestamp: timestamp1,
                    last_series_timestamp: timestamp1,
                    suspicious_series_count: 0,
                };
                storage.upsert_results_row(tx, &row1)?;

                // Update with new data
                let row2 = ResultsRow {
                    bench: bench.clone(),
                    config: config.clone(),
                    stable_series_timestamp: timestamp1, // Keep stable
                    last_series_timestamp: timestamp2,   // Update last
                    suspicious_series_count: 2,
                };
                storage.upsert_results_row(tx, &row2)?;

                // Verify update
                let retrieved = storage.get_results_row(tx, bench, config.clone())?.unwrap();
                assert_eq!(retrieved.last_series_timestamp, timestamp2);
                assert_eq!(retrieved.suspicious_series_count, 2);

                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_get_results_row_not_found() {
        let (_dir, storage) = temp_storage("pi5");
        let config = storage
            .config_file()
            .config_from_string("commit=abc1234,host=pi5")
            .unwrap();

        storage
            .with_transaction(|tx| {
                let result =
                    storage.get_results_row(tx, "nonexistent".try_into().unwrap(), config)?;
                assert!(result.is_none());
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_transaction_rollback_on_error() {
        let (_dir, storage) = temp_storage("pi5");
        let config = storage
            .config_file()
            .config_from_string("commit=abc1234,host=pi5")
            .unwrap();
        let series = sample_series(config.clone());

        // Transaction that fails
        let result = storage.with_transaction(|tx| {
            storage.insert_run_series_row(tx, &series)?;
            // Simulate an error
            Err::<(), _>(StorageError::UnknownHost("test-error".to_string()))
        });

        assert!(result.is_err());

        // Verify nothing was committed
        storage
            .with_transaction(|tx| {
                let mut stmt = tx.prepare("SELECT COUNT(*) FROM run_series WHERE bench = ?1")?;
                let count: i64 =
                    stmt.query_row(params![series.bench.as_str()], |row| row.get(0))?;
                assert_eq!(count, 0);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_multiple_run_series_for_same_config() {
        let (_dir, storage) = temp_storage("pi5");
        let config = storage
            .config_file()
            .config_from_string("commit=abc1234,host=pi5")
            .unwrap();

        // Insert multiple series with different timestamps
        storage
            .with_transaction(|tx| {
                for i in 0..5 {
                    let mut series = sample_series(config.clone());
                    series.timestamp = Timestamp::from_second(1_700_000_000 + i * 100).unwrap();
                    storage.insert_run_series_row(tx, &series)?;
                }
                Ok(())
            })
            .unwrap();

        // Verify all were inserted
        storage
            .with_transaction(|tx| {
                let mut stmt = tx.prepare("SELECT COUNT(*) FROM run_series WHERE bench = ?1")?;
                let count: i64 = stmt.query_row(params!["2015-04"], |row| row.get(0))?;
                assert_eq!(count, 5);
                Ok(())
            })
            .unwrap();
    }
}

use crate::config::{
    Benchmark, BenchmarkId, Config, ConfigError, ConfigFile, ConfigProductIter, Key, KeyValue,
};
use crate::run::{RunSeries, RunSeriesDef};
use crate::storage::{
    PerHostStorage, ResultsRow, ResultsRowWithStats, RunSeriesRow, RunSeriesStats, Storage,
    StorageRead,
};
use jiff::Timestamp;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use rusqlite::types::{Type, ValueRef};
use rusqlite::{
    Connection, OptionalExtension, Row, ToSql, Transaction, TransactionBehavior, params,
    params_from_iter,
};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::fs::{File, TryLockError};
use std::io;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::NamedTempFile;
use tracing::{info, trace};

const RESULTS_DIR: &str = "results";
const RUNS_DIR: &str = "runs";
const LOCK_FILE: &str = ".lock";
const DB_FILE: &str = "metadata.db";
const EMPTY_CONFIG_DIR: &str = "__default__";
const CONFIG_GENERATED_COLUMNS: &[&str] = &["commit"];

static MIGRATIONS: &[&str] = &[
    include_str!("sql_migrations/00-initial-schema.sql"),
    include_str!("sql_migrations/01-results-counts.sql"),
    include_str!("sql_migrations/02-run-series-metrics.sql"),
];

/// Hybrid storage backend that stores individual run series in immutable JSON files and metadata
/// in an SQLite database.
#[derive(Debug)]
pub struct HybridDiskStorage {
    config_file: ConfigFile,
    host: KeyValue,
    runs_dir: PathBuf,
    lock_path: PathBuf,
    db_path: PathBuf,
}

impl HybridDiskStorage {
    /// Create or open storage rooted at `data_dir` for the provided `host`.
    pub fn new(config_file: ConfigFile, host: &str) -> Result<Self, HybridDiskError> {
        let host = config_file
            .host_key()
            .value_from_name(host)
            .ok_or_else(|| HybridDiskError::UnknownHost(host.to_string()))?;

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

    fn run_series_dir(
        &self,
        bench: &BenchmarkId,
        config: &Config,
    ) -> Result<PathBuf, HybridDiskError> {
        let bench_dir = self.runs_dir.join(bench.as_str());

        let host_key = self.ensure_host_matches(config)?;
        let config = config.without_key(host_key.key());

        Ok(if config.is_empty() {
            bench_dir.join(EMPTY_CONFIG_DIR)
        } else {
            config
                .iter()
                .fold(bench_dir, |dir, kv| dir.join(kv.to_string()))
        })
    }

    fn series_file(
        &self,
        bench: &BenchmarkId,
        config: &Config,
        timestamp: Timestamp,
    ) -> Result<PathBuf, HybridDiskError> {
        let run_series_dir = self.run_series_dir(bench, config)?;
        let filename = format!("{}.json", timestamp.strftime("%Y-%m-%dT%H-%M-%S"));
        Ok(run_series_dir.join(filename))
    }

    fn ensure_config_valid_for_benchmark(
        &self,
        bench: &BenchmarkId,
        config: &Config,
    ) -> Result<(), HybridDiskError> {
        if let Some(candidate) = self.config_file.benchmark_by_id(bench) {
            if candidate.valid_config(config) {
                return Ok(());
            }

            Err(HybridDiskError::BenchmarkConfigMismatch {
                bench: bench.clone(),
                config: config.to_string(),
            })
        } else {
            Err(HybridDiskError::UnknownBenchmark(bench.to_string()))
        }
    }

    // Used for full configs where the host is required
    fn ensure_host_matches<'a>(&self, config: &'a Config) -> Result<&'a KeyValue, HybridDiskError> {
        self.ensure_host_matches_if_present(config)?
            .ok_or_else(|| HybridDiskError::ConfigMissingHostKey(config.to_string()))
    }

    // Used for stats config filters where the host is optional
    fn ensure_host_matches_if_present<'a>(
        &self,
        config: &'a Config,
    ) -> Result<Option<&'a KeyValue>, HybridDiskError> {
        match config.get(self.config_file.host_key()) {
            Some(kv) if kv == &self.host => Ok(Some(kv)),
            Some(value) => Err(HybridDiskError::HostMismatch {
                expected: self.host.to_string(),
                found: value.to_string(),
            }),
            None => Ok(None),
        }
    }

    fn ensure_run_series_dir(
        &self,
        bench: &BenchmarkId,
        config: &Config,
    ) -> Result<PathBuf, HybridDiskError> {
        let run_series_dir = self.run_series_dir(bench, config)?;
        fs::create_dir_all(&run_series_dir).map_err(|e| io_error(&run_series_dir, e))?;
        Ok(run_series_dir)
    }

    fn open_connection(&self) -> Result<Connection, HybridDiskError> {
        trace!(db = ?self.db_path, "opening database connection");

        let mut conn = Connection::open(&self.db_path)?;
        conn.busy_timeout(Duration::from_secs(30))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", true)?;

        if tracing::enabled!(tracing::Level::TRACE) {
            conn.trace_v2(
                TraceEventCodes::SQLITE_TRACE_STMT,
                Some(|event| {
                    if let TraceEvent::Stmt(stmt, sql) = event {
                        let expanded_sql = stmt.expanded_sql();
                        let sql = expanded_sql.as_ref().map_or(sql, |sql| sql.as_str());
                        let sql = sql.split('\n').fold(
                            String::with_capacity(sql.len()),
                            |mut acc, line| {
                                acc.push_str(line.trim_ascii());
                                acc.push(' ');
                                acc
                            },
                        );
                        trace!(sql = sql.trim_ascii(), "running query");
                    }
                }),
            );
        }

        self.run_migrations(&mut conn)?;

        Ok(conn)
    }

    fn run_migrations(&self, conn: &mut Connection) -> Result<(), HybridDiskError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at INTEGER NOT NULL DEFAULT (unixepoch())
            );",
        )?;

        loop {
            let txn = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

            // None iff no migrations have been run yet
            // This needs to be done in the same transaction as running the migration
            let current_version: Option<usize> =
                txn.query_one("SELECT MAX(version) FROM schema_migrations", [], |row| {
                    row.get(0)
                })?;

            let next_version = current_version.map_or(0, |v| v + 1);
            let Some(&migration) = MIGRATIONS.get(next_version) else {
                return Ok(());
            };

            info!(migration = next_version, db = ?self.db_path, "apply database migration");

            if let Err(err) = txn
                .execute_batch(migration)
                .and_then(|()| {
                    txn.execute(
                        "INSERT INTO schema_migrations (version) VALUES (?1)",
                        params![next_version],
                    )
                })
                .and_then(|_| txn.commit())
            {
                return Err(HybridDiskError::MigrationError {
                    version: next_version,
                    source: err,
                });
            }
        }
    }

    fn sql_to_ts(value: ValueRef<'_>) -> rusqlite::Result<Timestamp> {
        Timestamp::from_second(value.as_i64()?)
            .map_err(|e| rusqlite::Error::FromSqlConversionFailure(2, Type::Integer, e.into()))
    }

    fn sql_to_results_row(&self, row: &Row<'_>) -> Result<ResultsRow, HybridDiskError> {
        let bench = row.get_ref("bench")?;
        let config = row.get_ref("config")?;
        let stable_series_timestamp = row.get_ref("stable_series_timestamp")?;
        let last_series_timestamp = row.get_ref("last_series_timestamp")?;
        let suspicious_count = row.get("suspicious_count")?;
        let matched_count = row.get("matched_count")?;
        let replaced_count = row.get("replaced_count")?;

        let bench_str = bench.as_str()?;
        let bench = BenchmarkId::new(bench_str)
            .map_err(|_| HybridDiskError::UnknownBenchmark(bench_str.to_string()))?;

        let config_json = config.as_str()?;
        let config_map: BTreeMap<String, String> = serde_json::from_str(config_json)?;
        let config = self
            .config_file
            .config_from_map(&config_map)?
            .with(self.host.clone());

        Ok(ResultsRow {
            bench,
            config,
            stable_series_timestamp: Self::sql_to_ts(stable_series_timestamp)?,
            last_series_timestamp: Self::sql_to_ts(last_series_timestamp)?,
            suspicious_count,
            matched_count,
            replaced_count,
        })
    }

    fn sql_bench_config_filter<'a>(
        table_name: &'static str,
        benchmark_filter: Option<&'a BenchmarkId>,
        config_filter: &'a Config,
    ) -> (String, Vec<&'a dyn ToSql>) {
        let mut condition = String::new();
        let mut binds = Vec::new();

        condition.push_str("(1=1");

        if let Some(benchmark_filter) = benchmark_filter {
            condition.push_str(" AND ");
            condition.push_str(table_name);
            condition.push_str(".bench = ?");
            binds.push(benchmark_filter.as_arc() as &dyn ToSql);
        }

        for kv in config_filter
            .iter()
            .filter(|kv| kv.key().name() != Key::HOST_KEY_NAME)
        {
            condition.push_str(" AND ");
            condition.push_str(table_name);
            if CONFIG_GENERATED_COLUMNS.contains(&kv.key().name()) {
                condition.push_str(".config_");
                condition.push_str(kv.key().name());
                condition.push_str(" = ?");
                binds.push(kv.value_name_arc() as &dyn ToSql);
            } else {
                condition.push_str(".config ->> ? = ?");
                binds.push(kv.key().name_arc() as &dyn ToSql);
                binds.push(kv.value_name_arc() as &dyn ToSql);
            }
        }

        condition.push(')');

        (condition, binds)
    }
}

impl StorageRead for HybridDiskStorage {
    type Tx<'a> = Transaction<'a>;
    type Error = HybridDiskError;

    fn read_run_series_json(
        &self,
        bench: &BenchmarkId,
        config: &Config,
        timestamp: Timestamp,
    ) -> Result<RunSeries, HybridDiskError> {
        // Don't check for valid config for benchmark when reading existing results

        let path = self.series_file(bench, config, timestamp)?;
        let data = fs::read(&path).map_err(|e| io_error(&path, e))?;

        let series_def: RunSeriesDef =
            serde_json::from_slice(&data).map_err(|source| HybridDiskError::JsonAtPath {
                path: path.clone(),
                source,
            })?;

        let mut series = series_def
            .try_to_run_series(&self.config_file)
            .map_err(|_| HybridDiskError::JsonContentsMismatch {
                path: path.clone(),
                key: "config",
            })?;

        // Reattach the current host to complete the in-memory config
        series.config = series.config.with(self.host.clone());

        // Ensure the JSON file contains the expected path keys
        if &series.bench != bench {
            Err(HybridDiskError::JsonContentsMismatch { path, key: "bench" })
        } else if &series.config != config {
            Err(HybridDiskError::JsonContentsMismatch {
                path,
                key: "config",
            })
        } else if series.timestamp != timestamp {
            Err(HybridDiskError::JsonContentsMismatch {
                path,
                key: "timestamp",
            })
        } else {
            Ok(series)
        }
    }

    fn read_transaction<F, T>(&self, f: F) -> Result<T, HybridDiskError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T, HybridDiskError>,
    {
        let mut conn = self.open_connection()?;
        let result;
        {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
            result = f(&tx)?;
            tx.rollback()?;
        }
        Ok(result)
    }

    fn get_result_with_stats(
        &self,
        tx: &Transaction<'_>,
        bench: &BenchmarkId,
        config: &Config,
    ) -> Result<Option<ResultsRowWithStats>, HybridDiskError> {
        self.ensure_host_matches(config)?;
        // Don't check for valid config for benchmark when reading existing results

        let config_json = serde_json::to_string(&config.without_host_key())?;
        let mut stmt = tx.prepare_cached(
            "SELECT r.stable_series_timestamp,
                    r.last_series_timestamp,
                    r.suspicious_count,
                    r.matched_count,
                    r.replaced_count,
                    s.run_count,
                    s.median_run_mean_ns, s.median_run_ci95_half_ns,
                    s.median_run_outlier_count, s.median_run_sample_count,
                    l.run_count,
                    l.median_run_mean_ns, l.median_run_ci95_half_ns,
                    l.median_run_outlier_count, l.median_run_sample_count
             FROM results r
             JOIN run_series s ON s.bench = r.bench AND s.config = r.config AND s.timestamp = r.stable_series_timestamp
             JOIN run_series l ON l.bench = r.bench AND l.config = r.config AND l.timestamp = r.last_series_timestamp
             WHERE r.bench = ?1 AND r.config = ?2",
        )?;

        Ok(stmt
            .query_row(params![bench.as_str(), config_json], |row| {
                Ok(ResultsRowWithStats {
                    row: ResultsRow {
                        bench: bench.clone(),
                        config: config.clone(),
                        stable_series_timestamp: Self::sql_to_ts(row.get_ref(0)?)?,
                        last_series_timestamp: Self::sql_to_ts(row.get_ref(1)?)?,
                        suspicious_count: row.get(2)?,
                        matched_count: row.get(3)?,
                        replaced_count: row.get(4)?,
                    },
                    stable_stats: RunSeriesStats {
                        run_count: row.get(5)?,
                        median_run_mean_ns: row.get(6)?,
                        median_run_ci95_half_ns: row.get(7)?,
                        median_run_outlier_count: row.get(8)?,
                        median_run_sample_count: row.get(9)?,
                    },
                    last_stats: RunSeriesStats {
                        run_count: row.get(10)?,
                        median_run_mean_ns: row.get(11)?,
                        median_run_ci95_half_ns: row.get(12)?,
                        median_run_outlier_count: row.get(13)?,
                        median_run_sample_count: row.get(14)?,
                    },
                })
            })
            .optional()?)
    }

    fn for_each_result_with_stats(
        &self,
        tx: &Self::Tx<'_>,
        benchmark_filter: Option<&BenchmarkId>,
        config_filter: &Config,
        mut f: impl FnMut(&[ResultsRowWithStats]) -> ControlFlow<()>,
    ) -> Result<(), Self::Error> {
        self.ensure_host_matches_if_present(config_filter)?;

        let mut results: Vec<ResultsRowWithStats> = Vec::new();

        // Benchmarks are batched by benchmark then sorted by config, using the current value order
        // for each key from the config file.
        // This may be different to lexicographic ordering of the configs.
        let sort_fn =
            |a: &ResultsRowWithStats, b: &ResultsRowWithStats| a.row.config.cmp(&b.row.config);

        let (condition, params) =
            Self::sql_bench_config_filter("r", benchmark_filter, config_filter);

        let mut stmt = tx.prepare_cached(&format!(
            "SELECT r.bench,
                    r.config,
                    r.stable_series_timestamp,
                    r.last_series_timestamp,
                    r.suspicious_count,
                    r.matched_count,
                    r.replaced_count,
                    s.run_count,
                    s.median_run_mean_ns,
                    s.median_run_ci95_half_ns,
                    s.median_run_outlier_count,
                    s.median_run_sample_count,
                    l.run_count,
                    l.median_run_mean_ns,
                    l.median_run_ci95_half_ns,
                    l.median_run_outlier_count,
                    l.median_run_sample_count
             FROM results r
             JOIN run_series s ON s.bench = r.bench AND s.config = r.config AND s.timestamp = r.stable_series_timestamp
             JOIN run_series l ON l.bench = r.bench AND l.config = r.config AND l.timestamp = r.last_series_timestamp
             WHERE {condition}
             ORDER BY r.bench",
        ))?;

        let mut rows_iter = stmt.query(params_from_iter(params))?;
        while let Some(row) = rows_iter.next()? {
            let Ok(base_row) = self.sql_to_results_row(row) else {
                continue;
            };

            if let Some(existing) = results.first()
                && existing.row.bench != base_row.bench
            {
                // New benchmark, flush the current batch
                results.sort_unstable_by(sort_fn);
                let control_flow = f(&results);
                results.clear();

                if let ControlFlow::Break(()) = control_flow {
                    break;
                }
            }

            let stable_stats = RunSeriesStats {
                run_count: row.get(7)?,
                median_run_mean_ns: row.get(8)?,
                median_run_ci95_half_ns: row.get(9)?,
                median_run_outlier_count: row.get(10)?,
                median_run_sample_count: row.get(11)?,
            };

            let last_stats = RunSeriesStats {
                run_count: row.get(12)?,
                median_run_mean_ns: row.get(13)?,
                median_run_ci95_half_ns: row.get(14)?,
                median_run_outlier_count: row.get(15)?,
                median_run_sample_count: row.get(16)?,
            };

            results.push(ResultsRowWithStats {
                row: base_row,
                stable_stats,
                last_stats,
            });
        }

        if !results.is_empty() {
            results.sort_unstable_by(sort_fn);
            let _ = f(&results);
        }

        Ok(())
    }

    fn for_each_run_series(
        &self,
        tx: &Self::Tx<'_>,
        benchmark: &BenchmarkId,
        mut f: impl FnMut(&[RunSeriesRow]) -> ControlFlow<()>,
    ) -> Result<(), Self::Error> {
        let mut stmt = tx.prepare_cached(
            "SELECT config, timestamp, median_run_mean_ns, median_run_ci95_half_ns, run_count
             FROM run_series
             WHERE bench = ?1
             ORDER BY timestamp",
        )?;

        let mut batch: Vec<RunSeriesRow> = Vec::new();
        let mut rows_iter = stmt.query(params![benchmark.as_str()])?;
        while let Some(row) = rows_iter.next()? {
            let config_json: &str = row.get_ref(0)?.as_str()?;
            let config_map: BTreeMap<String, String> = match serde_json::from_str(config_json) {
                Ok(m) => m,
                Err(_) => continue,
            };
            let config = match self.config_file.config_from_map(&config_map) {
                Ok(c) => c.with(self.host.clone()),
                Err(_) => continue,
            };

            batch.push(RunSeriesRow {
                config,
                timestamp: Self::sql_to_ts(row.get_ref(1)?)?,
                median_run_mean_ns: row.get(2)?,
                median_run_ci95_half_ns: row.get(3)?,
                run_count: row.get(4)?,
            });
        }

        if !batch.is_empty() {
            let _ = f(&batch);
        }

        Ok(())
    }

    fn oldest_results(
        &self,
        tx: &Transaction<'_>,
        benchmark_filter: Option<&BenchmarkId>,
        config_filter: &Config,
        limit: usize,
    ) -> Result<Vec<ResultsRow>, HybridDiskError> {
        self.ensure_host_matches_if_present(config_filter)?;

        if limit == 0 {
            return Ok(Vec::new());
        }

        let (condition, params) =
            Self::sql_bench_config_filter("results", benchmark_filter, config_filter);

        // Don't pass limit to query as there is extra filtering below to check the benchmarks and
        // configs still exist in the current config file
        let mut stmt = tx.prepare_cached(&format!(
            "SELECT *
             FROM results
             WHERE {condition}
             ORDER BY last_series_timestamp ASC",
        ))?;

        let mut rows = Vec::new();
        let mut rows_iter = stmt.query(params_from_iter(params))?;
        while let Some(row) = rows_iter.next()? {
            if let Ok(row) = self.sql_to_results_row(row)
                && let Some(benchmark) = self.config_file.benchmark_by_id(&row.bench)
                && benchmark.valid_config(&row.config)
            {
                rows.push(row);

                if rows.len() >= limit {
                    break;
                }
            }
        }

        Ok(rows)
    }

    fn missing_results(
        &self,
        tx: &Transaction<'_>,
        benchmark_filter: Option<&BenchmarkId>,
        config_filter: &Config,
        limit: usize,
    ) -> Result<Vec<(BenchmarkId, Config)>, HybridDiskError> {
        self.ensure_host_matches_if_present(config_filter)?;
        let config_filter = config_filter.without_host_key();

        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut stmt =
            tx.prepare_cached("SELECT 1 FROM results WHERE bench = ?1 AND config = ?2")?;

        self.config_file
            .benchmarks_filtered(benchmark_filter)
            .iter()
            .flat_map(Benchmark::variants)
            .flat_map(|variant| {
                match variant.config().filter(&config_filter) {
                    None => ConfigProductIter::empty(),
                    Some(product) => product.into_iter(),
                }
                .map(|c| (variant.benchmark_id(), c))
            })
            .filter_map(|(bench, config)| {
                let config_json = match serde_json::to_string(&config) {
                    Ok(json) => json,
                    Err(e) => return Some(Err(HybridDiskError::from(e))),
                };

                match stmt
                    .query_row(params![bench.as_str(), config_json], |_| Ok(()))
                    .optional()
                {
                    Ok(Some(())) => None,
                    Ok(None) => Some(Ok((bench.clone(), config))),
                    Err(e) => Some(Err(HybridDiskError::from(e))),
                }
            })
            .take(limit)
            .collect::<Result<Vec<_>, HybridDiskError>>()
    }
}

impl Storage for HybridDiskStorage {
    type Lock = FileLock;

    fn write_run_series_json(&self, mut series: RunSeries) -> Result<PathBuf, HybridDiskError> {
        self.ensure_config_valid_for_benchmark(&series.bench, &series.config)?;

        let series_dir = self.ensure_run_series_dir(&series.bench, &series.config)?;
        let path = self.series_file(&series.bench, &series.config, series.timestamp)?;
        let mut tmp = NamedTempFile::new_in(&series_dir).map_err(|e| io_error(&series_dir, e))?;

        // Persist hostless configs; storage is already split per-host.
        series.config = series.config.without_key(self.config_file.host_key());
        serde_json::to_writer_pretty(tmp.as_file_mut(), &series)?;

        tmp.as_file_mut()
            .sync_all()
            .map_err(|e| io_error(tmp.path(), e))?;

        tmp.persist(&path)
            .map_err(|e| io_error(path.clone(), e.error))?;

        Ok(path)
    }

    fn acquire_lock(&self) -> Result<FileLock, HybridDiskError> {
        FileLock::new(self.lock_path.clone())
    }

    fn write_transaction<F, T>(&self, f: F) -> Result<T, HybridDiskError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T, HybridDiskError>,
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

    fn insert_run_series(
        &self,
        tx: &Transaction<'_>,
        series: &RunSeries,
    ) -> Result<(), HybridDiskError> {
        self.ensure_host_matches(&series.config)?;
        self.ensure_config_valid_for_benchmark(&series.bench, &series.config)?;

        let config_json = serde_json::to_string(&series.config.without_host_key())?;

        let mut stmt = tx.prepare_cached(
            "INSERT INTO run_series (
                 bench, config, timestamp,
                 run_count,
                 median_run_mean_ns,
                 median_run_ci95_half_ns,
                 median_run_outlier_count,
                 median_run_sample_count
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        stmt.execute(params![
            &series.bench.as_str(),
            config_json,
            series.timestamp.as_second(),
            series.runs.len(),
            series.median_stats().mean_ns_per_iter,
            series.median_stats().ci95_half_width_ns,
            series.median_stats().outlier_count,
            series.median_stats().samples.len(),
        ])?;

        Ok(())
    }

    fn upsert_results(
        &self,
        tx: &Transaction<'_>,
        row: &ResultsRow,
    ) -> Result<(), HybridDiskError> {
        self.ensure_host_matches(&row.config)?;
        self.ensure_config_valid_for_benchmark(&row.bench, &row.config)?;

        let config_json = serde_json::to_string(&row.config.without_host_key())?;

        let mut stmt = tx.prepare_cached(
            "INSERT INTO results (bench, config, stable_series_timestamp, last_series_timestamp, suspicious_count, matched_count, replaced_count)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(bench, config) DO UPDATE SET
                updated_at = unixepoch(),
                stable_series_timestamp = excluded.stable_series_timestamp,
                last_series_timestamp = excluded.last_series_timestamp,
                suspicious_count = excluded.suspicious_count,
                matched_count = excluded.matched_count,
                replaced_count = excluded.replaced_count"
        )?;
        stmt.execute(params![
            &row.bench.as_str(),
            config_json,
            row.stable_series_timestamp.as_second(),
            row.last_series_timestamp.as_second(),
            row.suspicious_count,
            row.matched_count,
            row.replaced_count,
        ])?;
        Ok(())
    }
}

impl PerHostStorage for HybridDiskStorage {
    fn new_for_host(config_file: ConfigFile, host: &str) -> Result<Self, Self::Error> {
        Self::new(config_file, host)
    }

    fn host(&self) -> &KeyValue {
        &self.host
    }
}

/// Handle to the global `.lock` file.
#[derive(Debug)]
pub struct FileLock {
    _file: File,
}

impl FileLock {
    fn new(path: PathBuf) -> Result<Self, HybridDiskError> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| io_error(&path, e))?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(HybridDiskError::LockUnavailable { path }),
            Err(TryLockError::Error(e)) => Err(io_error(&path, e)),
        }
    }
}

/// [`HybridDiskStorage`] errors.
#[derive(Debug, thiserror::Error)]
pub enum HybridDiskError {
    #[error("host '{0}' not in loaded config file")]
    UnknownHost(String),
    #[error("benchmark '{0}' not in loaded config file")]
    UnknownBenchmark(String),
    #[error("config '{config}' is not valid for benchmark '{bench}'")]
    BenchmarkConfigMismatch { bench: BenchmarkId, config: String },
    #[error("config missing host key: {0}")]
    ConfigMissingHostKey(String),
    #[error("config host mismatch: expected '{expected}', found '{found}'")]
    HostMismatch { expected: String, found: String },
    #[error(transparent)]
    Config(#[from] ConfigError),
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
    #[error("json file at '{path:?}' does not contain expected key '{key:?}'")]
    JsonContentsMismatch { path: PathBuf, key: &'static str },
    #[error("failed to apply migration {version:?} to database: {source}")]
    MigrationError {
        version: usize,
        #[source]
        source: rusqlite::Error,
    },
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    SqlConversionFailure(#[from] rusqlite::types::FromSqlError),
}

fn io_error(path: impl AsRef<Path>, source: io::Error) -> HybridDiskError {
    HybridDiskError::Io {
        path: path.as_ref().to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BenchmarkId, Config, ConfigFile};
    use crate::run::Run;
    use crate::stats::{EstimationMode, Sample, StatsResult};
    use tempfile::TempDir;

    fn temp_storage(host: &str) -> (TempDir, HybridDiskStorage) {
        let dir = TempDir::new().unwrap();
        let results_dir = dir.path().join(RESULTS_DIR);
        fs::create_dir_all(results_dir.join("pi3")).unwrap();
        fs::create_dir_all(results_dir.join("pi4")).unwrap();

        let json = r#"{
            "config_keys": {
                "build": {"values": ["generic", "native"]},
                "commit": {"values": ["abc1234", "def4567"]},
                "opt": {"values": ["x"]}
            },
            "benchmarks": [
                {
                    "benchmark": "2015-04",
                    "command": ["run", "{build}", "{commit}"],
                    "config": {
                        "build": ["generic", "native"],
                        "commit": ["abc1234", "def4567"]
                    }
                },
                {
                    "benchmark": "empty-config",
                    "command": ["run"],
                    "config": {}
                }
            ]
        }"#;
        let config_file = ConfigFile::from_str(dir.path(), Some(host), json).unwrap();
        let storage = HybridDiskStorage::new(config_file, host).unwrap();
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
            checksum: None,
        }
    }

    fn run_series_for_bench(bench: BenchmarkId, config: Config, timestamp: Timestamp) -> RunSeries {
        let mut series = sample_series(config);
        series.bench = bench;
        series.timestamp = timestamp;
        for run in &mut series.runs {
            run.timestamp = Timestamp::from_second(timestamp.as_second() + 1).unwrap();
        }
        series
    }

    #[test]
    fn test_without_host_key() {
        let (_dir, storage) = temp_storage("pi5");

        let config = storage
            .config_file()
            .config_from_string("build=native,commit=abc1234")
            .unwrap();
        let series = sample_series(config.clone());
        let err = storage.write_run_series_json(series.clone()).unwrap_err();
        assert!(matches!(err, HybridDiskError::ConfigMissingHostKey(_)));

        let err = storage
            .read_run_series_json(&series.bench, &config, series.timestamp)
            .unwrap_err();
        assert!(matches!(err, HybridDiskError::ConfigMissingHostKey(_)));
    }

    #[test]
    fn test_mismatched_host_key() {
        let (_dir, storage) = temp_storage("pi5");

        let config = storage
            .config_file()
            .config_from_string("build=native,commit=abc1234,host=pi3")
            .unwrap();
        let series = sample_series(config.clone());
        let err = storage.write_run_series_json(series.clone()).unwrap_err();
        assert!(matches!(err, HybridDiskError::HostMismatch { .. }));

        let err = storage
            .read_run_series_json(&series.bench, &config, series.timestamp)
            .unwrap_err();
        assert!(matches!(err, HybridDiskError::HostMismatch { .. }));
    }

    #[test]
    fn test_write_and_read_series_json() {
        let (_dir, storage) = temp_storage("pi5");

        let config = storage
            .config_file()
            .config_from_string("build=native,commit=abc1234,host=pi5")
            .unwrap();
        let series = sample_series(config.clone());

        let path = storage.write_run_series_json(series.clone()).unwrap();
        assert!(path.exists());

        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            r#"{
  "schema": 1,
  "bench": "2015-04",
  "config": {
    "build": "native",
    "commit": "abc1234"
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
  "checksum": null
}"#
        );

        let loaded = storage
            .read_run_series_json(&series.bench, &config, series.timestamp)
            .unwrap();
        assert_eq!(loaded, series);
    }

    #[test]
    fn test_sqlite_insert_and_query() {
        let (_dir, storage) = temp_storage("pi5");
        let config = storage
            .config_file()
            .config_from_string("build=generic,commit=def4567,host=pi5")
            .unwrap();
        let series = sample_series(config.clone());

        storage
            .write_transaction(|tx| {
                storage.insert_run_series(tx, &series)?;

                storage.upsert_results(
                    tx,
                    &ResultsRow {
                        bench: series.bench.clone(),
                        config: config.clone(),
                        stable_series_timestamp: series.timestamp,
                        last_series_timestamp: series.timestamp,
                        suspicious_count: 0,
                        matched_count: 0,
                        replaced_count: 0,
                    },
                )?;

                let retrieved = storage.get_result_with_stats(tx, &series.bench, &config)?;
                assert!(retrieved.is_some());
                let r = retrieved.unwrap();
                assert_eq!(r.row.bench, series.bench);
                assert_eq!(r.row.config, config);
                assert_eq!(r.row.stable_series_timestamp, series.timestamp);
                assert_eq!(r.row.last_series_timestamp, series.timestamp);

                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_lock_is_exclusive() {
        let (_dir, storage) = temp_storage("pi5");

        let lock1 = storage.acquire_lock().unwrap();

        let err = storage.acquire_lock().unwrap_err();
        assert!(matches!(err, HybridDiskError::LockUnavailable { .. }));

        drop(lock1);

        storage.acquire_lock().unwrap();
    }

    #[test]
    fn test_new_unknown_host() {
        let (_dir, storage) = temp_storage("pi5");

        // Try to create storage with unknown host
        let err = HybridDiskStorage::new(storage.config_file, "unknown-host").unwrap_err();
        assert!(matches!(err, HybridDiskError::UnknownHost(_)));
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
        let mut series = sample_series(config.clone());
        series.bench = "empty-config".try_into().unwrap();

        let path = storage.write_run_series_json(series.clone()).unwrap();
        assert!(
            path.to_string_lossy()
                .ends_with("results/pi5/runs/empty-config/__default__/2023-11-14T22-13-20.json")
        );
    }

    #[test]
    fn test_invalid_config_rejected_write() {
        let (_dir, storage) = temp_storage("pi5");

        let config = storage
            .config_file()
            .config_from_string("commit=abc1234,opt=x,host=pi5")
            .unwrap();
        let series = sample_series(config.clone());

        let err = storage.write_run_series_json(series.clone()).unwrap_err();
        assert!(matches!(
            err,
            HybridDiskError::BenchmarkConfigMismatch { .. }
        ));

        let err = storage
            .read_run_series_json(&series.bench, &config, series.timestamp)
            .unwrap_err();
        assert!(matches!(err, HybridDiskError::Io { .. }));
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
        let path = storage.write_run_series_json(series.clone()).unwrap();
        assert!(path.to_string_lossy().ends_with(
            "results/pi5/runs/2015-04/build=native/commit=abc1234/2023-11-14T22-13-20.json"
        ));
    }

    #[test]
    fn test_read_nonexistent_series() {
        let (_dir, storage) = temp_storage("pi5");

        let config = storage
            .config_file()
            .config_from_string("build=native,commit=abc1234,host=pi5")
            .unwrap();
        let timestamp = Timestamp::from_second(1_600_000_000).unwrap();

        let err = storage
            .read_run_series_json(&"2015-04".try_into().unwrap(), &config, timestamp)
            .unwrap_err();
        assert!(matches!(err, HybridDiskError::Io { .. }));
    }

    #[test]
    fn test_read_series_json_content_mismatch_bench() {
        let (_dir, storage) = temp_storage("pi5");

        let config = storage
            .config_file()
            .config_from_string("build=native,commit=abc1234,host=pi5")
            .unwrap();
        let series = sample_series(config.clone());

        // Write with one bench name
        let path = storage.write_run_series_json(series.clone()).unwrap();

        // Modify the bench name in the JSON file
        let json = fs::read_to_string(&path).unwrap();
        fs::write(path, json.replace("2015-04", "2016-05").as_bytes()).unwrap();

        // Try to read expecting the original bench name (2015-04)
        let err = storage
            .read_run_series_json(&series.bench, &config, series.timestamp)
            .unwrap_err();
        assert!(matches!(err, HybridDiskError::JsonContentsMismatch { .. }));
    }

    #[test]
    fn test_upsert_results_row_updates_existing() {
        let (_dir, storage) = temp_storage("pi5");
        let config = storage
            .config_file()
            .config_from_string("build=generic,commit=def4567,host=pi5")
            .unwrap();

        storage
            .write_transaction(|tx| {
                let bench: BenchmarkId = "2015-04".try_into().unwrap();
                let timestamp1 = Timestamp::from_second(1_700_000_000).unwrap();
                let timestamp2 = Timestamp::from_second(1_700_000_100).unwrap();

                // Insert run_series rows first (foreign key requirement)
                let mut series1 = sample_series(config.clone());
                series1.timestamp = timestamp1;
                storage.insert_run_series(tx, &series1)?;

                let mut series2 = sample_series(config.clone());
                series2.timestamp = timestamp2;
                storage.insert_run_series(tx, &series2)?;

                // Insert initial results row
                let row1 = ResultsRow {
                    bench: bench.clone(),
                    config: config.clone(),
                    stable_series_timestamp: timestamp1,
                    last_series_timestamp: timestamp1,
                    suspicious_count: 0,
                    matched_count: 0,
                    replaced_count: 0,
                };
                storage.upsert_results(tx, &row1)?;

                // Update with new data
                let row2 = ResultsRow {
                    bench: bench.clone(),
                    config: config.clone(),
                    stable_series_timestamp: timestamp1, // Keep stable
                    last_series_timestamp: timestamp2,   // Update last
                    suspicious_count: 2,
                    matched_count: 0,
                    replaced_count: 0,
                };
                storage.upsert_results(tx, &row2)?;

                // Verify update
                let retrieved = storage.get_result_with_stats(tx, &bench, &config)?.unwrap();
                assert_eq!(retrieved.row.last_series_timestamp, timestamp2);
                assert_eq!(retrieved.row.suspicious_count, 2);

                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_get_results_row_not_found() {
        let (_dir, storage) = temp_storage("pi5");
        let config = storage
            .config_file()
            .config_from_string("build=generic,commit=abc1234,host=pi5")
            .unwrap();
        let bench: BenchmarkId = "2015-04".try_into().unwrap();

        storage
            .read_transaction(|tx| {
                let result = storage.get_result_with_stats(tx, &bench, &config)?;
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
            .config_from_string("build=generic,commit=abc1234,host=pi5")
            .unwrap();
        let series = sample_series(config.clone());

        // Transaction that fails
        let result = storage.write_transaction(|tx| {
            storage.insert_run_series(tx, &series)?;
            // Simulate an error
            Err::<(), _>(HybridDiskError::UnknownHost("test-error".to_string()))
        });

        assert!(result.is_err());

        // Verify nothing was committed
        storage
            .write_transaction(|tx| {
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
            .config_from_string("build=generic,commit=abc1234,host=pi5")
            .unwrap();

        // Insert multiple series with different timestamps
        storage
            .write_transaction(|tx| {
                for i in 0..5 {
                    let mut series = sample_series(config.clone());
                    series.timestamp = Timestamp::from_second(1_700_000_000 + i * 100).unwrap();
                    storage.insert_run_series(tx, &series)?;
                }
                Ok(())
            })
            .unwrap();

        // Verify all were inserted
        storage
            .read_transaction(|tx| {
                let mut stmt = tx.prepare("SELECT COUNT(*) FROM run_series WHERE bench = ?1")?;
                let count: usize = stmt.query_row(params!["2015-04"], |row| row.get(0))?;
                assert_eq!(count, 5);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_missing_results_lists_all_unseen_configs() {
        let (_dir, storage) = temp_storage("pi3");

        let missing = storage
            .read_transaction(|tx| storage.missing_results(tx, None, &Config::new(), 10))
            .unwrap();

        // 2015-04 has 2 (build) x 2 (commit) configs, empty-config has 1.
        assert_eq!(missing.len(), 5);
    }

    #[test]
    fn test_missing_results_with_existing_series() {
        let (_dir, storage) = temp_storage("pi4");

        let bench1: BenchmarkId = "2015-04".try_into().unwrap();
        let cfg1 = storage
            .config_file()
            .config_from_string("build=generic,commit=abc1234,host=pi4")
            .unwrap();

        let bench2: BenchmarkId = "empty-config".try_into().unwrap();
        let cfg2 = storage
            .config_file()
            .config_from_string("host=pi4")
            .unwrap();

        // Insert one config for each bench
        storage
            .write_transaction(|tx| {
                for (b, cfg) in [
                    (bench1.clone(), cfg1.clone()),
                    (bench2.clone(), cfg2.clone()),
                ] {
                    let series = run_series_for_bench(
                        b.clone(),
                        cfg.clone(),
                        Timestamp::from_second(10).unwrap(),
                    );
                    storage.insert_run_series(tx, &series)?;
                    storage.upsert_results(
                        tx,
                        &ResultsRow {
                            bench: b.clone(),
                            config: cfg.clone(),
                            stable_series_timestamp: series.timestamp,
                            last_series_timestamp: series.timestamp,
                            suspicious_count: 0,
                            matched_count: 0,
                            replaced_count: 0,
                        },
                    )?;
                }
                Ok(())
            })
            .unwrap();

        // 3 outstanding configs for 2015-04
        let missing = storage
            .read_transaction(|tx| storage.missing_results(tx, None, &Config::new(), 10))
            .unwrap();

        assert!(missing.iter().all(|(b, _)| b == &bench1));
        let configs: Vec<_> = missing.iter().map(|(_, c)| c.to_string()).collect();
        assert_eq!(
            configs,
            vec![
                "build=generic,commit=def4567",
                "build=native,commit=abc1234",
                "build=native,commit=def4567",
            ]
        );
    }

    #[test]
    fn test_missing_results_respects_limit() {
        let (_dir, storage) = temp_storage("pi3");

        let missing = storage
            .read_transaction(|tx| storage.missing_results(tx, None, &Config::new(), 1))
            .unwrap();

        assert_eq!(missing.len(), 1);
    }

    #[test]
    fn test_oldest_results_orders_and_limits() {
        let (_dir, storage) = temp_storage("pi3");
        let bench: BenchmarkId = "2015-04".try_into().unwrap();

        let older_config = storage
            .config_file()
            .config_from_string("build=generic,commit=abc1234,host=pi3")
            .unwrap();
        let newer_config = storage
            .config_file()
            .config_from_string("build=native,commit=abc1234,host=pi3")
            .unwrap();

        let older = ResultsRow {
            bench: bench.clone(),
            config: older_config,
            stable_series_timestamp: Timestamp::from_second(10).unwrap(),
            last_series_timestamp: Timestamp::from_second(10).unwrap(),
            suspicious_count: 0,
            matched_count: 0,
            replaced_count: 0,
        };

        let newer = ResultsRow {
            bench,
            config: newer_config,
            stable_series_timestamp: Timestamp::from_second(20).unwrap(),
            last_series_timestamp: Timestamp::from_second(20).unwrap(),
            suspicious_count: 0,
            matched_count: 0,
            replaced_count: 0,
        };

        storage
            .write_transaction(|tx| {
                let mut series_newer = sample_series(newer.config.clone());
                series_newer.timestamp = newer.last_series_timestamp;
                storage.insert_run_series(tx, &series_newer)?;

                let mut series_older = sample_series(older.config.clone());
                series_older.timestamp = older.last_series_timestamp;
                storage.insert_run_series(tx, &series_older)?;

                storage.upsert_results(tx, &newer)?;
                storage.upsert_results(tx, &older)
            })
            .unwrap();

        let results = storage
            .read_transaction(|tx| storage.oldest_results(tx, None, &Config::new(), 1))
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].last_series_timestamp.as_second(), 10);
    }

    #[test]
    fn test_oldest_results_skips_removed_benchmarks() {
        let (dir, storage) = temp_storage("pi3");
        let bench_keep: BenchmarkId = "2015-04".try_into().unwrap();
        let bench_drop: BenchmarkId = "empty-config".try_into().unwrap();

        let bench_keep_config_keep = storage
            .config_file()
            .config_from_string("build=generic,commit=abc1234,host=pi3")
            .unwrap();
        let bench_keep_config_drop = storage
            .config_file()
            .config_from_string("build=generic,commit=def4567,host=pi3")
            .unwrap();
        let bench_drop_config = storage
            .config_file()
            .config_from_string("host=pi3")
            .unwrap();

        let pairs = [
            (&bench_drop, &bench_drop_config),
            (&bench_keep, &bench_keep_config_keep),
            (&bench_keep, &bench_keep_config_drop),
        ];

        storage
            .write_transaction(|tx| {
                let mut seconds = 0;
                for &(bench, config) in &pairs {
                    seconds += 10;
                    let timestamp = Timestamp::from_second(seconds).unwrap();

                    storage.insert_run_series(
                        tx,
                        &run_series_for_bench(bench.clone(), config.clone(), timestamp),
                    )?;
                    storage.upsert_results(
                        tx,
                        &ResultsRow {
                            bench: bench.clone(),
                            config: config.clone(),
                            stable_series_timestamp: timestamp,
                            last_series_timestamp: timestamp,
                            suspicious_count: 0,
                            matched_count: 0,
                            replaced_count: 0,
                        },
                    )?;
                }
                Ok(())
            })
            .unwrap();

        let results = storage
            .read_transaction(|tx| storage.oldest_results(tx, None, &Config::new(), 10))
            .unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].bench, bench_drop);
        assert_eq!(results[0].config, bench_drop_config);
        assert_eq!(results[1].bench, bench_keep);
        assert_eq!(results[1].config, bench_keep_config_keep);
        assert_eq!(results[1].bench, bench_keep);
        assert_eq!(results[2].config, bench_keep_config_drop);

        drop(storage);

        // Drop commit=def4567 kv and empty-config benchmark
        let json = r#"{
            "config_keys": {
                "build": {"values": ["generic", "native"]},
                "commit": {"values": ["abc1234"]},
                "opt": {"values": ["x"]}
            },
            "benchmarks": [
                {
                    "benchmark": "2015-04",
                    "command": ["run", "{build}", "{commit}"],
                    "config": {
                        "build": ["generic", "native"],
                        "commit": ["abc1234"]
                    }
                }
            ]
        }"#;

        let config_file = ConfigFile::from_str(dir.path(), Some("pi3"), json).unwrap();
        let storage = HybridDiskStorage::new(config_file, "pi3").unwrap();

        let results = storage
            .read_transaction(|tx| storage.oldest_results(tx, None, &Config::new(), 10))
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].bench, bench_keep);

        // Compare after converting to string map instances as the keys now come from different
        // ConfigFile instances so don't equal directly
        assert_eq!(
            BTreeMap::from(results[0].config.clone()),
            bench_keep_config_keep.into()
        );
    }

    #[test]
    fn test_for_each_result_with_stats_batches_and_respects_break() {
        let (_dir, storage) = temp_storage("pi3");

        let bench1: BenchmarkId = "2015-04".try_into().unwrap();
        let bench2: BenchmarkId = "empty-config".try_into().unwrap();

        let cfg1 = storage
            .config_file()
            .config_from_string("build=generic,commit=abc1234,host=pi3")
            .unwrap();
        let cfg2 = storage
            .config_file()
            .config_from_string("build=native,commit=abc1234,host=pi3")
            .unwrap();
        let cfg_empty = storage
            .config_file()
            .config_from_string("host=pi3")
            .unwrap();

        // Insert two configs for bench1 and one for bench2
        storage
            .write_transaction(|tx| {
                for (b, cfg, ts) in [
                    (bench1.clone(), cfg1.clone(), 10),
                    (bench1.clone(), cfg2.clone(), 20),
                    (bench2.clone(), cfg_empty.clone(), 30),
                ] {
                    let series = run_series_for_bench(
                        b.clone(),
                        cfg.clone(),
                        Timestamp::from_second(ts).unwrap(),
                    );
                    storage.insert_run_series(tx, &series)?;
                    storage.upsert_results(
                        tx,
                        &ResultsRow {
                            bench: b.clone(),
                            config: cfg.clone(),
                            stable_series_timestamp: series.timestamp,
                            last_series_timestamp: series.timestamp,
                            suspicious_count: 0,
                            matched_count: 0,
                            replaced_count: 0,
                        },
                    )?;
                }
                Ok(())
            })
            .unwrap();

        // Collect all batches
        let mut batches: Vec<Vec<ResultsRowWithStats>> = Vec::new();
        storage
            .read_transaction(|tx| {
                storage.for_each_result_with_stats(tx, None, &Config::new(), |rows| {
                    batches.push(rows.to_vec());
                    ControlFlow::Continue(())
                })
            })
            .unwrap();

        assert_eq!(batches.len(), 2);

        // First batch should be bench1 with configs sorted
        assert!(batches[0].iter().all(|r| r.row.bench == bench1));
        let configs: Vec<_> = batches[0]
            .iter()
            .map(|r| r.row.config.to_string())
            .collect();
        assert_eq!(
            configs,
            vec![
                "build=generic,commit=abc1234,host=pi3",
                "build=native,commit=abc1234,host=pi3"
            ]
        );

        // Second batch should be bench2 with empty config
        assert!(batches[1].iter().all(|r| r.row.bench == bench2));
        assert_eq!(batches[1].len(), 1);

        // Filter with config
        let config_filter = storage
            .config_file()
            .config_from_string("build=native")
            .unwrap();
        let mut batches: Vec<Vec<ResultsRowWithStats>> = Vec::new();
        storage
            .read_transaction(|tx| {
                storage.for_each_result_with_stats(tx, None, &config_filter, |rows| {
                    batches.push(rows.to_vec());
                    ControlFlow::Continue(())
                })
            })
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[0][0].row.bench.to_string(), "2015-04");
        assert_eq!(
            batches[0][0].row.config.to_string(),
            "build=native,commit=abc1234,host=pi3"
        );

        // Break after first batch
        let mut seen = 0usize;
        storage
            .read_transaction(|tx| {
                storage.for_each_result_with_stats(tx, None, &Config::new(), |_rows| {
                    seen += 1;
                    ControlFlow::Break(())
                })
            })
            .unwrap();
        assert_eq!(seen, 1);
    }
}

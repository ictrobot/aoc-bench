use crate::config::{BenchmarkId, Config, ConfigError, ConfigFile, Key, KeyValue};
use crate::measurement::Measurement;
use crate::storage::measurement_id::MeasurementId;
use crate::storage::{
    CaseId, MeasurementHistoryRow, MeasurementRecord, MeasurementStats, PerHostStorage, ResultsRow,
    ResultsRowWithStats, Storage, StorageRead, WorkloadId, WorkloadMeta, WorkloadState,
};
use crate::workload::{GroupSpec, WorkloadIdentity};
use jiff::Timestamp;
use once_cell::unsync::OnceCell;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use rusqlite::types::{Type, ValueRef};
use rusqlite::{
    Connection, OptionalExtension, Row, ToSql, Transaction, TransactionBehavior, params,
    params_from_iter,
};
use std::cell::{RefCell, RefMut};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::fs::{File, TryLockError};
use std::io;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::NamedTempFile;
use tracing::{info, trace};

use super::measurement_file;
use super::migration::MIGRATIONS;

const RESULTS_DIR: &str = "results";
const RUNS_DIR: &str = "runs";
const LOCK_FILE: &str = ".lock";
const DB_FILE: &str = "metadata.db";
const CONFIG_GENERATED_COLUMNS: &[&str] = &["commit"];

/// Full durable identity columns loaded for collision-safe workload lookup.
struct StoredWorkloadIdentity {
    workload_id: i64,
    benchmark: String,
    executable_sha256: Option<String>,
    stdin_sha256: Option<String>,
    group_spec_json: String,
}

/// Hybrid storage backend that stores immutable measurement JSON and indexed metadata in SQLite.
#[derive(Debug)]
pub struct HybridDiskStorage {
    config_file: ConfigFile,
    host: KeyValue,
    runs_dir: PathBuf,
    lock_path: PathBuf,
    db_path: PathBuf,
    connection: OnceCell<RefCell<Connection>>,
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
            connection: OnceCell::new(),
        })
    }

    /// Config file tied to this storage.
    #[must_use]
    pub fn config_file(&self) -> &ConfigFile {
        &self.config_file
    }

    /// Execute a write transaction whose callback may return a higher-level error.
    ///
    /// Returning any error rolls the transaction back. This allows callers to keep non-database
    /// assertions inside the same atomic operation without adding those errors to the storage
    /// layer.
    pub fn try_write_transaction<F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T, E>,
        E: From<HybridDiskError>,
    {
        let mut conn = self.connection().map_err(E::from)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(HybridDiskError::from)
            .map_err(E::from)?;
        let result = f(&tx)?;
        tx.commit()
            .map_err(HybridDiskError::from)
            .map_err(E::from)?;
        Ok(result)
    }

    /// Host identifier tied to this storage.
    #[must_use]
    pub fn host(&self) -> &KeyValue {
        &self.host
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

    /// Borrow the storage's lazily initialized persistent connection.
    ///
    /// `HybridDiskStorage` is deliberately single-threaded (`RefCell` makes it `!Sync`). A nested
    /// transaction on the same storage is an architecture violation, so it panics rather than
    /// deadlocking or silently opening a connection with a different transactional view.
    fn connection(&self) -> Result<RefMut<'_, Connection>, HybridDiskError> {
        let connection = self
            .connection
            .get_or_try_init(|| self.initialize_connection().map(RefCell::new))?;
        Ok(connection
            .try_borrow_mut()
            .expect("nested transaction on the same storage connection"))
    }

    fn initialize_connection(&self) -> Result<Connection, HybridDiskError> {
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

        let mut last_applied = None;
        loop {
            let txn = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

            // None iff no migrations have been run yet
            // This needs to be done in the same transaction as running the migration
            let current_version: Option<usize> =
                txn.query_one("SELECT MAX(version) FROM schema_migrations", [], |row| {
                    row.get(0)
                })?;

            let next_version = current_version.map_or(0, |v| v + 1);
            let Some(migration) = MIGRATIONS.get(next_version) else {
                // VACUUM cannot run inside a transaction, including the read transaction used to
                // determine that the migration sequence is complete.
                drop(txn);
                if let Some(version) = last_applied {
                    info!(migration = version, db = ?self.db_path, "vacuum database after migrations");
                    conn.execute_batch("VACUUM").map_err(|source| {
                        HybridDiskError::MigrationVacuumError { version, source }
                    })?;
                }
                return Ok(());
            };

            info!(migration = next_version, db = ?self.db_path, "apply database migration");

            txn.execute_batch(migration.sql)
                .map_err(|source| HybridDiskError::MigrationError {
                    version: next_version,
                    source,
                })?;
            if let Some(apply) = migration.apply {
                apply(self, &txn).map_err(|source| HybridDiskError::RustMigrationError {
                    version: next_version,
                    source: Box::new(source),
                })?;
            }
            txn.execute(
                "INSERT INTO schema_migrations (version) VALUES (?1)",
                params![next_version],
            )
            .and_then(|_| txn.commit())
            .map_err(|source| HybridDiskError::MigrationError {
                version: next_version,
                source,
            })?;
            last_applied = Some(next_version);
        }
    }

    fn sql_to_ts(value: ValueRef<'_>) -> rusqlite::Result<Timestamp> {
        Timestamp::from_second(value.as_i64()?)
            .map_err(|e| rusqlite::Error::FromSqlConversionFailure(2, Type::Integer, e.into()))
    }

    fn sql_to_results_row(&self, row: &Row<'_>) -> Result<ResultsRow, HybridDiskError> {
        let bench = row.get_ref("bench")?;
        let config = row.get_ref("config")?;
        let stable_measurement_timestamp = row.get_ref("stable_measurement_timestamp")?;
        let last_measurement_timestamp = row.get_ref("last_measurement_timestamp")?;
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
            stable_measurement_timestamp: Self::sql_to_ts(stable_measurement_timestamp)?,
            last_measurement_timestamp: Self::sql_to_ts(last_measurement_timestamp)?,
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
            condition.push_str(".benchmark = ?");
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

    fn read_transaction<F, T>(&self, f: F) -> Result<T, HybridDiskError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T, HybridDiskError>,
    {
        let mut conn = self.connection()?;
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
            "SELECT sm.timestamp,
                    lm.timestamp,
                    w.suspicious_count,
                    w.matched_count,
                    w.replaced_count,
                    sm.run_count,
                    sm.median_run_mean_ns, sm.median_run_ci95_half_ns,
                    sm.median_run_outlier_count, sm.median_run_sample_count,
                    lm.run_count,
                    lm.median_run_mean_ns, lm.median_run_ci95_half_ns,
                    lm.median_run_outlier_count, lm.median_run_sample_count,
                    sm.measurement_id,
                    w.executable_sha256 IS NOT NULL
             FROM cases c
             JOIN workloads w ON w.workload_id = c.workload_id
             JOIN measurements sm ON sm.measurement_id = w.stable_measurement_id
             JOIN measurements lm ON lm.measurement_id = w.last_measurement_id
             WHERE c.benchmark = ?1 AND c.config = ?2",
        )?;

        Ok(stmt
            .query_row(params![bench.as_str(), config_json], |row| {
                Ok(ResultsRowWithStats {
                    row: ResultsRow {
                        bench: bench.clone(),
                        config: config.clone(),
                        stable_measurement_timestamp: Self::sql_to_ts(row.get_ref(0)?)?,
                        last_measurement_timestamp: Self::sql_to_ts(row.get_ref(1)?)?,
                        suspicious_count: row.get(2)?,
                        matched_count: row.get(3)?,
                        replaced_count: row.get(4)?,
                    },
                    stable_stats: MeasurementStats {
                        run_count: row.get(5)?,
                        median_run_mean_ns: row.get(6)?,
                        median_run_ci95_half_ns: row.get(7)?,
                        median_run_outlier_count: row.get(8)?,
                        median_run_sample_count: row.get(9)?,
                    },
                    last_stats: MeasurementStats {
                        run_count: row.get(10)?,
                        median_run_mean_ns: row.get(11)?,
                        median_run_ci95_half_ns: row.get(12)?,
                        median_run_outlier_count: row.get(13)?,
                        median_run_sample_count: row.get(14)?,
                    },
                    stable_measurement_id: row.get(15)?,
                    is_shared: row.get(16)?,
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
            Self::sql_bench_config_filter("c", benchmark_filter, config_filter);

        let mut stmt = tx.prepare_cached(&format!(
            "SELECT c.benchmark AS bench,
                    c.config AS config,
                    sm.timestamp AS stable_measurement_timestamp,
                    lm.timestamp AS last_measurement_timestamp,
                    w.suspicious_count AS suspicious_count,
                    w.matched_count AS matched_count,
                    w.replaced_count AS replaced_count,
                    sm.run_count,
                    sm.median_run_mean_ns,
                    sm.median_run_ci95_half_ns,
                    sm.median_run_outlier_count,
                    sm.median_run_sample_count,
                    lm.run_count,
                    lm.median_run_mean_ns,
                    lm.median_run_ci95_half_ns,
                    lm.median_run_outlier_count,
                    lm.median_run_sample_count,
                    sm.measurement_id,
                    w.executable_sha256 IS NOT NULL
             FROM cases c
             JOIN workloads w ON w.workload_id = c.workload_id
             JOIN measurements sm ON sm.measurement_id = w.stable_measurement_id
             JOIN measurements lm ON lm.measurement_id = w.last_measurement_id
             WHERE {condition}
             ORDER BY c.benchmark",
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

            let stable_stats = MeasurementStats {
                run_count: row.get(7)?,
                median_run_mean_ns: row.get(8)?,
                median_run_ci95_half_ns: row.get(9)?,
                median_run_outlier_count: row.get(10)?,
                median_run_sample_count: row.get(11)?,
            };

            let last_stats = MeasurementStats {
                run_count: row.get(12)?,
                median_run_mean_ns: row.get(13)?,
                median_run_ci95_half_ns: row.get(14)?,
                median_run_outlier_count: row.get(15)?,
                median_run_sample_count: row.get(16)?,
            };

            results.push(ResultsRowWithStats {
                row: base_row,
                stable_measurement_id: row.get(17)?,
                is_shared: row.get(18)?,
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

    fn for_each_measurement_history(
        &self,
        tx: &Self::Tx<'_>,
        benchmark: &BenchmarkId,
        mut f: impl FnMut(&[MeasurementHistoryRow]) -> ControlFlow<()>,
    ) -> Result<(), Self::Error> {
        let mut stmt = tx.prepare_cached(
            "SELECT c.config, m.measurement_id, w.executable_sha256 IS NOT NULL,
                    m.timestamp, m.median_run_mean_ns, m.median_run_ci95_half_ns, m.run_count
             FROM measurement_cases mc
             JOIN measurements m ON m.measurement_id = mc.measurement_id
             JOIN workloads w ON w.workload_id = m.workload_id
             JOIN cases c ON c.case_id = mc.case_id
             WHERE c.benchmark = ?1
             ORDER BY m.timestamp, m.measurement_id",
        )?;

        let mut batch: Vec<MeasurementHistoryRow> = Vec::new();
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

            batch.push(MeasurementHistoryRow {
                config,
                measurement_id: row.get(1)?,
                is_shared: row.get(2)?,
                timestamp: Self::sql_to_ts(row.get_ref(3)?)?,
                median_run_mean_ns: row.get(4)?,
                median_run_ci95_half_ns: row.get(5)?,
                run_count: row.get(6)?,
            });
        }

        if !batch.is_empty() {
            let _ = f(&batch);
        }

        Ok(())
    }
}

impl Storage for HybridDiskStorage {
    type Lock = FileLock;

    fn acquire_lock(&self) -> Result<FileLock, HybridDiskError> {
        FileLock::new(self.lock_path.clone())
    }

    fn write_transaction<F, T>(&self, f: F) -> Result<T, HybridDiskError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T, HybridDiskError>,
    {
        self.try_write_transaction(f)
    }

    fn write_measurement_json(
        &self,
        measurement: &Measurement,
    ) -> Result<PathBuf, HybridDiskError> {
        let path = measurement_file::current_path(&self.runs_dir, measurement.measurement_id);
        let dir = path
            .parent()
            .expect("measurement path always has a parent directory");
        fs::create_dir_all(dir).map_err(|e| io_error(dir, e))?;

        let mut tmp = NamedTempFile::new_in(dir).map_err(|e| io_error(dir, e))?;
        serde_json::to_writer_pretty(tmp.as_file_mut(), measurement)?;
        tmp.as_file_mut()
            .sync_all()
            .map_err(|e| io_error(tmp.path(), e))?;
        tmp.persist(&path)
            .map_err(|e| io_error(path.clone(), e.error))?;

        Ok(path)
    }

    fn get_or_create_case(
        &self,
        tx: &Transaction<'_>,
        benchmark: &str,
        config_json: &str,
    ) -> Result<CaseId, HybridDiskError> {
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO cases (benchmark, config) VALUES (?1, ?2)",
            )?;
            stmt.execute(params![benchmark, config_json])?;
        }
        let mut stmt =
            tx.prepare_cached("SELECT case_id FROM cases WHERE benchmark = ?1 AND config = ?2")?;
        let id: i64 = stmt.query_row(params![benchmark, config_json], |row| row.get(0))?;
        Ok(CaseId(id))
    }

    fn find_case_id(
        &self,
        tx: &Transaction<'_>,
        benchmark: &str,
        config_json: &str,
    ) -> Result<Option<CaseId>, HybridDiskError> {
        let mut stmt =
            tx.prepare_cached("SELECT case_id FROM cases WHERE benchmark = ?1 AND config = ?2")?;
        let id: Option<i64> = stmt
            .query_row(params![benchmark, config_json], |row| row.get(0))
            .optional()?;
        Ok(id.map(CaseId))
    }

    fn set_case_workload(
        &self,
        tx: &Transaction<'_>,
        case: CaseId,
        workload: WorkloadId,
    ) -> Result<(), HybridDiskError> {
        let updated = tx
            .prepare_cached("UPDATE cases SET workload_id = ?1 WHERE case_id = ?2")?
            .execute(params![workload.0, case.0])?;
        if updated != 1 {
            return Err(HybridDiskError::MalformedStoredValue {
                kind: "case",
                value: case.0.to_string(),
            });
        }
        tx.prepare_cached(
            "INSERT OR IGNORE INTO measurement_cases (measurement_id, case_id)
             SELECT stable_measurement_id, ?1 FROM workloads
             WHERE workload_id = ?2 AND stable_measurement_id IS NOT NULL
             UNION
             SELECT last_measurement_id, ?1 FROM workloads
             WHERE workload_id = ?2 AND last_measurement_id IS NOT NULL",
        )?
        .execute(params![case.0, workload.0])?;
        Ok(())
    }

    fn get_case_workload(
        &self,
        tx: &Transaction<'_>,
        case: CaseId,
    ) -> Result<Option<WorkloadId>, HybridDiskError> {
        let mut stmt = tx.prepare_cached("SELECT workload_id FROM cases WHERE case_id = ?1")?;
        let id: Option<i64> = stmt.query_row(params![case.0], |row| row.get(0))?;
        Ok(id.map(WorkloadId))
    }

    fn intern_workload(
        &self,
        tx: &Transaction<'_>,
        identity: &WorkloadIdentity,
    ) -> Result<WorkloadId, HybridDiskError> {
        let workload_sha256 = identity.workload_sha256.to_string();
        let executable_sha256 = identity.executable_sha256.map(|digest| digest.to_string());
        let stdin_sha256 = identity.stdin_sha256.map(|digest| digest.to_string());
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO workloads
                     (workload_sha256, benchmark, executable_sha256, stdin_sha256, group_spec)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            stmt.execute(params![
                workload_sha256,
                identity.benchmark.as_str(),
                executable_sha256,
                stdin_sha256,
                identity.group_spec_json,
            ])?;
        }
        self.find_workload_id(tx, identity)?
            .ok_or_else(|| HybridDiskError::MalformedStoredValue {
                kind: "workload",
                value: workload_sha256,
            })
    }

    fn find_workload_id(
        &self,
        tx: &Transaction<'_>,
        identity: &WorkloadIdentity,
    ) -> Result<Option<WorkloadId>, HybridDiskError> {
        let workload_sha256 = identity.workload_sha256.to_string();
        let executable_sha256 = identity.executable_sha256.map(|digest| digest.to_string());
        let stdin_sha256 = identity.stdin_sha256.map(|digest| digest.to_string());
        let mut stmt = tx.prepare_cached(
            "SELECT workload_id, benchmark, executable_sha256, stdin_sha256, group_spec
             FROM workloads WHERE workload_sha256 = ?1",
        )?;
        let row: Option<StoredWorkloadIdentity> = stmt
            .query_row(params![workload_sha256], |row| {
                Ok(StoredWorkloadIdentity {
                    workload_id: row.get(0)?,
                    benchmark: row.get(1)?,
                    executable_sha256: row.get(2)?,
                    stdin_sha256: row.get(3)?,
                    group_spec_json: row.get(4)?,
                })
            })
            .optional()?;
        let Some(stored) = row else {
            return Ok(None);
        };

        let artifact_fields_match = stored.executable_sha256.as_deref()
            == executable_sha256.as_deref()
            && stored.stdin_sha256.as_deref() == stdin_sha256.as_deref();
        let group_spec_matches = if identity.is_shared() {
            serde_json::from_str::<GroupSpec>(&stored.group_spec_json)?
                == serde_json::from_str::<GroupSpec>(&identity.group_spec_json)?
        } else {
            stored.group_spec_json == identity.group_spec_json
        };

        assert!(
            stored.benchmark == identity.benchmark.as_str()
                && artifact_fields_match
                && group_spec_matches,
            "stored workload identity does not match SHA-256 {}",
            identity.workload_sha256,
        );
        Ok(Some(WorkloadId(stored.workload_id)))
    }

    fn get_workload_meta(
        &self,
        tx: &Transaction<'_>,
        workload: WorkloadId,
    ) -> Result<Option<WorkloadMeta>, HybridDiskError> {
        let mut stmt = tx.prepare_cached(
            "SELECT executable_sha256 IS NOT NULL, group_spec
             FROM workloads WHERE workload_id = ?1",
        )?;
        let row: Option<(bool, String)> = stmt
            .query_row(params![workload.0], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()?;

        row.map(|(is_shared, group_spec_json)| {
            let group_spec_digest = if is_shared {
                Some(serde_json::from_str::<GroupSpec>(&group_spec_json)?.digest())
            } else {
                None
            };
            Ok(WorkloadMeta {
                workload_id: workload,
                is_shared,
                group_spec_digest,
            })
        })
        .transpose()
    }

    fn get_workload_state(
        &self,
        tx: &Transaction<'_>,
        workload: WorkloadId,
    ) -> Result<Option<WorkloadState>, HybridDiskError> {
        let mut stmt = tx.prepare_cached(
            "SELECT stable_measurement_id, last_measurement_id, matched_count, suspicious_count,
                    replaced_count
             FROM workloads
             WHERE workload_id = ?1 AND stable_measurement_id IS NOT NULL",
        )?;
        Ok(stmt
            .query_row(params![workload.0], |row| {
                Ok(WorkloadState {
                    workload_id: workload,
                    stable_measurement_id: row.get(0)?,
                    last_measurement_id: row.get(1)?,
                    matched_count: u64::try_from(row.get::<_, i64>(2)?).unwrap_or(0),
                    suspicious_count: u64::try_from(row.get::<_, i64>(3)?).unwrap_or(0),
                    replaced_count: u64::try_from(row.get::<_, i64>(4)?).unwrap_or(0),
                })
            })
            .optional()?)
    }

    fn set_workload_state(
        &self,
        tx: &Transaction<'_>,
        state: &WorkloadState,
    ) -> Result<(), HybridDiskError> {
        let mut stmt = tx.prepare_cached(
            "UPDATE workloads SET
                 stable_measurement_id = ?2,
                 last_measurement_id = ?3,
                 matched_count = ?4,
                 suspicious_count = ?5,
                 replaced_count = ?6,
                 updated_at = unixepoch()
             WHERE workload_id = ?1
               AND EXISTS (
                   SELECT 1 FROM measurements
                   WHERE measurement_id = ?2 AND workload_id = ?1
               )
               AND EXISTS (
                   SELECT 1 FROM measurements
                   WHERE measurement_id = ?3 AND workload_id = ?1
               )",
        )?;
        let updated = stmt.execute(params![
            state.workload_id.0,
            state.stable_measurement_id,
            state.last_measurement_id,
            state.matched_count,
            state.suspicious_count,
            state.replaced_count,
        ])?;
        if updated != 1 {
            return Err(HybridDiskError::MalformedStoredValue {
                kind: "workload state",
                value: state.workload_id.0.to_string(),
            });
        }

        let mut link = tx.prepare_cached(
            "INSERT OR IGNORE INTO measurement_cases (measurement_id, case_id)
             SELECT ?1, case_id FROM cases WHERE workload_id = ?2",
        )?;
        link.execute(params![state.stable_measurement_id, state.workload_id.0])?;
        link.execute(params![state.last_measurement_id, state.workload_id.0])?;
        Ok(())
    }

    fn get_workload_last_measurement_ts(
        &self,
        tx: &Transaction<'_>,
        workload: WorkloadId,
    ) -> Result<Option<Timestamp>, HybridDiskError> {
        let mut stmt = tx.prepare_cached(
            "SELECT m.timestamp
             FROM workloads w
             JOIN measurements m ON m.measurement_id = w.last_measurement_id
             WHERE w.workload_id = ?1",
        )?;
        let seconds: Option<i64> = stmt
            .query_row(params![workload.0], |row| row.get(0))
            .optional()?;
        seconds.map(|s| parse_ts(s, "measurements")).transpose()
    }

    fn insert_measurement(
        &self,
        tx: &Transaction<'_>,
        record: &MeasurementRecord,
    ) -> Result<(), HybridDiskError> {
        let mut stmt = tx.prepare_cached(
            "INSERT OR IGNORE INTO measurements
                 (measurement_id, workload_id, timestamp, schema_version,
                  run_count, median_run_mean_ns, median_run_ci95_half_ns,
                  median_run_outlier_count, median_run_sample_count, checksum)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )?;
        stmt.execute(params![
            record.measurement_id,
            record.workload_id.0,
            record.timestamp.as_second(),
            record.schema_version,
            record.stats.run_count,
            record.stats.median_run_mean_ns,
            record.stats.median_run_ci95_half_ns,
            record.stats.median_run_outlier_count,
            record.stats.median_run_sample_count,
            record.checksum,
        ])?;
        Ok(())
    }

    fn link_measurement_cases(
        &self,
        tx: &Transaction<'_>,
        measurement_id: MeasurementId,
        cases: &[CaseId],
    ) -> Result<(), HybridDiskError> {
        let mut stmt = tx.prepare_cached(
            "INSERT OR IGNORE INTO measurement_cases (measurement_id, case_id) VALUES (?1, ?2)",
        )?;
        for case in cases {
            stmt.execute(params![measurement_id, case.0])?;
        }
        Ok(())
    }

    fn get_measurement_stats(
        &self,
        tx: &Transaction<'_>,
        measurement_id: MeasurementId,
    ) -> Result<Option<MeasurementStats>, HybridDiskError> {
        let mut stmt = tx.prepare_cached(
            "SELECT run_count, median_run_mean_ns, median_run_ci95_half_ns,
                    median_run_outlier_count, median_run_sample_count
             FROM measurements WHERE measurement_id = ?1",
        )?;
        Ok(stmt
            .query_row(params![measurement_id], |row| {
                Ok(MeasurementStats {
                    run_count: row.get(0)?,
                    median_run_mean_ns: row.get(1)?,
                    median_run_ci95_half_ns: row.get(2)?,
                    median_run_outlier_count: row.get(3)?,
                    median_run_sample_count: row.get(4)?,
                })
            })
            .optional()?)
    }
}

pub(super) fn parse_ts(seconds: i64, table: &'static str) -> Result<Timestamp, HybridDiskError> {
    Timestamp::from_second(seconds).map_err(|_| HybridDiskError::MalformedStoredValue {
        kind: "timestamp",
        value: table.into(),
    })
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
    #[error("failed to apply migration {version:?} to database: {source}")]
    MigrationError {
        version: usize,
        #[source]
        source: rusqlite::Error,
    },
    #[error("failed to apply Rust step for migration {version:?}: {source}")]
    RustMigrationError {
        version: usize,
        #[source]
        source: Box<HybridDiskError>,
    },
    #[error("failed to vacuum database after migration {version:?}: {source}")]
    MigrationVacuumError {
        version: usize,
        #[source]
        source: rusqlite::Error,
    },
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    SqlConversionFailure(#[from] rusqlite::types::FromSqlError),
    #[error("malformed stored {kind} '{value}'")]
    MalformedStoredValue { kind: &'static str, value: String },
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
    use crate::run::{Run, RunSeries};
    use crate::stats::{EstimationMode, Sample, StatsResult};
    use crate::storage::{seed_measurement, seed_result};
    use crate::workload::Sha256;
    use tempfile::TempDir;

    const TEST_CONFIG: &str = r#"{
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

    fn storage_at(data_dir: &Path, host: &str) -> HybridDiskStorage {
        let config_file = ConfigFile::from_str(data_dir, Some(host), TEST_CONFIG).unwrap();
        HybridDiskStorage::new(config_file, host).unwrap()
    }

    fn temp_storage(host: &str) -> (TempDir, HybridDiskStorage) {
        let dir = TempDir::new().unwrap();
        let results_dir = dir.path().join(RESULTS_DIR);
        fs::create_dir_all(results_dir.join("pi3")).unwrap();
        fs::create_dir_all(results_dir.join("pi4")).unwrap();
        let storage = storage_at(dir.path(), host);
        (dir, storage)
    }

    fn open_v1_database(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_migrations
                 (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL DEFAULT (unixepoch()));",
        )
        .unwrap();
        for (version, migration) in MIGRATIONS.iter().take(3).enumerate() {
            assert!(migration.apply.is_none());
            conn.execute_batch(migration.sql).unwrap();
            conn.execute(
                "INSERT INTO schema_migrations (version) VALUES (?1)",
                params![version],
            )
            .unwrap();
        }
        conn
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

    fn series_for_bench(bench: BenchmarkId, config: Config, timestamp: Timestamp) -> RunSeries {
        let mut series = sample_series(config);
        series.bench = bench;
        series.timestamp = timestamp;
        for run in &mut series.runs {
            run.timestamp = Timestamp::from_second(timestamp.as_second() + 1).unwrap();
        }
        series
    }

    /// Seed a series and immediately make it the case's stable+last result.
    fn seed(storage: &HybridDiskStorage, series: &RunSeries) -> MeasurementId {
        let measurement_id = seed_measurement(storage, series);
        seed_result(
            storage,
            &ResultsRow {
                bench: series.bench.clone(),
                config: series.config.clone(),
                stable_measurement_timestamp: series.timestamp,
                last_measurement_timestamp: series.timestamp,
                suspicious_count: 0,
                matched_count: 0,
                replaced_count: 0,
            },
        );
        measurement_id
    }

    #[test]
    fn test_mismatched_host_key() {
        let (_dir, storage) = temp_storage("pi5");

        let config = storage
            .config_file()
            .config_from_string("build=native,commit=abc1234,host=pi3")
            .unwrap();
        let series = sample_series(config.clone());

        let err = storage
            .read_transaction(|tx| storage.get_result_with_stats(tx, &series.bench, &config))
            .unwrap_err();
        assert!(matches!(err, HybridDiskError::HostMismatch { .. }));
    }

    #[test]
    fn test_write_measurement_json() {
        let (dir, storage) = temp_storage("pi5");

        let config = storage
            .config_file()
            .config_from_string("build=native,commit=abc1234,host=pi5")
            .unwrap();
        let series = sample_series(config.clone());
        let measurement_id = seed_measurement(&storage, &series);

        let path = measurement_file::current_path(&storage.runs_dir, measurement_id);
        assert!(path.is_file());
        assert!(path.starts_with(dir.path().join("results/pi5/runs/by-measurement")));
    }

    #[test]
    fn test_measurement_json_survives_storage_relocation() {
        let (source, storage) = temp_storage("pi5");
        let config = storage
            .config_file()
            .config_from_string("build=native,commit=abc1234,host=pi5")
            .unwrap();
        let series = sample_series(config.clone());
        let measurement_id = seed_measurement(&storage, &series);

        // Simulate stopping the service, then syncing or moving the results tree to a new root.
        drop(storage);
        let destination = TempDir::new().unwrap();
        fs::rename(
            source.path().join(RESULTS_DIR),
            destination.path().join(RESULTS_DIR),
        )
        .unwrap();
        let relocated = storage_at(destination.path(), "pi5");

        let relocated_path = measurement_file::current_path(&relocated.runs_dir, measurement_id);
        assert!(relocated_path.is_file());
    }

    #[test]
    fn test_result_query_joins_normalized_schema() {
        let (_dir, storage) = temp_storage("pi5");
        let config = storage
            .config_file()
            .config_from_string("build=generic,commit=def4567,host=pi5")
            .unwrap();
        let series = sample_series(config.clone());
        let measurement_id = seed(&storage, &series);

        storage
            .read_transaction(|tx| {
                let r = storage
                    .get_result_with_stats(tx, &series.bench, &config)?
                    .unwrap();
                assert_eq!(r.row.bench, series.bench);
                assert_eq!(r.row.config, config);
                assert_eq!(r.row.stable_measurement_timestamp, series.timestamp);
                assert_eq!(r.row.last_measurement_timestamp, series.timestamp);
                assert_eq!(r.stable_measurement_id, measurement_id);
                assert!(!r.is_shared);
                assert!((r.stable_stats.median_run_mean_ns - 30_000.0).abs() < 1e-6);
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
    #[should_panic(expected = "nested transaction on the same storage connection")]
    fn test_nested_transaction_panics() {
        let (_dir, storage) = temp_storage("pi5");

        storage
            .read_transaction(|_| {
                storage.read_transaction(|_| Ok(()))?;
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn higher_level_error_rolls_back_write_transaction() {
        #[derive(Debug, thiserror::Error)]
        enum CallbackError {
            #[error("abort transaction")]
            Abort,
            #[error(transparent)]
            Storage(#[from] HybridDiskError),
        }

        let (_dir, storage) = temp_storage("pi5");
        let result: Result<(), CallbackError> = storage.try_write_transaction(|tx| {
            storage.get_or_create_case(tx, "empty-config", "{}")?;
            Err(CallbackError::Abort)
        });
        assert!(matches!(result, Err(CallbackError::Abort)));

        storage
            .read_transaction(|tx| {
                assert_eq!(storage.find_case_id(tx, "empty-config", "{}")?, None);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_new_unknown_host() {
        let (_dir, storage) = temp_storage("pi5");
        let err = HybridDiskStorage::new(storage.config_file, "unknown-host").unwrap_err();
        assert!(matches!(err, HybridDiskError::UnknownHost(_)));
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
                assert!(
                    storage
                        .get_result_with_stats(tx, &bench, &config)?
                        .is_none()
                );
                Ok(())
            })
            .unwrap();
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

        for (b, cfg, ts) in [
            (bench1.clone(), cfg1, 10),
            (bench1.clone(), cfg2, 20),
            (bench2.clone(), cfg_empty, 30),
        ] {
            seed(
                &storage,
                &series_for_bench(b, cfg, Timestamp::from_second(ts).unwrap()),
            );
        }

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
        assert!(batches[1].iter().all(|r| r.row.bench == bench2));

        // Filtered.
        let filter = storage
            .config_file()
            .config_from_string("build=native")
            .unwrap();
        let mut filtered: Vec<Vec<ResultsRowWithStats>> = Vec::new();
        storage
            .read_transaction(|tx| {
                storage.for_each_result_with_stats(tx, None, &filter, |rows| {
                    filtered.push(rows.to_vec());
                    ControlFlow::Continue(())
                })
            })
            .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].len(), 1);

        // Break after first batch.
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

    #[test]
    fn test_for_each_measurement_history_returns_history() {
        let (_dir, storage) = temp_storage("pi3");
        let bench: BenchmarkId = "2015-04".try_into().unwrap();
        let cfg = storage
            .config_file()
            .config_from_string("build=generic,commit=abc1234,host=pi3")
            .unwrap();

        for ts in [10, 20, 30] {
            seed_measurement(
                &storage,
                &series_for_bench(
                    bench.clone(),
                    cfg.clone(),
                    Timestamp::from_second(ts).unwrap(),
                ),
            );
        }

        let mut rows: Vec<MeasurementHistoryRow> = Vec::new();
        storage
            .read_transaction(|tx| {
                storage.for_each_measurement_history(tx, &bench, |batch| {
                    rows.extend_from_slice(batch);
                    ControlFlow::Continue(())
                })
            })
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|row| !row.is_shared));
        assert!(
            rows.windows(2)
                .all(|pair| pair[0].measurement_id != pair[1].measurement_id)
        );
        let timestamps: Vec<_> = rows.iter().map(|r| r.timestamp.as_second()).collect();
        assert_eq!(timestamps, vec![10, 20, 30]);
    }

    // --- Normalized-schema operation tests (exercise the Storage trait directly) ---

    fn shared_identity(byte: u8) -> WorkloadIdentity {
        use crate::stats::StatsOptions;
        use crate::workload::GroupSpec;
        let gs = GroupSpec::new(vec!["arg".into()], None, StatsOptions::default());
        WorkloadIdentity::shared(
            "b".try_into().unwrap(),
            Sha256::hash_bytes(&[byte]),
            None,
            &gs,
        )
    }

    fn op_stats(mean: f64) -> MeasurementStats {
        MeasurementStats {
            run_count: 3,
            median_run_mean_ns: mean,
            median_run_ci95_half_ns: 1.0,
            median_run_outlier_count: 0,
            median_run_sample_count: 32,
        }
    }

    #[test]
    fn store_intern_case_is_idempotent() {
        let (_dir, storage) = temp_storage("pi5");
        storage
            .write_transaction(|tx| {
                let a = storage.get_or_create_case(tx, "b", r#"{"commit":"abc"}"#)?;
                let b = storage.get_or_create_case(tx, "b", r#"{"commit":"abc"}"#)?;
                assert_eq!(a, b);
                let other = storage.get_or_create_case(tx, "b", r#"{"commit":"def"}"#)?;
                assert_ne!(a, other);
                assert_eq!(
                    storage.find_case_id(tx, "b", r#"{"commit":"abc"}"#)?,
                    Some(a)
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn store_intern_workload_is_idempotent() {
        let (_dir, storage) = temp_storage("pi5");
        storage
            .write_transaction(|tx| {
                let id = shared_identity(1);
                let w1 = storage.intern_workload(tx, &id)?;
                let w2 = storage.intern_workload(tx, &id)?;
                assert_eq!(w1, w2);
                assert_eq!(storage.find_workload_id(tx, &id)?, Some(w1));
                Ok(())
            })
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "stored workload identity does not match SHA-256")]
    fn store_panics_on_hash_hit_with_different_full_identity() {
        let (_dir, storage) = temp_storage("pi5");
        let original = shared_identity(1);
        let mut collision = shared_identity(2);
        collision.workload_sha256 = original.workload_sha256;

        storage
            .write_transaction(|tx| storage.intern_workload(tx, &original).map(drop))
            .unwrap();
        storage
            .write_transaction(|tx| storage.intern_workload(tx, &collision).map(drop))
            .unwrap();
    }

    #[test]
    fn store_matches_shared_identity_across_json_representation_changes() {
        let (_dir, storage) = temp_storage("pi5");
        let identity = shared_identity(1);

        storage
            .write_transaction(|tx| {
                let workload = storage.intern_workload(tx, &identity)?;
                let mut stored: serde_json::Value =
                    serde_json::from_str(&identity.group_spec_json)?;
                stored
                    .as_object_mut()
                    .unwrap()
                    .insert("serde_only_metadata".into(), serde_json::Value::Bool(true));
                tx.execute(
                    "UPDATE workloads SET group_spec = ?1 WHERE workload_id = ?2",
                    params![serde_json::to_string_pretty(&stored)?, workload.0],
                )?;

                assert_eq!(storage.find_workload_id(tx, &identity)?, Some(workload));
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn store_measurement_history_and_case_workload() {
        let (_dir, storage) = temp_storage("pi5");
        storage
            .write_transaction(|tx| {
                let case_a = storage.get_or_create_case(tx, "b", r#"{"commit":"a"}"#)?;
                let case_b = storage.get_or_create_case(tx, "b", r#"{"commit":"b"}"#)?;
                let id = shared_identity(1);
                let workload = storage.intern_workload(tx, &id)?;

                let measurement = MeasurementId::new_v7();
                storage.insert_measurement(
                    tx,
                    &MeasurementRecord {
                        measurement_id: measurement,
                        workload_id: workload,
                        timestamp: Timestamp::from_second(100).unwrap(),
                        schema_version: 2,
                        stats: op_stats(1000.0),
                        checksum: None,
                    },
                )?;
                storage.link_measurement_cases(tx, measurement, &[case_a])?;
                storage.set_workload_state(
                    tx,
                    &WorkloadState {
                        workload_id: workload,
                        stable_measurement_id: measurement,
                        last_measurement_id: measurement,
                        matched_count: 0,
                        suspicious_count: 0,
                        replaced_count: 0,
                    },
                )?;
                storage.set_case_workload(tx, case_a, workload)?;
                // B did not participate in the execution, but inheriting the workload makes its
                // current stable/last measurement visible in B's history.
                storage.set_case_workload(tx, case_b, workload)?;

                assert_eq!(storage.get_case_workload(tx, case_a)?, Some(workload));
                assert_eq!(storage.get_case_workload(tx, case_b)?, Some(workload));
                assert_eq!(
                    storage.get_measurement_stats(tx, measurement)?,
                    Some(op_stats(1000.0))
                );
                let covered: i64 = tx.query_one(
                    "SELECT COUNT(*) FROM measurement_cases WHERE measurement_id = ?1",
                    params![measurement],
                    |r| r.get(0),
                )?;
                assert_eq!(covered, 2);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn store_workload_state_round_trips() {
        let (_dir, storage) = temp_storage("pi5");
        storage
            .write_transaction(|tx| {
                let id = shared_identity(1);
                let workload = storage.intern_workload(tx, &id)?;
                let stable = MeasurementId::new_v7();
                let last = MeasurementId::new_v7();
                for m in [stable, last] {
                    storage.insert_measurement(
                        tx,
                        &MeasurementRecord {
                            measurement_id: m,
                            workload_id: workload,
                            timestamp: Timestamp::from_second(1).unwrap(),
                            schema_version: 2,
                            stats: op_stats(1.0),
                            checksum: None,
                        },
                    )?;
                }

                let result = WorkloadState {
                    workload_id: workload,
                    stable_measurement_id: stable,
                    last_measurement_id: last,
                    matched_count: 5,
                    suspicious_count: 1,
                    replaced_count: 2,
                };
                storage.set_workload_state(tx, &result)?;
                assert_eq!(
                    storage.get_workload_state(tx, workload)?,
                    Some(result.clone())
                );
                assert_eq!(
                    storage.get_workload_last_measurement_ts(tx, workload)?,
                    Some(Timestamp::from_second(1).unwrap())
                );

                let other_workload = storage.intern_workload(tx, &shared_identity(2))?;
                let other_measurement = MeasurementId::new_v7();
                storage.insert_measurement(
                    tx,
                    &MeasurementRecord {
                        measurement_id: other_measurement,
                        workload_id: other_workload,
                        timestamp: Timestamp::from_second(2).unwrap(),
                        schema_version: 2,
                        stats: op_stats(2.0),
                        checksum: None,
                    },
                )?;
                let invalid = WorkloadState {
                    last_measurement_id: other_measurement,
                    ..result.clone()
                };
                assert!(matches!(
                    storage.set_workload_state(tx, &invalid),
                    Err(HybridDiskError::MalformedStoredValue {
                        kind: "workload state",
                        ..
                    })
                ));

                let updated = WorkloadState {
                    matched_count: 6,
                    ..result
                };
                storage.set_workload_state(tx, &updated)?;
                assert_eq!(
                    storage
                        .get_workload_state(tx, workload)?
                        .unwrap()
                        .matched_count,
                    6
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn store_stdin_without_executable_rejected() {
        let (_dir, storage) = temp_storage("pi5");
        // The CHECK constraint forbids stdin_sha256 without executable_sha256.
        let err = storage.write_transaction(|tx| {
            tx.execute(
                "INSERT INTO workloads (workload_sha256, benchmark, executable_sha256, stdin_sha256, group_spec)
                 VALUES ('00', 'b', NULL, 'ff', '{}')",
                [],
            )?;
            Ok(())
        });
        assert!(err.is_err());
    }

    #[test]
    fn failed_rust_migration_rolls_back_schema_and_version() {
        let (dir, storage) = temp_storage("pi5");
        let db_path = dir.path().join(RESULTS_DIR).join("pi5").join(DB_FILE);
        {
            let conn = open_v1_database(&db_path);
            conn.execute(
                "INSERT INTO run_series (bench, config, timestamp, run_count,
                     median_run_mean_ns, median_run_ci95_half_ns,
                     median_run_outlier_count, median_run_sample_count)
                 VALUES ('invalid benchmark', '{}', 100, 3, 1000.0, 5.0, 0, 32)",
                [],
            )
            .unwrap();
        }

        let error = storage.read_transaction(|_| Ok(())).unwrap_err();
        assert!(matches!(
            error,
            HybridDiskError::RustMigrationError { version: 3, .. }
        ));

        let conn = Connection::open(&db_path).unwrap();
        let migration_version: i64 = conn
            .query_one("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(migration_version, 2);
        let v1_rows: i64 = conn
            .query_one("SELECT COUNT(*) FROM run_series", [], |row| row.get(0))
            .unwrap();
        assert_eq!(v1_rows, 1);
        let normalized_tables: i64 = conn
            .query_one(
                "SELECT COUNT(*) FROM sqlite_schema
                 WHERE type = 'table'
                   AND name IN ('cases', 'workloads', 'measurements', 'measurement_cases')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(normalized_tables, 0);
    }

    #[test]
    fn v1_migration_folds_rows_on_open() {
        let (dir, storage) = temp_storage("pi5");
        let db_path = dir.path().join(RESULTS_DIR).join("pi5").join(DB_FILE);

        // Pre-seed the DB with v1 rows before the storage first opens (which migrates).
        {
            let conn = open_v1_database(&db_path);
            conn.execute(
                "INSERT INTO run_series (bench, config, timestamp, run_count, median_run_mean_ns,
                     median_run_ci95_half_ns, median_run_outlier_count, median_run_sample_count)
                 VALUES ('2015-04', '{\"build\":\"generic\",\"commit\":\"abc1234\"}',
                         100, 3, 1000.0, 5.0, 0, 32)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO run_series (bench, config, timestamp, run_count, median_run_mean_ns,
                     median_run_ci95_half_ns, median_run_outlier_count, median_run_sample_count)
                 VALUES ('2015-04', '{\"build\":\"generic\",\"commit\":\"abc1234\"}',
                         200, 3, 1100.0, 5.0, 0, 32)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO results (bench, config, stable_series_timestamp, last_series_timestamp,
                     matched_count, suspicious_count, replaced_count)
                 VALUES ('2015-04', '{\"build\":\"generic\",\"commit\":\"abc1234\"}',
                         100, 200, 4, 1, 2)",
                [],
            )
            .unwrap();
        }

        // Opening the storage triggers the one-time v1 migration.
        storage
            .read_transaction(|tx| {
                let cases: i64 = tx.query_one("SELECT COUNT(*) FROM cases", [], |r| r.get(0))?;
                assert_eq!(cases, 1);
                let measurements: i64 =
                    tx.query_one("SELECT COUNT(*) FROM measurements", [], |r| r.get(0))?;
                assert_eq!(measurements, 2);
                let v1_tables: i64 = tx.query_one(
                    "SELECT COUNT(*) FROM sqlite_schema
                     WHERE type = 'table' AND name IN ('results', 'run_series')",
                    [],
                    |r| r.get(0),
                )?;
                assert_eq!(v1_tables, 0);
                let migration_version: i64 =
                    tx.query_one("SELECT MAX(version) FROM schema_migrations", [], |r| {
                        r.get(0)
                    })?;
                assert_eq!(migration_version, 4);
                let free_pages: i64 = tx.query_one("PRAGMA freelist_count", [], |r| r.get(0))?;
                assert_eq!(free_pages, 0);
                // The case's result points at the migrated isolated workload with copied counters.
                let case = storage
                    .find_case_id(tx, "2015-04", r#"{"build":"generic","commit":"abc1234"}"#)?
                    .unwrap();
                let workload = storage.get_case_workload(tx, case)?.unwrap();
                let result = storage.get_workload_state(tx, workload)?.unwrap();
                assert_eq!(result.matched_count, 4);
                assert_eq!(result.suspicious_count, 1);
                assert_eq!(result.replaced_count, 2);
                Ok(())
            })
            .unwrap();

        // Reopening at migration 04 is idempotent and does not duplicate the backfill.
        drop(storage);
        let storage = storage_at(dir.path(), "pi5");
        storage
            .read_transaction(|tx| {
                let measurements: i64 =
                    tx.query_one("SELECT COUNT(*) FROM measurements", [], |r| r.get(0))?;
                assert_eq!(measurements, 2);
                Ok(())
            })
            .unwrap();

        // A migrated v1 file is also found relative to a new live root.
        let config = storage
            .config_file()
            .config_from_string("build=generic,commit=abc1234,host=pi5")
            .unwrap();
        let timestamp = Timestamp::from_second(100).unwrap();
        let mut series = sample_series(config.clone());
        series.timestamp = timestamp;
        let v1_path =
            measurement_file::v1_path(&storage.runs_dir, &series.bench, &config, timestamp);
        fs::create_dir_all(v1_path.parent().unwrap()).unwrap();
        fs::write(&v1_path, serde_json::to_vec_pretty(&series).unwrap()).unwrap();

        // Relocation is supported between processes, after the original connection is closed.
        drop(storage);
        let destination = TempDir::new().unwrap();
        fs::rename(
            dir.path().join(RESULTS_DIR),
            destination.path().join(RESULTS_DIR),
        )
        .unwrap();
        let relocated = storage_at(destination.path(), "pi5");
        let relocated_v1_path =
            measurement_file::v1_path(&relocated.runs_dir, &series.bench, &config, timestamp);
        assert!(relocated_v1_path.is_file());
    }
}

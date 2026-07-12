//! Database migration definitions and Rust-driven migration steps.
//!
//! Most migrations are pure SQL. Migration 03 also folds the v1 `results`/`run_series` rows
//! into the normalized dedupe model because each row has to be hashed into the workload identity
//! model and re-linked through the case/measurement tables. Its Rust step runs in the same SQLite
//! transaction as its SQL and schema-version record. After all pending migrations commit, the
//! migration runner vacuums the database once to reclaim any freed pages.

use crate::config::BenchmarkId;
use crate::storage::hybrid_disk::{HybridDiskError, HybridDiskStorage, parse_ts};
use crate::storage::measurement_id::MeasurementId;
use crate::storage::{
    CaseId, MeasurementRecord, MeasurementStats, Storage, WorkloadId, WorkloadState,
};
use crate::workload::WorkloadIdentity;
use ahash::{HashSet, HashSetExt as _};
use rusqlite::Transaction;
use std::collections::BTreeMap;
use tracing::info;

/// A schema migration and its optional Rust-driven data step.
pub(super) struct Migration {
    pub sql: &'static str,
    pub apply: Option<MigrationFn>,
}

type MigrationFn =
    for<'conn> fn(&HybridDiskStorage, &Transaction<'conn>) -> Result<(), HybridDiskError>;

impl Migration {
    const fn sql(sql: &'static str) -> Self {
        Self { sql, apply: None }
    }

    const fn with_rust(sql: &'static str, apply: MigrationFn) -> Self {
        Self {
            sql,
            apply: Some(apply),
        }
    }
}

pub(super) static MIGRATIONS: &[Migration] = &[
    Migration::sql(include_str!("sql_migrations/00-initial-schema.sql")),
    Migration::sql(include_str!("sql_migrations/01-results-counts.sql")),
    Migration::sql(include_str!("sql_migrations/02-run-series-metrics.sql")),
    Migration::with_rust(
        include_str!("sql_migrations/03-dedupe-schema.sql"),
        migrate_v1_into_dedupe,
    ),
    Migration::sql(include_str!("sql_migrations/04-drop-v1-schema.sql")),
];

type V1CaseMap = BTreeMap<(String, String), V1Case>;

struct V1Case {
    case_id: CaseId,
    workload_id: WorkloadId,
    benchmark: BenchmarkId,
    canonical_config: String,
}

struct V1MigrationStats {
    cases: usize,
    workloads: usize,
    measurements: usize,
    results: usize,
}

/// Migrate the v1 `results`/`run_series` tables into the normalized model.
///
/// Each v1 `(benchmark, config)` becomes a case backed by an isolated workload; every v1 run series
/// becomes a v1 measurement linked only to its case, while its JSON file remains in the
/// reconstructible v1 layout. Stable/last pointers and counters are copied exactly. No
/// shared content identity is inferred.
/// Idempotent: v1 measurement ids are deterministic and every write is an upsert.
fn migrate_v1_into_dedupe(
    storage: &HybridDiskStorage,
    tx: &Transaction<'_>,
) -> Result<(), HybridDiskError> {
    let case_ids = migrate_v1_cases(storage, tx)?;
    assert_unique_v1_tails(&case_ids);
    let measurements = migrate_v1_measurements(storage, tx, &case_ids)?;
    let results = migrate_v1_results(storage, tx, &case_ids)?;

    let stats = V1MigrationStats {
        cases: case_ids.len(),
        workloads: case_ids.len(),
        measurements,
        results,
    };
    info!(
        cases = stats.cases,
        workloads = stats.workloads,
        measurements = stats.measurements,
        results = stats.results,
        "v1 backfill staged"
    );

    Ok(())
}

fn assert_unique_v1_tails(case_ids: &V1CaseMap) {
    let epoch = jiff::Timestamp::from_second(0).unwrap();
    let mut ids = HashSet::with_capacity(case_ids.len());
    for source in case_ids.values() {
        let id = MeasurementId::for_v1(&source.benchmark, &source.canonical_config, epoch);
        assert!(
            ids.insert(id),
            "two v1 cases produced the same UUIDv7 hash tail"
        );
    }
}

/// Pass 1: create a case + isolated workload for every v1 `(benchmark, config)`.
fn migrate_v1_cases(
    storage: &HybridDiskStorage,
    tx: &Transaction<'_>,
) -> Result<V1CaseMap, HybridDiskError> {
    let mut case_ids: V1CaseMap = BTreeMap::new();
    let mut stmt = tx.prepare(
        "SELECT DISTINCT bench, config FROM run_series
         UNION
         SELECT DISTINCT bench, config FROM results",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let bench: String = row.get(0)?;
        let config_json: String = row.get(1)?;

        let benchmark = parse_bench(&bench)?;
        let canonical_config = canonical_v1_config(&config_json)?;
        let case = storage.get_or_create_case(tx, &bench, &config_json)?;
        let identity = WorkloadIdentity::isolated_from_json(benchmark.clone(), &config_json);
        let workload = storage.intern_workload(tx, &identity)?;
        storage.set_case_workload(tx, case, workload)?;
        case_ids.insert(
            (bench, config_json),
            V1Case {
                case_id: case,
                workload_id: workload,
                benchmark,
                canonical_config,
            },
        );
    }
    Ok(case_ids)
}

/// Pass 2: copy every run series to a v1 measurement linked to its case.
fn migrate_v1_measurements(
    storage: &HybridDiskStorage,
    tx: &Transaction<'_>,
    case_ids: &V1CaseMap,
) -> Result<usize, HybridDiskError> {
    let mut count = 0;
    let mut stmt = tx.prepare(
        "SELECT bench, config, timestamp, run_count, median_run_mean_ns,
                median_run_ci95_half_ns, median_run_outlier_count, median_run_sample_count
         FROM run_series",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let bench: String = row.get(0)?;
        let config_json: String = row.get(1)?;
        let timestamp = parse_ts(row.get(2)?, "run_series")?;

        let source = case_ids
            .get(&(bench.clone(), config_json.clone()))
            .expect("run_series (bench, config) was inserted into cases in pass 1");

        let measurement_id =
            MeasurementId::for_v1(&source.benchmark, &source.canonical_config, timestamp);
        storage.insert_measurement(
            tx,
            &MeasurementRecord {
                measurement_id,
                workload_id: source.workload_id,
                timestamp,
                schema_version: 1,
                stats: MeasurementStats {
                    run_count: row.get(3)?,
                    median_run_mean_ns: row.get(4)?,
                    median_run_ci95_half_ns: row.get(5)?,
                    median_run_outlier_count: row.get(6)?,
                    median_run_sample_count: row.get(7)?,
                },
                checksum: None,
            },
        )?;
        storage.link_measurement_cases(tx, measurement_id, &[source.case_id])?;
        count += 1;
    }
    Ok(count)
}

/// Pass 3: copy stable/last pointers and counters onto each workload.
fn migrate_v1_results(
    storage: &HybridDiskStorage,
    tx: &Transaction<'_>,
    case_ids: &V1CaseMap,
) -> Result<usize, HybridDiskError> {
    let mut count = 0;
    let mut stmt = tx.prepare(
        "SELECT bench, config, stable_series_timestamp, last_series_timestamp,
                matched_count, suspicious_count, replaced_count
         FROM results",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let bench: String = row.get(0)?;
        let config_json: String = row.get(1)?;
        let stable_ts = parse_ts(row.get(2)?, "results")?;
        let last_ts = parse_ts(row.get(3)?, "results")?;

        let source = case_ids
            .get(&(bench.clone(), config_json.clone()))
            .expect("results (bench, config) was inserted into cases in pass 1");

        storage.set_workload_state(
            tx,
            &WorkloadState {
                workload_id: source.workload_id,
                stable_measurement_id: MeasurementId::for_v1(
                    &source.benchmark,
                    &source.canonical_config,
                    stable_ts,
                ),
                last_measurement_id: MeasurementId::for_v1(
                    &source.benchmark,
                    &source.canonical_config,
                    last_ts,
                ),
                matched_count: u64::try_from(row.get::<_, i64>(4)?).unwrap_or(0),
                suspicious_count: u64::try_from(row.get::<_, i64>(5)?).unwrap_or(0),
                replaced_count: u64::try_from(row.get::<_, i64>(6)?).unwrap_or(0),
            },
        )?;
        count += 1;
    }
    Ok(count)
}

fn canonical_v1_config(config_json: &str) -> Result<String, HybridDiskError> {
    let config: BTreeMap<String, String> = serde_json::from_str(config_json)?;
    let mut canonical = String::new();
    // Config keys and values cannot contain ',' or '=', so sorted key=value pairs are unambiguous.
    for (index, (key, value)) in config.into_iter().enumerate() {
        if index > 0 {
            canonical.push(',');
        }
        canonical.push_str(&key);
        canonical.push('=');
        canonical.push_str(&value);
    }
    Ok(canonical)
}

fn parse_bench(bench: &str) -> Result<BenchmarkId, HybridDiskError> {
    bench
        .try_into()
        .map_err(|_| HybridDiskError::MalformedStoredValue {
            kind: "benchmark",
            value: bench.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_config_is_canonicalized_independently_of_json_formatting() {
        assert_eq!(
            canonical_v1_config(r#"{ "b": "2", "a": "1" }"#).unwrap(),
            "a=1,b=2"
        );
        assert_eq!(
            canonical_v1_config(r#"{"a":"1","b":"2"}"#).unwrap(),
            "a=1,b=2"
        );
    }
}

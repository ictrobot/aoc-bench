//! Processing selected run groups: opportunistic reuse or execute-and-record.
//!
//! Shared groups are restated, content-hashed, and either inherit a measured workload or execute one
//! representative. Isolated groups resolve and execute their single case lazily. Both paths record
//! through the same measurement and drift workflow. Reuse atomically points every covered case at
//! an already-measured workload without re-running the workload; execution records one measurement,
//! its case fan-out, and the per-workload stable/drift update in one transaction.
//!
//! Deliberate reruns and `run-all` pass `reuse = false` so they always execute after hashing.

use crate::config::{BenchmarkId, Config, ConfigFile, KeyValue};
use crate::group::{CaseGroup, GroupError, IsolatedGroup, RunGroup};
use crate::host_config::HostConfig;
use crate::measurement::{MEASUREMENT_SCHEMA, Measurement};
use crate::run::RunSeries;
use crate::runner::{RunError, Runner};
use crate::stable::{DriftCounters, DriftUpdate, RecordOutcome, compute_drift};
use crate::storage::{
    CaseId, HybridDiskError, HybridDiskStorage, MeasurementId, MeasurementRecord, MeasurementStats,
    Storage, StorageRead, WorkloadId, WorkloadState,
};
use crate::workload::{GroupSpec, WorkloadIdentity};
use rusqlite::Transaction;
use std::path::PathBuf;
use tracing::{info, warn};

/// Dependencies and flags supplied by the engine for processing one selected group.
pub struct ProcessContext<'a> {
    pub config_file: &'a ConfigFile,
    pub host_config: &'a HostConfig,
    pub host_kv: &'a KeyValue,
    pub storage: &'a HybridDiskStorage,
    pub dry_run: bool,
    pub force_update_stable: bool,
}

/// How a selected group was processed.
#[derive(Debug, Clone, PartialEq)]
pub enum GroupOutcome {
    /// The group inherited an existing measured workload without re-running the workload.
    Reused { workload_id: WorkloadId },
    /// The group ran the workload and recorded a measurement.
    Executed {
        workload_id: WorkloadId,
        measurement_id: MeasurementId,
        outcome: RecordOutcome,
    },
    /// Dry run: the group would inherit an existing workload.
    WouldReuse,
    /// Dry run: the group would execute and record with this outcome.
    WouldExecute { outcome: RecordOutcome },
}

/// Process either kind of selected run group without depending on the engine type.
pub fn process_group(
    group: &RunGroup,
    context: &ProcessContext<'_>,
    reuse: bool,
) -> Result<GroupOutcome, RunProcessError> {
    let outcome = match group {
        RunGroup::Shared(group) => process_shared_group(
            context.storage,
            context.host_config,
            group,
            reuse,
            context.dry_run,
            context.force_update_stable,
        )?,
        RunGroup::Isolated(group) => process_isolated(context, group)?,
    };

    // Log the stable/drift outcome for executions; inheritance logs in the shared path.
    match &outcome {
        GroupOutcome::Executed {
            outcome: record, ..
        }
        | GroupOutcome::WouldExecute { outcome: record } => {
            log_outcome(record, context.dry_run);
        }
        GroupOutcome::Reused { .. } | GroupOutcome::WouldReuse => {}
    }
    Ok(outcome)
}

/// Process a single selected group.
///
/// `reuse` enables the inheritance short-circuit (used for groups selected from the new pool).
/// Deliberate reruns and `run-all` pass `reuse = false` and always execute. `dry_run` performs the
/// same hashing and any enabled reuse lookup but writes no inheritance, measurement, case
/// association, or workload state.
pub fn process_shared_group(
    storage: &HybridDiskStorage,
    host_config: &HostConfig,
    group: &CaseGroup,
    reuse: bool,
    dry_run: bool,
    force_update_stable: bool,
) -> Result<GroupOutcome, RunProcessError> {
    // Before-hash stat guard: every member must still resolve to the same group key.
    group.restat()?;

    let representative = group.resolve_representative()?;
    let artifacts = representative.hash_artifacts()?;
    // After-hash stat guard: the files must not have changed while being hashed.
    group.restat()?;
    let identity = representative.workload_identity(artifacts.executable(), artifacts.stdin());

    let bench = &group.key.benchmark;
    let covered: Vec<Config> = group.configs().collect();

    if reuse {
        // The hash selects a candidate, then every stored identity field is verified. For a real
        // run, verification and case inheritance share one transaction so the decision cannot
        // become stale before the cases are repointed. Dry runs remain read-only.
        let reuse_hit = if dry_run {
            storage.read_transaction(|tx| lookup_measured_workload(storage, tx, &identity))?
        } else {
            storage.try_write_transaction(|tx| -> Result<_, RunProcessError> {
                let Some(workload_id) = lookup_measured_workload(storage, tx, &identity)? else {
                    return Ok(None);
                };
                for case in intern_cases(storage, tx, bench, &covered)? {
                    storage.set_case_workload(tx, case, workload_id)?;
                }
                // Roll back the case links if any covered path changed during inheritance.
                group.restat()?;
                Ok(Some(workload_id))
            })?
        };
        if let Some(workload_id) = reuse_hit {
            if dry_run {
                return Ok(GroupOutcome::WouldReuse);
            }
            info!(
                cases = group.case_count(),
                "inherited existing workload (reused)"
            );
            return Ok(GroupOutcome::Reused { workload_id });
        }
    }

    // Execute the representative once.
    let runner = Runner::from_hashed(
        &representative,
        artifacts,
        representative.config.clone(),
        host_config.clone(),
    );
    let series = runner.run_series()?;

    // After-measurement stat guard: discard if the executable or stdin changed while running.
    group.restat()?;

    if dry_run {
        let stats = MeasurementStats::from(&series);
        let outcome = storage.read_transaction(|tx| {
            preview_outcome(storage, tx, &identity, stats, force_update_stable)
        })?;
        return Ok(GroupOutcome::WouldExecute { outcome });
    }

    let (workload_id, measurement_id, outcome, _path) = record_measurement(
        storage,
        &identity,
        Some(&representative.group_spec),
        &series,
        &representative.config,
        &covered,
        force_update_stable,
    )?;

    info!(%measurement_id, cases = group.case_count(), "recorded measurement");
    Ok(GroupOutcome::Executed {
        workload_id,
        measurement_id,
        outcome,
    })
}

/// Run a single isolated case and record it as an isolated workload in normalized storage.
///
/// This is the lazy per-case path for isolated benchmarks: it runs the workload (never inheriting)
/// and records one isolated workload + measurement.
/// `series.config` may carry the host key; it is stripped for the stored case/covered configs.
/// On a dry run it computes the outcome that would result but writes nothing.
fn record_isolated_measurement(
    storage: &HybridDiskStorage,
    series: &RunSeries,
    dry_run: bool,
    force_update_stable: bool,
) -> Result<GroupOutcome, RunProcessError> {
    let hostless = series.config.without_host_key();
    let identity = WorkloadIdentity::isolated(series.bench.clone(), &hostless);

    if dry_run {
        let stats = MeasurementStats::from(series);
        let outcome = storage.read_transaction(|tx| {
            preview_outcome(storage, tx, &identity, stats, force_update_stable)
        })?;
        return Ok(GroupOutcome::WouldExecute { outcome });
    }

    let (workload_id, measurement_id, outcome, json_path) = record_measurement(
        storage,
        &identity,
        None,
        series,
        &hostless,
        std::slice::from_ref(&hostless),
        force_update_stable,
    )?;
    info!(path = %json_path.display(), "stored measurement");
    Ok(GroupOutcome::Executed {
        workload_id,
        measurement_id,
        outcome,
    })
}

/// Resolve and execute one isolated case, recording it as an isolated workload.
fn process_isolated(
    context: &ProcessContext<'_>,
    group: &IsolatedGroup,
) -> Result<GroupOutcome, RunProcessError> {
    let benchmark = context
        .config_file
        .benchmark_by_id(&group.benchmark)
        .expect("isolated group benchmark exists in the config file");
    let variant = benchmark
        .variant_for_config(&group.config)
        .expect("isolated group config is valid for its benchmark");

    let config = group.config.with(context.host_kv.clone());
    let runner = Runner::new(
        context.config_file.data_dir(),
        variant,
        config,
        context.host_config.clone(),
    )?;
    let series = runner.run_series()?;

    record_isolated_measurement(
        context.storage,
        &series,
        context.dry_run,
        context.force_update_stable,
    )
}

/// Log a stable/drift outcome at the appropriate level.
fn log_outcome(outcome: &RecordOutcome, dry_run: bool) {
    match outcome {
        RecordOutcome::Initial if dry_run => info!("would have recorded new measurement"),
        RecordOutcome::Initial => info!("recorded new measurement"),
        RecordOutcome::Matched => info!("matched existing stable result"),
        RecordOutcome::Suspicious {
            current_stable,
            suspicious_count,
        } => warn!(
            suspicious_count,
            stable_ns = current_stable.median_run_mean_ns,
            "didn't match stable result, suspicious"
        ),
        RecordOutcome::Replaced { old_stable } if dry_run => warn!(
            old_stable_ns = old_stable.median_run_mean_ns,
            "didn't match stable result, would have replaced"
        ),
        RecordOutcome::Replaced { old_stable } => warn!(
            old_stable_ns = old_stable.median_run_mean_ns,
            "didn't match stable result, replaced"
        ),
        RecordOutcome::Forced { old_stable } if dry_run => warn!(
            old_stable_ns = old_stable.median_run_mean_ns,
            "would have forced replacement of stable result"
        ),
        RecordOutcome::Forced { old_stable } => warn!(
            old_stable_ns = old_stable.median_run_mean_ns,
            "forced replacement of stable result"
        ),
    }
}

/// The shared measurement-recording core used by both the grouped (shared) and isolated paths.
///
/// Writes the immutable JSON first, then in one transaction interns the workload, inserts the
/// measurement and its execution-case fan-out, applies the per-workload drift update, and points
/// every covered case at the workload. The workload's benchmark, shared/isolated kind, and content
/// digests all come from `identity`.
fn record_measurement(
    storage: &HybridDiskStorage,
    identity: &WorkloadIdentity,
    group_spec: Option<&GroupSpec>,
    series: &RunSeries,
    executed_case: &Config,
    covered_cases: &[Config],
    force_update_stable: bool,
) -> Result<(WorkloadId, MeasurementId, RecordOutcome, PathBuf), RunProcessError> {
    let stats = MeasurementStats::from(series);
    let measurement_id = MeasurementId::new_v7();

    let measurement = Measurement {
        schema: MEASUREMENT_SCHEMA,
        measurement_id,
        bench: identity.benchmark.clone(),
        workload_sha256: identity.workload_sha256,
        group_spec: group_spec.cloned(),
        executable_sha256: identity.executable_sha256,
        stdin_sha256: identity.stdin_sha256,
        executed_case: executed_case.clone(),
        covered_cases: covered_cases.to_vec(),
        timestamp: series.timestamp,
        checksum: series.checksum.clone(),
        runs: series.runs.clone(),
    };

    // Write the immutable JSON before any database metadata (allowed orphan-file failure mode).
    let json_path = storage.write_measurement_json(&measurement)?;

    let (workload_id, outcome) = storage.write_transaction(|tx| {
        let workload_id = storage.intern_workload(tx, identity)?;
        storage.insert_measurement(
            tx,
            &MeasurementRecord {
                measurement_id,
                workload_id,
                timestamp: series.timestamp,
                schema_version: MEASUREMENT_SCHEMA,
                stats,
                checksum: series.checksum.clone(),
            },
        )?;

        let case_ids = intern_cases(storage, tx, &identity.benchmark, covered_cases)?;
        storage.link_measurement_cases(tx, measurement_id, &case_ids)?;

        let existing = storage.get_workload_state(tx, workload_id)?;
        let outcome = apply_workload_drift(
            storage,
            tx,
            workload_id,
            existing,
            measurement_id,
            stats,
            force_update_stable,
        )?;

        for case in case_ids {
            storage.set_case_workload(tx, case, workload_id)?;
        }

        Ok((workload_id, outcome))
    })?;

    Ok((workload_id, measurement_id, outcome, json_path))
}

/// A workload is an eligible reuse result only if it exists *and* has been measured.
fn lookup_measured_workload(
    storage: &HybridDiskStorage,
    tx: &Transaction<'_>,
    identity: &WorkloadIdentity,
) -> Result<Option<WorkloadId>, HybridDiskError> {
    let Some(workload_id) = storage.find_workload_id(tx, identity)? else {
        return Ok(None);
    };
    if storage.get_workload_state(tx, workload_id)?.is_some() {
        Ok(Some(workload_id))
    } else {
        Ok(None)
    }
}

/// Intern every `(bench, config)` case, returning their surrogate ids.
fn intern_cases(
    storage: &HybridDiskStorage,
    tx: &Transaction<'_>,
    bench: &BenchmarkId,
    configs: &[Config],
) -> Result<Vec<CaseId>, HybridDiskError> {
    configs
        .iter()
        .map(|config| {
            let config_json = serde_json::to_string(config)?;
            storage.get_or_create_case(tx, bench.as_str(), &config_json)
        })
        .collect()
}

/// Run the drift state machine for a new measurement against a workload's existing result.
fn drift_against_existing(
    storage: &HybridDiskStorage,
    tx: &Transaction<'_>,
    existing: &WorkloadState,
    new_stats: MeasurementStats,
    force_update_stable: bool,
) -> Result<(DriftUpdate, RecordOutcome), HybridDiskError> {
    let stable_stats = storage
        .get_measurement_stats(tx, existing.stable_measurement_id)?
        .ok_or(HybridDiskError::MalformedStoredValue {
            kind: "stable_measurement",
            value: existing.stable_measurement_id.to_string(),
        })?;

    Ok(compute_drift(
        stable_stats,
        new_stats,
        DriftCounters {
            matched_count: existing.matched_count,
            suspicious_count: existing.suspicious_count,
            replaced_count: existing.replaced_count,
        },
        force_update_stable,
    ))
}

/// Apply the per-workload stable/drift update for a new measurement.
fn apply_workload_drift(
    storage: &HybridDiskStorage,
    tx: &Transaction<'_>,
    workload_id: WorkloadId,
    existing: Option<WorkloadState>,
    new_measurement_id: MeasurementId,
    new_stats: MeasurementStats,
    force_update_stable: bool,
) -> Result<RecordOutcome, HybridDiskError> {
    let Some(existing) = existing else {
        // First measurement of this workload.
        storage.set_workload_state(
            tx,
            &WorkloadState {
                workload_id,
                stable_measurement_id: new_measurement_id,
                last_measurement_id: new_measurement_id,
                matched_count: 0,
                suspicious_count: 0,
                replaced_count: 0,
            },
        )?;
        return Ok(RecordOutcome::Initial);
    };

    let (update, outcome) =
        drift_against_existing(storage, tx, &existing, new_stats, force_update_stable)?;

    let stable_measurement_id = if update.stable_moved {
        new_measurement_id
    } else {
        existing.stable_measurement_id
    };

    storage.set_workload_state(
        tx,
        &WorkloadState {
            workload_id,
            stable_measurement_id,
            last_measurement_id: new_measurement_id,
            matched_count: update.counters.matched_count,
            suspicious_count: update.counters.suspicious_count,
            replaced_count: update.counters.replaced_count,
        },
    )?;

    Ok(outcome)
}

/// Compute what outcome a fresh measurement would produce without writing anything (dry run).
fn preview_outcome(
    storage: &HybridDiskStorage,
    tx: &Transaction<'_>,
    identity: &WorkloadIdentity,
    new_stats: MeasurementStats,
    force_update_stable: bool,
) -> Result<RecordOutcome, HybridDiskError> {
    let existing = match storage.find_workload_id(tx, identity)? {
        Some(workload_id) => storage.get_workload_state(tx, workload_id)?,
        None => None,
    };

    let Some(existing) = existing else {
        return Ok(RecordOutcome::Initial);
    };

    Ok(drift_against_existing(storage, tx, &existing, new_stats, force_update_stable)?.1)
}

/// Errors processing a selected group.
#[derive(Debug, thiserror::Error)]
pub enum RunProcessError {
    #[error("failed to resolve or hash group: {0}")]
    Group(#[from] GroupError),
    #[error("failed to run benchmark: {0}")]
    Run(#[from] RunError),
    #[error("storage error: {0}")]
    Storage(#[from] HybridDiskError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(unix)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BenchmarkVariant;
    use crate::group::resolve_and_group;
    use crate::run::test_support::{Fixture, write_sampler};
    use crate::storage::Storage;
    use std::fs;

    fn fixture() -> Fixture {
        let json = r#"{
            "config_keys": { "commit": { "values": ["a", "b", "c"] } },
            "benchmarks": [
                {
                    "benchmark": "bench",
                    "command": ["builds/{commit}/bin"],
                    "config": { "commit": ["a", "b", "c"] },
                    "dedupe": "inode-content",
                    "stats": {
                        "min_samples": 2,
                        "min_time_ns": 1,
                        "runs_per_series": 1,
                        "min_warmup_samples": 1,
                        "min_warmup_time_ns": 1
                    }
                }
            ]
        }"#;
        Fixture::new(json, |data| {
            // One real sampler, hardlinked under commit a and b; copied under commit c.
            let real = data.join("builds/real/bin");
            write_sampler(&real);
            fs::create_dir_all(data.join("builds/a")).unwrap();
            fs::create_dir_all(data.join("builds/b")).unwrap();
            fs::hard_link(&real, data.join("builds/a/bin")).unwrap();
            fs::hard_link(&real, data.join("builds/b/bin")).unwrap();
            write_sampler(&data.join("builds/c/bin")); // separate inode, identical bytes
        })
    }

    fn variant(f: &Fixture) -> &BenchmarkVariant {
        &f.storage.config_file().benchmarks()[0].variants()[0]
    }

    fn config(f: &Fixture, commit: &str) -> Config {
        f.storage
            .config_file()
            .config_from_string(&format!("commit={commit}"))
            .unwrap()
    }

    fn group_with(f: &Fixture, commit: &str) -> CaseGroup {
        let (groups, failures) =
            resolve_and_group(f.storage.config_file().data_dir(), [variant(f)]);
        assert!(failures.is_empty());
        groups
            .into_iter()
            .find(|group| {
                group.configs().any(|config| {
                    config
                        .get_by_name("commit")
                        .is_some_and(|value| value.value_name() == commit)
                })
            })
            .expect("commit belongs to a resolved group")
    }

    #[test]
    fn hardlinked_cases_execute_once_and_both_inherit() {
        let f = fixture();
        let _lock = f.storage.acquire_lock().unwrap();

        // commit a and b hardlink the same inode => one group of two members.
        let group = group_with(&f, "a");
        assert_eq!(group.case_count(), 2, "hardlinks must group together");

        let outcome =
            process_shared_group(&f.storage, &f.host_config, &group, true, false, false).unwrap();
        let workload_id = match outcome {
            GroupOutcome::Executed {
                workload_id,
                outcome,
                ..
            } => {
                assert_eq!(outcome, RecordOutcome::Initial);
                workload_id
            }
            other => panic!("expected Executed, got {other:?}"),
        };

        // Both cases now point at the same workload from a single execution.
        f.storage
            .read_transaction(|tx| {
                let ca = f.storage.get_or_create_case(
                    tx,
                    "bench",
                    &serde_json::to_string(&config(&f, "a"))?,
                )?;
                let cb = f.storage.get_or_create_case(
                    tx,
                    "bench",
                    &serde_json::to_string(&config(&f, "b"))?,
                )?;
                assert_eq!(f.storage.get_case_workload(tx, ca)?, Some(workload_id));
                assert_eq!(f.storage.get_case_workload(tx, cb)?, Some(workload_id));
                // Exactly one measurement, covering two cases.
                let measurements: i64 =
                    tx.query_one("SELECT COUNT(*) FROM measurements", [], |r| r.get(0))?;
                assert_eq!(measurements, 1);
                let covered: i64 =
                    tx.query_one("SELECT COUNT(*) FROM measurement_cases", [], |r| r.get(0))?;
                assert_eq!(covered, 2);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn copied_equal_group_inherits_without_executing() {
        let f = fixture();
        let _lock = f.storage.acquire_lock().unwrap();

        // First, run the hardlinked a/b group to create the shared workload.
        let ab = group_with(&f, "a");
        let executed =
            process_shared_group(&f.storage, &f.host_config, &ab, true, false, false).unwrap();
        let expected_workload = match executed {
            GroupOutcome::Executed { workload_id, .. } => workload_id,
            other => panic!("expected Executed, got {other:?}"),
        };

        // Now the byte-identical copy c (separate inode, its own group) inherits without re-running
        // the workload.
        let c = group_with(&f, "c");
        let inherited =
            process_shared_group(&f.storage, &f.host_config, &c, true, false, false).unwrap();
        assert_eq!(
            inherited,
            GroupOutcome::Reused {
                workload_id: expected_workload
            }
        );

        // Still exactly one measurement; c now points at the same workload and inherits the
        // current measurement into its visible history.
        f.storage
            .read_transaction(|tx| {
                let measurements: i64 =
                    tx.query_one("SELECT COUNT(*) FROM measurements", [], |r| r.get(0))?;
                assert_eq!(measurements, 1, "inheritance must not create a measurement");
                let cc = f.storage.get_or_create_case(
                    tx,
                    "bench",
                    &serde_json::to_string(&config(&f, "c"))?,
                )?;
                assert_eq!(
                    f.storage.get_case_workload(tx, cc)?,
                    Some(expected_workload)
                );
                let visible: i64 =
                    tx.query_one("SELECT COUNT(*) FROM measurement_cases", [], |r| r.get(0))?;
                assert_eq!(
                    visible, 3,
                    "A, B, and inheriting C must see the measurement"
                );
                let c_visible: i64 = tx.query_one(
                    "SELECT COUNT(*) FROM measurement_cases WHERE case_id = ?1",
                    [cc.0],
                    |r| r.get(0),
                )?;
                assert_eq!(c_visible, 1);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn rerun_measurement_reaches_every_case_linked_to_workload() {
        let f = fixture();
        let _lock = f.storage.acquire_lock().unwrap();

        let ab = group_with(&f, "a");
        process_shared_group(&f.storage, &f.host_config, &ab, true, false, false).unwrap();

        let c = group_with(&f, "c");
        process_shared_group(&f.storage, &f.host_config, &c, true, false, false).unwrap();

        // A deliberate rerun of the hardlinked A/B group creates one new measurement. C is not
        // covered by that execution, but it currently points at the same workload and therefore
        // receives the new current measurement in its visible history too.
        let ab = group_with(&f, "a");
        process_shared_group(&f.storage, &f.host_config, &ab, false, false, false).unwrap();

        f.storage
            .read_transaction(|tx| {
                let measurements: i64 =
                    tx.query_one("SELECT COUNT(*) FROM measurements", [], |r| r.get(0))?;
                assert_eq!(measurements, 2);
                let visible: i64 =
                    tx.query_one("SELECT COUNT(*) FROM measurement_cases", [], |r| r.get(0))?;
                assert_eq!(
                    visible, 6,
                    "both measurements must be visible to A, B, and C"
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "stored workload identity does not match SHA-256")]
    fn reuse_panics_on_hash_hit_with_different_full_identity() {
        let f = fixture();
        let _lock = f.storage.acquire_lock().unwrap();

        let ab = group_with(&f, "a");
        process_shared_group(&f.storage, &f.host_config, &ab, true, false, false).unwrap();

        // Simulate a framing bug/collision: the row keeps the same workload hash but its complete
        // stored spec describes a different invocation.
        f.storage
            .write_transaction(|tx| {
                tx.execute(
                    "UPDATE workloads
                     SET group_spec = json_set(group_spec, '$.checksum', 'different')
                     WHERE executable_sha256 IS NOT NULL",
                    [],
                )?;
                Ok(())
            })
            .unwrap();

        let c = group_with(&f, "c");
        let _ = process_shared_group(&f.storage, &f.host_config, &c, true, false, false);
    }

    #[test]
    fn rerun_always_executes_even_with_existing_result() {
        let f = fixture();
        let _lock = f.storage.acquire_lock().unwrap();

        let c = group_with(&f, "c");

        // First execution.
        process_shared_group(&f.storage, &f.host_config, &c, true, false, false).unwrap();

        // reuse = false => always re-runs the workload, producing a second measurement.
        let outcome =
            process_shared_group(&f.storage, &f.host_config, &c, false, false, false).unwrap();
        assert!(matches!(outcome, GroupOutcome::Executed { .. }));

        f.storage
            .read_transaction(|tx| {
                let measurements: i64 =
                    tx.query_one("SELECT COUNT(*) FROM measurements", [], |r| r.get(0))?;
                assert_eq!(measurements, 2, "deliberate rerun must execute");
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn dry_run_writes_nothing() {
        let f = fixture();
        let _lock = f.storage.acquire_lock().unwrap();

        let a = group_with(&f, "a");
        let outcome =
            process_shared_group(&f.storage, &f.host_config, &a, true, true, false).unwrap();
        assert_eq!(
            outcome,
            GroupOutcome::WouldExecute {
                outcome: RecordOutcome::Initial
            }
        );

        f.storage
            .read_transaction(|tx| {
                let measurements: i64 =
                    tx.query_one("SELECT COUNT(*) FROM measurements", [], |r| r.get(0))?;
                assert_eq!(measurements, 0, "dry run must not write");
                let cases: i64 = tx.query_one("SELECT COUNT(*) FROM cases", [], |r| r.get(0))?;
                assert_eq!(cases, 0);
                Ok(())
            })
            .unwrap();
    }
}

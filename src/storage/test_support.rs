//! Test-only helpers to seed the normalized schema.
//!
//! These populate cases, workloads, measurements, and stable/last state for query-layer tests.

use super::{
    HybridDiskStorage, MeasurementId, MeasurementRecord, MeasurementStats, ResultsRow, Storage,
    WorkloadState,
};
use crate::measurement::{MEASUREMENT_SCHEMA, Measurement};
use crate::run::RunSeries;
use crate::workload::WorkloadIdentity;

/// Record one isolated measurement for `series`, writing its schema-2 JSON and a measurement row
/// (deterministic id keyed by timestamp) linked to its case. Does not set workload state.
pub(crate) fn seed_measurement(storage: &HybridDiskStorage, series: &RunSeries) -> MeasurementId {
    let hostless = series.config.without_host_key();
    let config_json = serde_json::to_string(&hostless).unwrap();
    let canonical_config = hostless.to_string();
    let identity = WorkloadIdentity::isolated(series.bench.clone(), &hostless);
    let measurement_id = MeasurementId::for_v1(&series.bench, &canonical_config, series.timestamp);

    let measurement = Measurement {
        schema: MEASUREMENT_SCHEMA,
        measurement_id,
        bench: series.bench.clone(),
        workload_sha256: identity.workload_sha256,
        group_spec: None,
        executable_sha256: None,
        stdin_sha256: None,
        executed_case: hostless.clone(),
        covered_cases: vec![hostless.clone()],
        timestamp: series.timestamp,
        checksum: series.checksum.clone(),
        runs: series.runs.clone(),
    };
    storage.write_measurement_json(&measurement).unwrap();

    storage
        .write_transaction(|tx| {
            let case = storage.get_or_create_case(tx, series.bench.as_str(), &config_json)?;
            let workload = storage.intern_workload(tx, &identity)?;
            storage.insert_measurement(
                tx,
                &MeasurementRecord {
                    measurement_id,
                    workload_id: workload,
                    timestamp: series.timestamp,
                    schema_version: MEASUREMENT_SCHEMA,
                    stats: MeasurementStats::from(series),
                    checksum: series.checksum.clone(),
                },
            )?;
            storage.link_measurement_cases(tx, measurement_id, &[case])?;
            Ok(())
        })
        .unwrap();

    measurement_id
}

/// Point a case's result at its isolated workload, resolving the stable/last measurements from the
/// row's timestamps (which must have been seeded via [`seed_measurement`]).
pub(crate) fn seed_result(storage: &HybridDiskStorage, row: &ResultsRow) {
    let hostless = row.config.without_host_key();
    let config_json = serde_json::to_string(&hostless).unwrap();
    let canonical_config = hostless.to_string();
    let identity = WorkloadIdentity::isolated(row.bench.clone(), &hostless);
    let stable = MeasurementId::for_v1(
        &row.bench,
        &canonical_config,
        row.stable_measurement_timestamp,
    );
    let last = MeasurementId::for_v1(
        &row.bench,
        &canonical_config,
        row.last_measurement_timestamp,
    );

    storage
        .write_transaction(|tx| {
            let case = storage.get_or_create_case(tx, row.bench.as_str(), &config_json)?;
            let workload = storage
                .find_workload_id(tx, &identity)?
                .expect("seed_measurement must be called before seed_result");
            storage.set_workload_state(
                tx,
                &WorkloadState {
                    workload_id: workload,
                    stable_measurement_id: stable,
                    last_measurement_id: last,
                    matched_count: row.matched_count,
                    suspicious_count: row.suspicious_count,
                    replaced_count: row.replaced_count,
                },
            )?;
            storage.set_case_workload(tx, case, workload)?;
            Ok(())
        })
        .unwrap();
}

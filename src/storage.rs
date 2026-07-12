//! Storage traits and shared value types.
//!
//! Two traits divide the storage API by who can implement it:
//!
//! - [`StorageRead`] is the cross-host-aggregatable read subset — result rows and measurement
//!   history. Both the per-host [`HybridDiskStorage`] and the fan-out [`MultiHostStorage`] implement
//!   it, so every query command is generic over it.
//! - [`Storage`] is the full single-host backend contract: locking, transactions, and every
//!   normalized-schema operation (interning cases/workloads, recording measurements,
//!   maintaining case/workload associations and per-workload stable/drift state). Only
//!   [`HybridDiskStorage`] implements it. Its methods take `&Self::Tx<'_>` so a caller can compose
//!   several operations
//!   atomically inside one [`Storage::write_transaction`]; the rusqlite specifics stay in the impl.

use crate::config::{BenchmarkId, Config, ConfigFile, KeyValue};
use crate::measurement::Measurement;
use crate::run::RunSeries;
use crate::workload::WorkloadIdentity;
use jiff::Timestamp;
use std::fmt::Debug;
use std::ops::ControlFlow;
use std::path::PathBuf;

mod hybrid_disk;
mod measurement_file;
mod measurement_id;
mod migration;
mod multi_host;
#[cfg(test)]
mod test_support;

pub use hybrid_disk::{FileLock, HybridDiskError, HybridDiskStorage};
pub use measurement_id::{MeasurementId, MeasurementIdParseError};
pub use multi_host::{MultiHostError, MultiHostStorage};
#[cfg(test)]
pub(crate) use test_support::{seed_measurement, seed_result};

/// Cross-host-aggregatable read API for querying results.
pub trait StorageRead: Debug {
    type Tx<'a>;
    type Error: std::error::Error;

    // DB transactions

    /// Execute `f` inside a read transaction.
    fn read_transaction<F, T>(&self, f: F) -> Result<T, Self::Error>
    where
        F: FnOnce(&Self::Tx<'_>) -> Result<T, Self::Error>;

    // DB reads

    /// Retrieve the benchmark result row and stats summary for the provided benchmark and config.
    fn get_result_with_stats(
        &self,
        tx: &Self::Tx<'_>,
        bench: &BenchmarkId,
        config: &Config,
    ) -> Result<Option<ResultsRowWithStats>, Self::Error>;

    /// Retrieve all result rows and their associated stable/last stats that match the provided
    /// filters.
    ///
    /// `f` may be called multiple times with different batches of results. The result after
    /// concatenating all batches is guaranteed to be sorted by `(host, bench, config)`.
    fn for_each_result_with_stats(
        &self,
        tx: &Self::Tx<'_>,
        benchmark_filter: Option<&BenchmarkId>,
        config_filter: &Config,
        f: impl FnMut(&[ResultsRowWithStats]) -> ControlFlow<()>,
    ) -> Result<(), Self::Error>;

    /// Retrieve measurement-history rows for a benchmark.
    ///
    /// `f` may be called multiple times with different batches of rows. The ordering of rows is
    /// backend-defined.
    fn for_each_measurement_history(
        &self,
        tx: &Self::Tx<'_>,
        benchmark: &BenchmarkId,
        f: impl FnMut(&[MeasurementHistoryRow]) -> ControlFlow<()>,
    ) -> Result<(), Self::Error>;
}

/// The full single-host storage backend: locking, transactions, and normalized-schema operations.
///
/// Every operation takes `&Self::Tx<'_>` so the recording path can compose interning, measurement
/// insertion, case-history fan-out, drift updates, and case-workload writes atomically inside one
/// [`Storage::write_transaction`].
pub trait Storage: StorageRead {
    type Lock;

    // Lifecycle

    /// Acquire the exclusive system-wide benchmark lock.
    fn acquire_lock(&self) -> Result<Self::Lock, Self::Error>;

    /// Execute `f` inside a write transaction, committing on `Ok`.
    fn write_transaction<F, T>(&self, f: F) -> Result<T, Self::Error>
    where
        F: FnOnce(&Self::Tx<'_>) -> Result<T, Self::Error>;

    /// Write a measurement's immutable sample JSON, returning its path.
    fn write_measurement_json(&self, measurement: &Measurement) -> Result<PathBuf, Self::Error>;

    // Cases

    /// Get or create the case row for a `(benchmark, canonical hostless config JSON)` pair.
    fn get_or_create_case(
        &self,
        tx: &Self::Tx<'_>,
        benchmark: &str,
        config_json: &str,
    ) -> Result<CaseId, Self::Error>;

    /// Find a case's surrogate id without creating it.
    fn find_case_id(
        &self,
        tx: &Self::Tx<'_>,
        benchmark: &str,
        config_json: &str,
    ) -> Result<Option<CaseId>, Self::Error>;

    /// Point a case at its current workload and add that workload's stable/last measurements to
    /// the case's visible history.
    fn set_case_workload(
        &self,
        tx: &Self::Tx<'_>,
        case: CaseId,
        workload: WorkloadId,
    ) -> Result<(), Self::Error>;

    /// The workload a case currently displays, if any.
    fn get_case_workload(
        &self,
        tx: &Self::Tx<'_>,
        case: CaseId,
    ) -> Result<Option<WorkloadId>, Self::Error>;

    // Workloads

    /// Intern a workload from its identity, returning its surrogate id. Idempotent by
    /// `workload_sha256`; an existing hash must also match every full identity field. The identity's
    /// `executable_sha256`/`stdin_sha256` nullability records whether it is shared (content-backed)
    /// or isolated.
    ///
    /// # Panics
    ///
    /// Panics if an existing hash has different complete identity fields. That indicates an
    /// identity-encoding invariant failure or corrupted durable state; continuing could silently
    /// associate measurements with the wrong workload.
    fn intern_workload(
        &self,
        tx: &Self::Tx<'_>,
        identity: &WorkloadIdentity,
    ) -> Result<WorkloadId, Self::Error>;

    /// Find a workload by hash and verify that every stored identity field matches.
    ///
    /// # Panics
    ///
    /// Panics if a hash hit has different complete identity data.
    fn find_workload_id(
        &self,
        tx: &Self::Tx<'_>,
        identity: &WorkloadIdentity,
    ) -> Result<Option<WorkloadId>, Self::Error>;

    /// Read a workload's classification metadata (shared/isolated + stored group spec).
    fn get_workload_meta(
        &self,
        tx: &Self::Tx<'_>,
        workload: WorkloadId,
    ) -> Result<Option<WorkloadMeta>, Self::Error>;

    /// Read a workload's stable/drift state, if it has a current result.
    fn get_workload_state(
        &self,
        tx: &Self::Tx<'_>,
        workload: WorkloadId,
    ) -> Result<Option<WorkloadState>, Self::Error>;

    /// Update a workload's stable/drift state and add its stable/last measurements to every case
    /// currently pointing at it.
    fn set_workload_state(
        &self,
        tx: &Self::Tx<'_>,
        state: &WorkloadState,
    ) -> Result<(), Self::Error>;

    /// The timestamp of a workload's last measurement, for rerun ordering.
    fn get_workload_last_measurement_ts(
        &self,
        tx: &Self::Tx<'_>,
        workload: WorkloadId,
    ) -> Result<Option<Timestamp>, Self::Error>;

    // Measurements

    /// Insert a measurement row (idempotent by `measurement_id`).
    fn insert_measurement(
        &self,
        tx: &Self::Tx<'_>,
        record: &MeasurementRecord,
    ) -> Result<(), Self::Error>;

    /// Add a measurement to the visible history of each case (idempotent).
    ///
    /// The execution path calls this for the cases covered by the workload run. Inheritance and
    /// later workload updates add further links through `set_case_workload` and
    /// `set_workload_state`.
    fn link_measurement_cases(
        &self,
        tx: &Self::Tx<'_>,
        measurement_id: MeasurementId,
        cases: &[CaseId],
    ) -> Result<(), Self::Error>;

    /// The median-run summary of a measurement, for drift comparison.
    fn get_measurement_stats(
        &self,
        tx: &Self::Tx<'_>,
        measurement_id: MeasurementId,
    ) -> Result<Option<MeasurementStats>, Self::Error>;
}

/// Read-only storage bound to a single host, with a standard constructor.
pub trait PerHostStorage: StorageRead<Error: 'static> + Sized {
    /// Create a new storage instance for the given host.
    fn new_for_host(config_file: ConfigFile, host: &str) -> Result<Self, Self::Error>;

    /// The host key for this storage instance.
    fn host(&self) -> &KeyValue;
}

// --- Value types shared across the traits and their implementations ---

/// Surrogate key for a `cases` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CaseId(pub i64);

/// Surrogate key for a `workloads` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkloadId(pub i64);

/// Stable/drift state for a workload with a current result.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkloadState {
    pub workload_id: WorkloadId,
    pub stable_measurement_id: MeasurementId,
    pub last_measurement_id: MeasurementId,
    pub matched_count: u64,
    pub suspicious_count: u64,
    pub replaced_count: u64,
}

/// Cheap metadata about a workload, used for group classification without hashing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadMeta {
    pub workload_id: WorkloadId,
    /// `true` for shared (content-backed) workloads, `false` for isolated ones.
    pub is_shared: bool,
    /// Digest of the stored shared group spec's canonical binary encoding.
    ///
    /// `None` for isolated workloads, whose `group_spec` column contains config JSON instead.
    pub group_spec_digest: Option<crate::workload::Sha256>,
}

/// A measurement row to insert, plus its median-run summary.
#[derive(Debug, Clone)]
pub struct MeasurementRecord {
    pub measurement_id: MeasurementId,
    pub workload_id: WorkloadId,
    pub timestamp: Timestamp,
    pub schema_version: u32,
    pub stats: MeasurementStats,
    pub checksum: Option<String>,
}

/// Results row, without joined stats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultsRow {
    pub bench: BenchmarkId,
    pub config: Config,
    pub stable_measurement_timestamp: Timestamp,
    pub last_measurement_timestamp: Timestamp,
    pub suspicious_count: u64,
    pub matched_count: u64,
    pub replaced_count: u64,
}

/// Results row bundled with median stats for both stable and last measurement.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultsRowWithStats {
    pub row: ResultsRow,
    /// The stable measurement displayed by this result.
    pub stable_measurement_id: MeasurementId,
    /// Whether the current workload is shared (content-backed), rather than isolated.
    pub is_shared: bool,
    pub stable_stats: MeasurementStats,
    pub last_stats: MeasurementStats,
}

/// Summary statistics stored for one measurement.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MeasurementStats {
    pub run_count: usize,
    pub median_run_mean_ns: f64,
    pub median_run_ci95_half_ns: f64,
    pub median_run_outlier_count: usize,
    pub median_run_sample_count: usize,
}

impl MeasurementStats {
    /// Calculate confidence interval bounds for the mean.
    #[must_use]
    pub fn bounds(&self) -> (f64, f64) {
        (
            self.median_run_mean_ns - self.median_run_ci95_half_ns,
            self.median_run_mean_ns + self.median_run_ci95_half_ns,
        )
    }
}

impl From<&RunSeries> for MeasurementStats {
    fn from(series: &RunSeries) -> Self {
        let stats = series.median_stats();
        Self {
            run_count: series.runs.len(),
            median_run_mean_ns: stats.mean_ns_per_iter,
            median_run_ci95_half_ns: stats.ci95_half_width_ns,
            median_run_outlier_count: stats.outlier_count,
            median_run_sample_count: stats.samples.len(),
        }
    }
}

/// One entry in a benchmark's measurement history.
#[derive(Debug, Clone, PartialEq)]
pub struct MeasurementHistoryRow {
    pub config: Config,
    pub measurement_id: MeasurementId,
    pub is_shared: bool,
    pub timestamp: Timestamp,
    pub median_run_mean_ns: f64,
    pub median_run_ci95_half_ns: f64,
    pub run_count: usize,
}

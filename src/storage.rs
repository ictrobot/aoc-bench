use crate::config::{BenchmarkId, Config, ConfigFile, KeyValue};
use crate::run::RunSeries;
use jiff::Timestamp;
use std::fmt::Debug;
use std::ops::ControlFlow;
use std::path::PathBuf;

mod hybrid_disk;
mod multi_host;
pub use hybrid_disk::{FileLock, HybridDiskError, HybridDiskStorage};
pub use multi_host::{MultiHostError, MultiHostStorage};

/// Read-only storage API for querying results.
pub trait StorageRead: Debug {
    type Tx<'a>;
    type Error: std::error::Error;

    // Immutable JSON files

    /// Read a run series JSON file back into memory
    fn read_run_series_json(
        &self,
        bench: &BenchmarkId,
        config: &Config,
        timestamp: Timestamp,
    ) -> Result<RunSeries, Self::Error>;

    // DB transactions

    /// Execute `f` inside a read transaction.
    fn read_transaction<F, T>(&self, f: F) -> Result<T, Self::Error>
    where
        F: FnOnce(&Self::Tx<'_>) -> Result<T, Self::Error>;

    // DB reads

    /// Retrieve the benchmark result row and stats summary for the provided benchmark and config
    fn get_result_with_stats(
        &self,
        tx: &Self::Tx<'_>,
        bench: &BenchmarkId,
        config: &Config,
    ) -> Result<Option<ResultsRowWithStats>, Self::Error>;

    /// Retrieve all result rows and their associated stable/last stats that match the provided filters.
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

    /// Return up to `limit` existing results ordered by ascending `last_series_timestamp`.
    ///
    /// Only valid benchmarks and configs for the current config file are returned.
    fn oldest_results(
        &self,
        tx: &Self::Tx<'_>,
        benchmark_filter: Option<&BenchmarkId>,
        config_filter: &Config,
        limit: usize,
    ) -> Result<Vec<ResultsRow>, Self::Error>;

    /// Return up to `limit` (bench, config) pairs that have never been run.
    ///
    /// Only valid benchmarks and configs for the current config file are returned.
    fn missing_results(
        &self,
        tx: &Self::Tx<'_>,
        benchmark_filter: Option<&BenchmarkId>,
        config_filter: &Config,
        limit: usize,
    ) -> Result<Vec<(BenchmarkId, Config)>, Self::Error>;
}

/// Read/write storage API for run series and results.
pub trait Storage: StorageRead {
    type Lock;

    // Immutable JSON files

    /// Write the immutable JSON file for a run series
    fn write_run_series_json(&self, series: RunSeries) -> Result<PathBuf, Self::Error>;

    // Locking

    /// Acquire exclusive system-wide lock
    fn acquire_lock(&self) -> Result<Self::Lock, Self::Error>;

    // DB transactions

    /// Execute `f` inside a write transaction.
    fn write_transaction<F, T>(&self, f: F) -> Result<T, Self::Error>
    where
        F: FnOnce(&Self::Tx<'_>) -> Result<T, Self::Error>;

    // DB updates

    /// Store the provided run series summary in the database.
    fn insert_run_series(&self, tx: &Self::Tx<'_>, row: &RunSeries) -> Result<(), Self::Error>;

    /// Upsert the provided benchmark result row.
    fn upsert_results(&self, tx: &Self::Tx<'_>, row: &ResultsRow) -> Result<(), Self::Error>;
}

/// Read-only storage bound to a single host, with a standard constructor.
pub trait PerHostStorage: StorageRead<Error: 'static> + Sized {
    /// Create a new storage instance for the given host.
    fn new_for_host(config_file: ConfigFile, host: &str) -> Result<Self, Self::Error>;

    /// The host key for this storage instance.
    fn host(&self) -> &KeyValue;
}

/// Results row, without joined stats
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultsRow {
    pub bench: BenchmarkId,
    pub config: Config,
    pub stable_series_timestamp: Timestamp,
    pub last_series_timestamp: Timestamp,
    pub suspicious_count: u64,
    pub matched_count: u64,
    pub replaced_count: u64,
}

/// Results row bundled with median stats for both stable and last series.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultsRowWithStats {
    pub row: ResultsRow,
    pub stable_stats: RunSeriesStats,
    pub last_stats: RunSeriesStats,
}

/// Stats for a run series, including confidence interval bounds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunSeriesStats {
    pub run_count: usize,
    pub median_run_mean_ns: f64,
    pub median_run_ci95_half_ns: f64,
    pub median_run_outlier_count: usize,
    pub median_run_sample_count: usize,
}

impl From<&RunSeries> for RunSeriesStats {
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

impl RunSeriesStats {
    /// Calculate confidence interval bounds for the mean.
    #[must_use]
    pub fn bounds(&self) -> (f64, f64) {
        (
            self.median_run_mean_ns - self.median_run_ci95_half_ns,
            self.median_run_mean_ns + self.median_run_ci95_half_ns,
        )
    }
}

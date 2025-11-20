use crate::config::{BenchmarkId, Config};
use crate::run::RunSeries;
use jiff::Timestamp;
use std::path::PathBuf;

mod hybrid_disk;
pub use hybrid_disk::{FileLock, HybridDiskError, HybridDiskStorage};

/// Storage API for run series and results.
pub trait Storage {
    type Tx<'a>;
    type Error: std::error::Error;
    type Lock;

    // Immutable JSON files

    /// Write the immutable JSON file for a run series
    fn write_run_series_json(&self, series: &RunSeries) -> Result<PathBuf, Self::Error>;

    /// Read a run series JSON file back into memory
    fn read_run_series_json(
        &self,
        bench: &BenchmarkId,
        config: &Config,
        timestamp: Timestamp,
    ) -> Result<RunSeries, Self::Error>;

    // Locking

    /// Acquire exclusive system-wide lock
    fn acquire_lock(&self) -> Result<Self::Lock, Self::Error>;

    // DB transactions

    /// Execute `f` inside a write transaction.
    fn with_transaction<F, T>(&self, f: F) -> Result<T, Self::Error>
    where
        F: FnOnce(&Self::Tx<'_>) -> Result<T, Self::Error>;

    // DB updates

    /// Store the provided run series summary in the database.
    fn insert_run_series(&self, tx: &Self::Tx<'_>, row: &RunSeries) -> Result<(), Self::Error>;

    /// Upsert the provided benchmark result row.
    fn upsert_results(&self, tx: &Self::Tx<'_>, row: &ResultsRow) -> Result<(), Self::Error>;

    // DB reads

    /// Retrieve the benchmark result row and stats summary for the provided benchmark and config
    fn get_results_with_stats(
        &self,
        tx: &Self::Tx<'_>,
        bench: &BenchmarkId,
        config: &Config,
    ) -> Result<Option<ResultsRowWithStats>, Self::Error>;
}

/// Results row, without joined stats
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultsRow {
    pub bench: BenchmarkId,
    pub config: Config,
    pub stable_series_timestamp: Timestamp,
    pub last_series_timestamp: Timestamp,
    pub suspicious_series_count: i64,
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
    pub(crate) mean_ns_per_iter: f64,
    pub(crate) ci95_half_width_ns: f64,
}

impl From<&RunSeries> for RunSeriesStats {
    fn from(series: &RunSeries) -> Self {
        Self {
            mean_ns_per_iter: series.median_mean_ns_per_iter,
            ci95_half_width_ns: series.median_ci95_half_width_ns,
        }
    }
}

impl RunSeriesStats {
    /// Calculate confidence interval bounds for the mean.
    #[must_use]
    pub fn bounds(&self) -> (f64, f64) {
        (
            self.mean_ns_per_iter - self.ci95_half_width_ns,
            self.mean_ns_per_iter + self.ci95_half_width_ns,
        )
    }
}

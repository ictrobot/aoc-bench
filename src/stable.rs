//! Stable result management: drift detection and promotion logic.

use crate::run::RunSeries;
use crate::storage::{ResultsRow, ResultsRowWithStats, RunSeriesStats, Storage};
use std::path::PathBuf;

const STABLE_RESULT_CHANGE_REL_THRESHOLD: f64 = 0.03; // 3%
const STABLE_RESULT_CHANGE_REQUIRED_COUNT: i64 = 3;

/// Options that control how a run series is recorded.
#[derive(Debug, Clone, Copy, Default)]
pub struct RecordRunSeriesOptions {
    /// Force the new run series to become the stable result regardless of drift checks.
    pub force_update_stable: bool,
}

/// Outcome of [`record_run_series`].
#[derive(Debug, Clone)]
pub struct RunSeriesRecordOutcome {
    pub json_path: PathBuf,
    pub stable_result_changed: bool,
    pub was_suspicious: bool,
    pub suspicious_series_count: i64,
}

/// Write JSON, insert `run_series` row, and update results with drift detection.
pub fn record_run_series<S: Storage>(
    storage: &S,
    series: &RunSeries,
    options: RecordRunSeriesOptions,
) -> Result<RunSeriesRecordOutcome, S::Error> {
    let json_path = storage.write_run_series_json(series)?;

    let update = storage.with_transaction(|tx| {
        storage.insert_run_series(tx, series)?;
        update_results_with_series(storage, tx, series, options.force_update_stable)
    })?;

    Ok(RunSeriesRecordOutcome {
        json_path,
        stable_result_changed: update.stable_result_changed,
        was_suspicious: update.was_suspicious,
        suspicious_series_count: update.suspicious_series_count,
    })
}

fn update_results_with_series<S: Storage>(
    storage: &S,
    tx: &S::Tx<'_>,
    series: &RunSeries,
    force_update_stable: bool,
) -> Result<StableResultUpdate, S::Error> {
    let bench = series.bench.clone();
    let config = series.config.clone();

    if let Some(with_stats) = storage.get_results_with_stats(tx, &bench, &config)? {
        let ResultsRowWithStats {
            mut row,
            stable_stats,
            last_stats: _,
        } = with_stats;
        let new_stats = RunSeriesStats::from(series);
        let drift = DriftEvaluation::new(stable_stats, new_stats);

        let mut suspicious_series_count = if drift.is_suspicious {
            row.suspicious_series_count + 1
        } else {
            0
        };

        let mut stable_result_changed = false;
        if force_update_stable || suspicious_series_count >= STABLE_RESULT_CHANGE_REQUIRED_COUNT {
            row.stable_series_timestamp = series.timestamp;
            suspicious_series_count = 0;
            stable_result_changed = true;
        }

        row.last_series_timestamp = series.timestamp;
        row.suspicious_series_count = suspicious_series_count;
        storage.upsert_results(tx, &row)?;

        Ok(StableResultUpdate {
            stable_result_changed,
            was_suspicious: drift.is_suspicious,
            suspicious_series_count,
        })
    } else {
        let row = ResultsRow {
            bench,
            config,
            stable_series_timestamp: series.timestamp,
            last_series_timestamp: series.timestamp,
            suspicious_series_count: 0,
        };
        storage.upsert_results(tx, &row)?;
        Ok(StableResultUpdate {
            stable_result_changed: true,
            was_suspicious: false,
            suspicious_series_count: 0,
        })
    }
}

#[derive(Debug, Clone)]
struct StableResultUpdate {
    stable_result_changed: bool,
    was_suspicious: bool,
    suspicious_series_count: i64,
}

#[derive(Debug, Clone, Copy)]
struct DriftEvaluation {
    is_suspicious: bool,
}

impl DriftEvaluation {
    fn new(stable: RunSeriesStats, new_stats: RunSeriesStats) -> Self {
        let (stable_low, stable_high) = stable.bounds();
        let (new_low, new_high) = new_stats.bounds();
        let overlap = !(stable_high < new_low || new_high < stable_low);

        let rel_diff = if stable.mean_ns_per_iter == 0.0 {
            f64::INFINITY
        } else {
            (new_stats.mean_ns_per_iter - stable.mean_ns_per_iter).abs() / stable.mean_ns_per_iter
        };

        let is_suspicious = !overlap && rel_diff >= STABLE_RESULT_CHANGE_REL_THRESHOLD;
        Self { is_suspicious }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, ConfigFile};
    use crate::run::{Run, RunSeries};
    use crate::stats::{EstimationMode, Sample, StatsResult};
    use crate::storage::HybridDiskStorage;
    use jiff::Timestamp;
    use tempfile::TempDir;

    fn storage_with_config() -> (TempDir, impl Storage, Config) {
        let dir = TempDir::new().unwrap();
        let json = r#"{
            "config_keys": {
                "build": { "values": ["opt"] }
            },
            "benchmarks": [
                {
                    "benchmark": "bench",
                    "command": ["run", "{build}"],
                    "config": { "build": ["opt"] }
                }
            ]
        }"#;

        let config_file = ConfigFile::from_str(dir.path(), Some("test"), json).unwrap();
        let cfg = config_file
            .config_from_string("build=opt,host=test")
            .unwrap();
        let storage = HybridDiskStorage::new(config_file, "test").unwrap();
        (dir, storage, cfg)
    }

    fn run_series(config: &Config, mean: u32, half_width: u32, t: i64) -> RunSeries {
        RunSeries {
            schema: 1,
            bench: "bench".try_into().unwrap(),
            config: config.clone(),
            timestamp: Timestamp::from_second(t).unwrap(),
            runs: vec![Run {
                timestamp: Timestamp::from_second(t + 1).unwrap(),
                stats: StatsResult {
                    mean_ns_per_iter: f64::from(mean),
                    ci95_half_width_ns: f64::from(half_width),
                    mode: EstimationMode::PerIter,
                    intercept_ns: None,
                    outlier_count: 0,
                    samples: vec![Sample {
                        iters: 1,
                        total_ns: u64::from(mean),
                    }],
                    temporal_correlation: 0.0,
                },
            }],
            median_mean_ns_per_iter: f64::from(mean),
            median_ci95_half_width_ns: f64::from(half_width),
            checksum: None,
        }
    }

    #[test]
    fn first_run_promotes_to_stable() {
        let (_dir, storage, config) = storage_with_config();
        let series = run_series(&config, 1000, 50, 10);

        let outcome =
            record_run_series(&storage, &series, RecordRunSeriesOptions::default()).unwrap();

        assert!(outcome.stable_result_changed);
        assert!(!outcome.was_suspicious);
        assert_eq!(outcome.suspicious_series_count, 0);

        let results_row = storage
            .with_transaction(|tx| {
                storage.get_results_with_stats(tx, &series.bench, &series.config)
            })
            .unwrap()
            .unwrap();

        assert_eq!(results_row.row.stable_series_timestamp, series.timestamp);
        assert_eq!(results_row.row.last_series_timestamp, series.timestamp);
        assert_eq!(results_row.row.suspicious_series_count, 0);
    }

    #[test]
    fn non_suspicious_resets_counter() {
        let (_dir, storage, config) = storage_with_config();
        let stable = run_series(&config, 1000, 100, 10);
        let newer = run_series(&config, 1010, 50, 20); // overlaps, small diff

        record_run_series(&storage, &stable, RecordRunSeriesOptions::default()).unwrap();

        // artificially mark as suspicious once
        storage
            .with_transaction(|tx| {
                let mut row = storage
                    .get_results_with_stats(tx, &stable.bench, &stable.config)?
                    .unwrap()
                    .row;
                row.suspicious_series_count = 1;
                storage.upsert_results(tx, &row)
            })
            .unwrap();

        let outcome =
            record_run_series(&storage, &newer, RecordRunSeriesOptions::default()).unwrap();

        assert!(!outcome.stable_result_changed);
        assert!(!outcome.was_suspicious);
        assert_eq!(outcome.suspicious_series_count, 0);

        let results_row = storage
            .with_transaction(|tx| {
                storage.get_results_with_stats(tx, &stable.bench, &stable.config)
            })
            .unwrap()
            .unwrap();

        assert_eq!(results_row.row.stable_series_timestamp, stable.timestamp);
        assert_eq!(results_row.row.last_series_timestamp, newer.timestamp);
        assert_eq!(results_row.row.suspicious_series_count, 0);
    }

    #[test]
    fn suspicious_three_times_promotes() {
        let (_dir, storage, config) = storage_with_config();
        let stable = run_series(&config, 1000, 10, 10);
        record_run_series(&storage, &stable, RecordRunSeriesOptions::default()).unwrap();

        for i in 1..=3 {
            let series = run_series(&config, 1050 + i, 10, 20 + i64::from(i));
            let outcome =
                record_run_series(&storage, &series, RecordRunSeriesOptions::default()).unwrap();

            let results_row = storage
                .with_transaction(|tx| {
                    storage.get_results_with_stats(tx, &stable.bench, &stable.config)
                })
                .unwrap()
                .unwrap();

            if i < 3 {
                assert!(!outcome.stable_result_changed);
                assert!(outcome.was_suspicious);
                assert_eq!(outcome.suspicious_series_count, i64::from(i));

                assert_eq!(results_row.row.stable_series_timestamp, stable.timestamp);
            } else {
                assert!(outcome.stable_result_changed);
                assert!(outcome.was_suspicious);
                assert_eq!(outcome.suspicious_series_count, 0);

                assert_eq!(results_row.row.stable_series_timestamp, series.timestamp);
            }
            assert_eq!(results_row.row.last_series_timestamp, series.timestamp);
        }
    }

    #[test]
    fn force_update_bypasses_counter() {
        let (_dir, storage, config) = storage_with_config();
        let stable = run_series(&config, 1000, 10, 10);
        record_run_series(&storage, &stable, RecordRunSeriesOptions::default()).unwrap();

        let new = run_series(&config, 1010, 10, 20);
        let outcome = record_run_series(
            &storage,
            &new,
            RecordRunSeriesOptions {
                force_update_stable: true,
            },
        )
        .unwrap();

        assert!(outcome.stable_result_changed);
        assert_eq!(outcome.suspicious_series_count, 0);

        let results_row = storage
            .with_transaction(|tx| {
                storage.get_results_with_stats(tx, &stable.bench, &stable.config)
            })
            .unwrap()
            .unwrap();

        assert_eq!(results_row.row.stable_series_timestamp, new.timestamp);
        assert_eq!(results_row.row.last_series_timestamp, new.timestamp);
        assert_eq!(results_row.row.suspicious_series_count, 0);
    }
}

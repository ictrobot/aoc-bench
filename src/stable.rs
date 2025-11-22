//! Stable result management: drift detection and promotion logic.

use crate::run::RunSeries;
use crate::storage::{ResultsRow, ResultsRowWithStats, RunSeriesStats, Storage};
use std::path::PathBuf;

const STABLE_RESULT_CHANGE_REL_THRESHOLD: f64 = 0.03; // 3%
const STABLE_RESULT_CHANGE_REQUIRED_COUNT: i64 = 3;

/// Write JSON, insert `run_series` row, and update results with drift detection.
pub fn record_run_series<S: Storage>(
    storage: &S,
    series: &RunSeries,
    options: RecordOptions,
) -> Result<(RecordOutcome, PathBuf), S::Error> {
    let json_path = storage.write_run_series_json(series.clone())?;

    let update = storage.write_transaction(|tx| {
        storage.insert_run_series(tx, series)?;
        let (row, outcome) = process_series(storage, tx, series, options)?;
        storage.upsert_results(tx, &row)?;
        Ok(outcome)
    })?;

    Ok((update, json_path))
}

/// Simulate recording a run series without writing JSON or DB rows.
///
/// This runs the same drift-detection logic as [`record_run_series`] and returns the outcome
/// that would occur, but leaves storage untouched.
pub fn preview_run_series<S: Storage>(
    storage: &S,
    series: &RunSeries,
    options: RecordOptions,
) -> Result<RecordOutcome, S::Error> {
    storage.read_transaction(|tx| {
        let (_, outcome) = process_series(storage, tx, series, options)?;
        Ok(outcome)
    })
}

fn process_series<S: Storage>(
    storage: &S,
    tx: &S::Tx<'_>,
    series: &RunSeries,
    options: RecordOptions,
) -> Result<(ResultsRow, RecordOutcome), S::Error> {
    if let Some(ResultsRowWithStats {
        mut row,
        stable_stats,
        ..
    }) = storage.get_results_with_stats(tx, &series.bench, &series.config)?
    {
        let outcome = compute_outcome(&mut row, stable_stats, series, options.force_update_stable);
        Ok((row, outcome))
    } else {
        let row = ResultsRow {
            bench: series.bench.clone(),
            config: series.config.clone(),
            stable_series_timestamp: series.timestamp,
            last_series_timestamp: series.timestamp,
            suspicious_count: 0,
            matched_count: 0,
            replaced_count: 0,
        };
        Ok((row, RecordOutcome::Initial))
    }
}

fn compute_outcome(
    row: &mut ResultsRow,
    stable_stats: RunSeriesStats,
    series: &RunSeries,
    force_update_stable: bool,
) -> RecordOutcome {
    row.last_series_timestamp = series.timestamp;

    let new_stats = RunSeriesStats::from(series);
    let is_suspicious = is_suspicious(stable_stats, new_stats);

    if is_suspicious || force_update_stable {
        let suspicious_count = row.suspicious_count + 1;

        if suspicious_count >= STABLE_RESULT_CHANGE_REQUIRED_COUNT {
            row.stable_series_timestamp = series.timestamp;
            row.matched_count = 0;
            row.suspicious_count = 0;
            row.replaced_count += 1;

            RecordOutcome::Replaced {
                old_stable: stable_stats,
            }
        } else if force_update_stable {
            row.stable_series_timestamp = series.timestamp;
            row.matched_count = 0;
            row.suspicious_count = 0;
            row.replaced_count += 1;

            RecordOutcome::Forced {
                old_stable: stable_stats,
            }
        } else {
            // Does not reset matched_count which is the number of matches since replacement, not
            // the number of consecutive matches
            row.suspicious_count = suspicious_count;

            RecordOutcome::Suspicious {
                current_stable: stable_stats,
                suspicious_count,
            }
        }
    } else {
        row.suspicious_count = 0;
        row.matched_count += 1;

        RecordOutcome::Matched
    }
}

fn is_suspicious(stable: RunSeriesStats, new_stats: RunSeriesStats) -> bool {
    let (stable_low, stable_high) = stable.bounds();
    let (new_low, new_high) = new_stats.bounds();
    let overlap = !(stable_high < new_low || new_high < stable_low);

    let rel_diff = if stable.mean_ns_per_iter == 0.0 {
        f64::INFINITY
    } else {
        (new_stats.mean_ns_per_iter - stable.mean_ns_per_iter).abs() / stable.mean_ns_per_iter
    };

    !overlap && rel_diff >= STABLE_RESULT_CHANGE_REL_THRESHOLD
}

/// Options that control how a run series is recorded.
#[derive(Debug, Clone, Copy, Default)]
pub struct RecordOptions {
    /// Force the new run series to become the stable result regardless of drift checks.
    pub force_update_stable: bool,
}

/// Outcome of [`record_run_series`].
#[derive(Debug, Clone, PartialEq)]
pub enum RecordOutcome {
    Initial,
    Matched,
    Suspicious {
        current_stable: RunSeriesStats,
        suspicious_count: i64,
    },
    Replaced {
        old_stable: RunSeriesStats,
    },
    Forced {
        old_stable: RunSeriesStats,
    },
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
    fn test_first_run_promotes_to_stable() {
        let (_dir, storage, config) = storage_with_config();
        let series = run_series(&config, 1000, 50, 10);

        let (outcome, _) = record_run_series(&storage, &series, RecordOptions::default()).unwrap();
        assert_eq!(outcome, RecordOutcome::Initial);

        let results_row = storage
            .write_transaction(|tx| {
                storage.get_results_with_stats(tx, &series.bench, &series.config)
            })
            .unwrap()
            .unwrap();

        assert_eq!(results_row.row.stable_series_timestamp, series.timestamp);
        assert_eq!(results_row.row.last_series_timestamp, series.timestamp);
        assert_eq!(results_row.row.suspicious_count, 0);
        assert_eq!(results_row.row.matched_count, 0);
        assert_eq!(results_row.row.replaced_count, 0);
    }

    #[test]
    fn test_non_suspicious_resets_counter() {
        let (_dir, storage, config) = storage_with_config();
        let stable = run_series(&config, 1000, 100, 10);
        let newer = run_series(&config, 1010, 50, 20); // overlaps, small diff

        record_run_series(&storage, &stable, RecordOptions::default()).unwrap();

        // artificially mark as suspicious once
        storage
            .write_transaction(|tx| {
                let mut row = storage
                    .get_results_with_stats(tx, &stable.bench, &stable.config)?
                    .unwrap()
                    .row;
                row.suspicious_count = 1;
                storage.upsert_results(tx, &row)
            })
            .unwrap();

        let (outcome, _) = record_run_series(&storage, &newer, RecordOptions::default()).unwrap();
        assert_eq!(outcome, RecordOutcome::Matched);

        let results_row = storage
            .write_transaction(|tx| {
                storage.get_results_with_stats(tx, &stable.bench, &stable.config)
            })
            .unwrap()
            .unwrap();

        assert_eq!(results_row.row.stable_series_timestamp, stable.timestamp);
        assert_eq!(results_row.row.last_series_timestamp, newer.timestamp);
        assert_eq!(results_row.row.suspicious_count, 0);
        assert_eq!(results_row.row.matched_count, 1);
        assert_eq!(results_row.row.replaced_count, 0);
    }

    #[test]
    fn test_suspicious_three_times_promotes() {
        let (_dir, storage, config) = storage_with_config();
        let stable = run_series(&config, 1000, 10, 10);
        record_run_series(&storage, &stable, RecordOptions::default()).unwrap();

        // artificially increase matched_count and replaced_count
        storage
            .write_transaction(|tx| {
                let mut row = storage
                    .get_results_with_stats(tx, &stable.bench, &stable.config)?
                    .unwrap()
                    .row;
                row.matched_count = 54;
                row.replaced_count = 2;
                storage.upsert_results(tx, &row)
            })
            .unwrap();

        for i in 1..=3 {
            let series = run_series(&config, 1050 + i, 10, 20 + i64::from(i));
            let (outcome, _) =
                record_run_series(&storage, &series, RecordOptions::default()).unwrap();

            let results_row = storage
                .write_transaction(|tx| {
                    storage.get_results_with_stats(tx, &stable.bench, &stable.config)
                })
                .unwrap()
                .unwrap();

            if i < 3 {
                assert_eq!(
                    outcome,
                    RecordOutcome::Suspicious {
                        current_stable: RunSeriesStats::from(&stable),
                        suspicious_count: i64::from(i),
                    }
                );
                assert_eq!(results_row.row.matched_count, 54);
                assert_eq!(results_row.row.suspicious_count, i64::from(i));
                assert_eq!(results_row.row.replaced_count, 2);
                assert_eq!(results_row.row.stable_series_timestamp, stable.timestamp);
            } else {
                assert_eq!(
                    outcome,
                    RecordOutcome::Replaced {
                        old_stable: RunSeriesStats::from(&stable),
                    }
                );
                assert_eq!(results_row.row.matched_count, 0);
                assert_eq!(results_row.row.suspicious_count, 0);
                assert_eq!(results_row.row.replaced_count, 3);
                assert_eq!(results_row.row.stable_series_timestamp, series.timestamp);
            }
            assert_eq!(results_row.row.last_series_timestamp, series.timestamp);
        }
    }

    #[test]
    fn test_force_update_bypasses_counter() {
        let (_dir, storage, config) = storage_with_config();
        let stable = run_series(&config, 1000, 10, 10);
        record_run_series(&storage, &stable, RecordOptions::default()).unwrap();

        let new = run_series(&config, 1010, 10, 20);
        let (outcome, _) = record_run_series(
            &storage,
            &new,
            RecordOptions {
                force_update_stable: true,
            },
        )
        .unwrap();

        assert_eq!(
            outcome,
            RecordOutcome::Forced {
                old_stable: RunSeriesStats::from(&stable)
            }
        );

        let results_row = storage
            .write_transaction(|tx| {
                storage.get_results_with_stats(tx, &stable.bench, &stable.config)
            })
            .unwrap()
            .unwrap();

        assert_eq!(results_row.row.stable_series_timestamp, new.timestamp);
        assert_eq!(results_row.row.last_series_timestamp, new.timestamp);
        assert_eq!(results_row.row.suspicious_count, 0);
        assert_eq!(results_row.row.replaced_count, 1);
    }

    #[test]
    fn test_preview_run_series_does_not_mutate_storage() {
        let (_dir, storage, config) = storage_with_config();
        let stable = run_series(&config, 1000, 10, 10);
        record_run_series(&storage, &stable, RecordOptions::default()).unwrap();

        // Manually mark two suspicious runs so the next suspicious one would replace stable.
        storage
            .write_transaction(|tx| {
                let mut row = storage
                    .get_results_with_stats(tx, &stable.bench, &stable.config)?
                    .unwrap()
                    .row;
                row.suspicious_count = 2;
                storage.upsert_results(tx, &row)
            })
            .unwrap();

        let new_series = run_series(&config, 1100, 10, 20);
        let outcome = preview_run_series(
            &storage,
            &new_series,
            RecordOptions {
                force_update_stable: false,
            },
        )
        .unwrap();

        assert!(matches!(outcome, RecordOutcome::Replaced { .. }));

        // Storage state should remain unchanged after preview.
        let results_row = storage
            .read_transaction(|tx| {
                storage.get_results_with_stats(tx, &stable.bench, &stable.config)
            })
            .unwrap()
            .unwrap();

        assert_eq!(results_row.row.stable_series_timestamp, stable.timestamp);
        assert_eq!(results_row.row.suspicious_count, 2);
    }
}

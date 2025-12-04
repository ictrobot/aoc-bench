use crate::config::{BenchmarkId, Config, ConfigFile, Key, KeyValue};
use crate::stable::{Change, significant_change_with_threshold};
use crate::storage::{
    HybridDiskStorage, MultiHostError, MultiHostStorage, ResultsRowWithStats, RunSeriesStats,
    StorageRead,
};
use jiff::Timestamp;
use std::collections::BTreeMap;
use std::io;
use std::io::Write;
use std::ops::ControlFlow;

#[derive(Debug)]
pub struct StatsEngine {
    pub config_file: ConfigFile,
    pub storage: MultiHostStorage<HybridDiskStorage>,
}

impl StatsEngine {
    #[must_use]
    pub fn new(config_file: ConfigFile) -> Self {
        let storage = MultiHostStorage::new(config_file.clone());
        Self {
            config_file,
            storage,
        }
    }

    /// Export all matching stable results as TSV.
    pub fn export_tsv<W: Write>(
        &self,
        writer: &mut W,
        benchmark_filter: Option<&BenchmarkId>,
        config_filter: &Config,
    ) -> Result<(), StatsEngineError> {
        let config_keys: Vec<&Key> = self
            .config_file
            .config_keys()
            .iter()
            .filter(|&k| k != self.config_file.host_key())
            .collect();

        // Header
        write!(writer, "host\tbench").map_err(StatsEngineError::OutputError)?;
        for key in &config_keys {
            write!(writer, "\tcfg_{}", key.name()).map_err(StatsEngineError::OutputError)?;
        }
        writeln!(
            writer,
            "\tstable_timestamp\tmedian_run_mean_ns\tmedian_run_ci95_half_ns"
        )
        .map_err(StatsEngineError::OutputError)?;

        // Rows
        let mut write_row = |row: &ResultsRowWithStats| -> io::Result<()> {
            let host_value = row
                .row
                .config
                .get(self.config_file.host_key())
                .map_or("", |kv| kv.value_name());
            write!(writer, "{}\t{}", host_value, row.row.bench.as_str())?;

            for &key in &config_keys {
                let value = row.row.config.get(key).map_or("", |kv| kv.value_name());
                write!(writer, "\t{value}")?;
            }

            writeln!(
                writer,
                "\t{}\t{}\t{}",
                row.row.stable_series_timestamp.as_second(),
                row.stable_stats.median_run_mean_ns,
                row.stable_stats.median_run_ci95_half_ns
            )?;

            Ok(())
        };

        let mut io_result = Ok(());
        self.storage.read_transaction(|tx| {
            self.storage
                .for_each_result_with_stats(tx, benchmark_filter, config_filter, |rows| {
                    for row in rows {
                        io_result = write_row(row);
                        if io_result.is_err() {
                            return ControlFlow::Break(());
                        }
                    }
                    ControlFlow::Continue(())
                })
        })?;

        if io_result.is_ok() {
            io_result = writer.flush();
        }

        io_result.map_err(StatsEngineError::OutputError)
    }

    /// Find the fastest stable config for each matching benchmark.
    pub fn fastest_configs(
        &self,
        benchmark_filter: Option<&BenchmarkId>,
        config_filter: &Config,
    ) -> Result<Vec<FastestResult>, StatsEngineError> {
        let mut fastest: BTreeMap<BenchmarkId, FastestResult> = BTreeMap::new();

        self.storage.read_transaction(|tx| {
            self.storage
                .for_each_result_with_stats(tx, benchmark_filter, config_filter, |rows| {
                    for row in rows {
                        let entry =
                            fastest
                                .entry(row.row.bench.clone())
                                .or_insert_with(|| FastestResult {
                                    bench: row.row.bench.clone(),
                                    config: row.row.config.clone(),
                                    stable_stats: row.stable_stats,
                                });

                        if row.stable_stats.median_run_mean_ns
                            < entry.stable_stats.median_run_mean_ns
                        {
                            *entry = FastestResult {
                                bench: row.row.bench.clone(),
                                config: row.row.config.clone(),
                                stable_stats: row.stable_stats,
                            };
                        }
                    }

                    ControlFlow::Continue(())
                })
        })?;

        if fastest.is_empty() {
            return Err(StatsEngineError::NoResults);
        }

        Ok(fastest.into_values().collect())
    }

    /// Build a sorted timeline of stable results across a single varying config key.
    pub fn timeline(
        &self,
        benchmark: &BenchmarkId,
        config_filter: &Config,
    ) -> Result<TimelineResult, StatsEngineError> {
        let mut rows: Vec<ResultsRowWithStats> = Vec::new();

        self.storage.read_transaction(|tx| {
            self.storage
                .for_each_result_with_stats(tx, Some(benchmark), config_filter, |batch| {
                    rows.extend_from_slice(batch);
                    ControlFlow::Continue(())
                })
        })?;

        if rows.is_empty() {
            return Err(StatsEngineError::NoResults);
        }

        let expected_keys: Vec<&Key> = rows[0].row.config.iter().map(KeyValue::key).collect();
        for row in &rows[1..] {
            let keys: Vec<&Key> = row.row.config.iter().map(KeyValue::key).collect();
            if keys != expected_keys {
                return Err(StatsEngineError::MismatchedKeys {
                    expected: expected_keys.iter().map(|k| k.name().to_string()).collect(),
                    found: keys.iter().map(|k| k.name().to_string()).collect(),
                });
            }
        }

        let mut varying_keys: Vec<Key> = Vec::new();
        let base_config = &rows[0].row.config;
        for base_kv in base_config.iter() {
            let differs = rows.iter().skip(1).any(|row| {
                row.row.config.get(base_kv.key()).map(KeyValue::value_index)
                    != Some(base_kv.value_index())
            });
            if differs {
                varying_keys.push(base_kv.key().clone());
            }
        }

        let comparison_key = match varying_keys.len() {
            0 => return Err(StatsEngineError::NoVaryingKey),
            1 => varying_keys.remove(0),
            _ => {
                let keys = varying_keys
                    .iter()
                    .map(|k| k.name().to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(StatsEngineError::MultipleVaryingKeys(keys));
            }
        };

        let shared_config = base_config.without_key(&comparison_key);

        rows.sort_unstable_by_key(|row| {
            row.row
                .config
                .get(&comparison_key)
                .expect("comparison key present in config")
                .value_index()
        });

        let points = rows
            .into_iter()
            .map(|row| {
                let comparison_value = row
                    .row
                    .config
                    .get(&comparison_key)
                    .expect("comparison key present in config")
                    .clone();

                TimelinePoint {
                    comparison_value,
                    config: row.row.config,
                    stats: row.stable_stats,
                    stable_timestamp: row.row.stable_series_timestamp,
                }
            })
            .collect();

        Ok(TimelineResult {
            benchmark: benchmark.clone(),
            shared_config,
            comparison_key,
            points,
        })
    }

    /// Build a timeline and classify significant changes using the provided relative threshold.
    pub fn timeline_summary_with_threshold(
        &self,
        benchmark: &BenchmarkId,
        config_filter: &Config,
        rel_threshold: f64,
    ) -> Result<TimelineSummary, StatsEngineError> {
        let timeline = self.timeline(benchmark, config_filter)?;

        let Some(initial) = timeline.points.first().cloned() else {
            return Err(StatsEngineError::NoResults);
        };

        let mut omitted = 0usize;
        let mut changes = Vec::new();
        let mut previous: TimelinePoint = initial.clone();

        for point in timeline.points.into_iter().skip(1) {
            if let Some(change) =
                significant_change_with_threshold(previous.stats, point.stats, rel_threshold)
            {
                previous = point.clone();
                changes.push((point, change));
            } else {
                omitted += 1;
            }
        }

        Ok(TimelineSummary {
            benchmark: timeline.benchmark,
            shared_config: timeline.shared_config,
            comparison_key: timeline.comparison_key,
            initial,
            changes,
            omitted,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FastestResult {
    pub bench: BenchmarkId,
    pub config: Config,
    pub stable_stats: RunSeriesStats,
}

#[derive(Debug, Clone)]
pub struct TimelinePoint {
    pub comparison_value: KeyValue,
    pub config: Config,
    pub stats: RunSeriesStats,
    pub stable_timestamp: Timestamp,
}

#[derive(Debug, Clone)]
pub struct TimelineResult {
    pub benchmark: BenchmarkId,
    pub shared_config: Config,
    pub comparison_key: Key,
    pub points: Vec<TimelinePoint>,
}

#[derive(Debug, Clone)]
pub struct TimelineSummary {
    pub benchmark: BenchmarkId,
    pub shared_config: Config,
    pub comparison_key: Key,
    pub initial: TimelinePoint,
    pub changes: Vec<(TimelinePoint, Change)>,
    pub omitted: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum StatsEngineError {
    #[error("failed to read storage: {0}")]
    StorageError(#[from] MultiHostError<HybridDiskStorage>),
    #[error("error writing output: {0}")]
    OutputError(#[source] io::Error),
    #[error("no matching benchmark results found")]
    NoResults,
    #[error(
        "matched configs do not share the same set of keys (expected: {expected:?}, found: {found:?})"
    )]
    MismatchedKeys {
        expected: Vec<String>,
        found: Vec<String>,
    },
    #[error("no varying config key found; loosen the --config filter so exactly one key can vary")]
    NoVaryingKey,
    #[error(
        "multiple varying keys found ({0}); narrow the --config filter so only one key differs"
    )]
    MultipleVaryingKeys(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run::Run;
    use crate::stable::ChangeDirection;
    use crate::stats::{EstimationMode, Sample, StatsResult};
    use crate::storage::{HybridDiskStorage, Storage};
    use tempfile::TempDir;

    fn setup_storage(host: &str) -> (TempDir, StatsEngine) {
        let dir = TempDir::new().unwrap();

        let json = r#"{
            "config_keys": {
                "build": { "values": ["x", "y"] }
            },
            "benchmarks": [
                {
                    "benchmark": "bench",
                    "command": ["echo", "{build}"],
                    "config": { "build": ["x", "y"] }
                },
                {
                    "benchmark": "bench2",
                    "command": ["echo", "{build}"],
                    "config": { "build": ["x"] }
                }
            ]
        }"#;

        let config_file = ConfigFile::from_str(dir.path(), Some(host), json).unwrap();

        let storage = HybridDiskStorage::new(config_file.clone(), host).unwrap();
        let bench1: BenchmarkId = "bench".try_into().unwrap();
        let bench2: BenchmarkId = "bench2".try_into().unwrap();

        let mk_series = |bench: &BenchmarkId, build: &str, ts: i32| crate::run::RunSeries {
            schema: 1,
            bench: bench.clone(),
            config: config_file
                .config_from_string(&format!("build={build},host={host}"))
                .unwrap(),
            timestamp: jiff::Timestamp::from_second(i64::from(ts)).unwrap(),
            runs: vec![Run {
                timestamp: jiff::Timestamp::from_second(i64::from(ts) + 1).unwrap(),
                stats: StatsResult {
                    mean_ns_per_iter: 10.0 + f64::from(ts),
                    ci95_half_width_ns: 1.0,
                    mode: EstimationMode::PerIter,
                    intercept_ns: None,
                    outlier_count: 0,
                    temporal_correlation: 0.0,
                    samples: vec![Sample {
                        iters: 10,
                        total_ns: 100,
                    }],
                },
            }],
            checksum: None,
        };

        let series = [
            mk_series(&bench1, "x", 1_700_000_000),
            mk_series(&bench1, "y", 1_700_000_100),
            mk_series(&bench2, "x", 1_700_000_200),
        ];

        for s in series {
            storage.write_run_series_json(s.clone()).unwrap();
            storage
                .write_transaction(|tx| {
                    storage.insert_run_series(tx, &s)?;
                    storage.upsert_results(
                        tx,
                        &crate::storage::ResultsRow {
                            bench: s.bench.clone(),
                            config: s.config.clone(),
                            stable_series_timestamp: s.timestamp,
                            last_series_timestamp: s.timestamp,
                            suspicious_count: 0,
                            matched_count: 0,
                            replaced_count: 0,
                        },
                    )
                })
                .unwrap();
        }

        let engine = StatsEngine::new(config_file);
        (dir, engine)
    }

    #[test]
    fn test_export_tsv_writes_header_and_rows() {
        let (_dir, engine) = setup_storage("h1");

        let mut buf = std::io::Cursor::new(Vec::new());
        engine
            .export_tsv(&mut buf, None, &Config::new())
            .expect("export succeeds");

        let output = String::from_utf8(buf.into_inner()).unwrap();
        let mut lines = output.lines();

        assert_eq!(
            lines.next(),
            Some(
                "host\tbench\tcfg_build\tstable_timestamp\tmedian_run_mean_ns\tmedian_run_ci95_half_ns"
            )
        );
        assert_eq!(
            lines.next(),
            Some("h1\tbench\tx\t1700000000\t1700000010\t1")
        );
        assert_eq!(
            lines.next(),
            Some("h1\tbench\ty\t1700000100\t1700000110\t1")
        );
        assert_eq!(
            lines.next(),
            Some("h1\tbench2\tx\t1700000200\t1700000210\t1")
        );
        assert_eq!(lines.next(), None);
    }

    #[test]
    fn test_timeline_orders_by_comparison_key() {
        let (_dir, engine) = setup_storage("h1");
        let bench: BenchmarkId = "bench".try_into().unwrap();

        let timeline = engine
            .timeline(&bench, &Config::new())
            .expect("timeline succeeds");

        assert_eq!(timeline.comparison_key.name(), "build");
        let values: Vec<&str> = timeline
            .points
            .iter()
            .map(|p| p.comparison_value.value_name())
            .collect();
        assert_eq!(values, vec!["x", "y"]);

        let means: Vec<f64> = timeline
            .points
            .iter()
            .map(|p| p.stats.median_run_mean_ns)
            .collect();
        assert_eq!(means, vec![1_700_000_010.0, 1_700_000_110.0]);
    }

    #[test]
    fn test_timeline_errors_when_no_varying_key() {
        let (_dir, engine) = setup_storage("h1");
        let bench: BenchmarkId = "bench2".try_into().unwrap();

        let err = engine.timeline(&bench, &Config::new()).unwrap_err();
        assert!(matches!(err, StatsEngineError::NoVaryingKey));
    }

    #[test]
    fn test_timeline_errors_when_multiple_keys_vary() {
        let dir = TempDir::new().unwrap();
        let host = "h1";
        let json = r#"{
            "config_keys": {
                "build": { "values": ["x", "y"] },
                "commit": { "values": ["a", "b"] }
            },
            "benchmarks": [
                {
                    "benchmark": "bench",
                    "command": ["echo", "{build}-{commit}"],
                    "config": { "build": ["x", "y"], "commit": ["a", "b"] }
                }
            ]
        }"#;

        let config_file = ConfigFile::from_str(dir.path(), Some(host), json).unwrap();
        let storage = HybridDiskStorage::new(config_file.clone(), host).unwrap();
        let bench: BenchmarkId = "bench".try_into().unwrap();

        let mk_series = |build: &str, commit: &str, ts: i32| crate::run::RunSeries {
            schema: 1,
            bench: bench.clone(),
            config: config_file
                .config_from_string(&format!("build={build},commit={commit},host={host}"))
                .unwrap(),
            timestamp: Timestamp::from_second(i64::from(ts)).unwrap(),
            runs: vec![Run {
                timestamp: Timestamp::from_second(i64::from(ts) + 1).unwrap(),
                stats: StatsResult {
                    mean_ns_per_iter: f64::from(ts),
                    ci95_half_width_ns: 1.0,
                    mode: EstimationMode::PerIter,
                    intercept_ns: None,
                    outlier_count: 0,
                    temporal_correlation: 0.0,
                    samples: vec![Sample {
                        iters: 10,
                        total_ns: 100,
                    }],
                },
            }],
            checksum: None,
        };

        let series = [
            mk_series("x", "a", 1_000),
            mk_series("y", "a", 2_000),
            mk_series("x", "b", 3_000),
        ];

        for s in series {
            storage.write_run_series_json(s.clone()).unwrap();
            storage
                .write_transaction(|tx| {
                    storage.insert_run_series(tx, &s)?;
                    storage.upsert_results(
                        tx,
                        &crate::storage::ResultsRow {
                            bench: s.bench.clone(),
                            config: s.config.clone(),
                            stable_series_timestamp: s.timestamp,
                            last_series_timestamp: s.timestamp,
                            suspicious_count: 0,
                            matched_count: 0,
                            replaced_count: 0,
                        },
                    )
                })
                .unwrap();
        }

        let engine = StatsEngine::new(config_file);
        let err = engine.timeline(&bench, &Config::new()).unwrap_err();
        assert!(matches!(err, StatsEngineError::MultipleVaryingKeys(_)));
    }

    #[test]
    fn test_timeline_summary() {
        let dir = TempDir::new().unwrap();
        let host = "h1";
        let json = r#"{
            "config_keys": {
                "build": { "values": ["a", "b", "c", "d", "e", "f"] }
            },
            "benchmarks": [
                {
                    "benchmark": "bench",
                    "command": ["echo", "{build}"],
                    "config": { "build": ["a", "b", "c", "d", "e", "f"] }
                }
            ]
        }"#;

        let config_file = ConfigFile::from_str(dir.path(), Some(host), json).unwrap();
        let storage = HybridDiskStorage::new(config_file.clone(), host).unwrap();
        let bench: BenchmarkId = "bench".try_into().unwrap();

        let mk_series = |build: &str, mean: f64, ts: i64| crate::run::RunSeries {
            schema: 1,
            bench: bench.clone(),
            config: config_file
                .config_from_string(&format!("build={build},host={host}"))
                .unwrap(),
            timestamp: Timestamp::from_second(ts).unwrap(),
            runs: vec![Run {
                timestamp: Timestamp::from_second(ts + 1).unwrap(),
                stats: StatsResult {
                    mean_ns_per_iter: mean,
                    ci95_half_width_ns: 1.0,
                    mode: EstimationMode::PerIter,
                    intercept_ns: None,
                    outlier_count: 0,
                    temporal_correlation: 0.0,
                    samples: vec![Sample {
                        iters: 10,
                        total_ns: 100,
                    }],
                },
            }],
            checksum: None,
        };

        let series = [
            mk_series("a", 100.0, 1_000),
            mk_series("b", 106.0, 2_000),
            mk_series("c", 111.0, 3_000), // REGRESSION
            mk_series("d", 120.0, 4_000),
            mk_series("e", 123.0, 5_000), // REGRESSION
            mk_series("f", 50.0, 6_000),  // IMPROVEMENT
        ];

        for s in series {
            storage.write_run_series_json(s.clone()).unwrap();
            storage
                .write_transaction(|tx| {
                    storage.insert_run_series(tx, &s)?;
                    storage.upsert_results(
                        tx,
                        &crate::storage::ResultsRow {
                            bench: s.bench.clone(),
                            config: s.config.clone(),
                            stable_series_timestamp: s.timestamp,
                            last_series_timestamp: s.timestamp,
                            suspicious_count: 0,
                            matched_count: 0,
                            replaced_count: 0,
                        },
                    )
                })
                .unwrap();
        }

        let engine = StatsEngine::new(config_file);
        let summary = engine
            .timeline_summary_with_threshold(&bench, &Config::new(), 0.10)
            .unwrap();

        assert_eq!(summary.changes.len(), 3); // initial + two significant changes
        assert_eq!(summary.omitted, 2); // one insignificant point

        assert_eq!(summary.initial.comparison_value.value_name(), "a");

        assert_eq!(summary.changes[0].0.comparison_value.value_name(), "c");
        assert_eq!(summary.changes[0].1.direction, ChangeDirection::Regression);
        assert!((summary.changes[0].1.rel_change - 0.1100).abs() < 1e-4);

        assert_eq!(summary.changes[1].0.comparison_value.value_name(), "e");
        assert_eq!(summary.changes[1].1.direction, ChangeDirection::Regression);
        assert!((summary.changes[1].1.rel_change - 0.1081).abs() < 1e-4);

        assert_eq!(summary.changes[2].0.comparison_value.value_name(), "f");
        assert_eq!(summary.changes[2].1.direction, ChangeDirection::Improvement);
        assert!((summary.changes[2].1.rel_change - 0.5935).abs() < 1e-4);
    }

    #[test]
    fn test_fastest_configs_selects_min_per_bench() {
        let (_dir, engine) = setup_storage("h1");

        let fastest = engine
            .fastest_configs(None, &Config::new())
            .expect("query succeeds");

        assert_eq!(fastest.len(), 2);

        let bench: BenchmarkId = "bench".try_into().unwrap();
        let bench2: BenchmarkId = "bench2".try_into().unwrap();

        assert_eq!(fastest[0].bench, bench);
        assert_eq!(fastest[0].config.to_string(), "build=x,host=h1");
        assert!((fastest[0].stable_stats.median_run_mean_ns - 1_700_000_010.0).abs() < 1e-4);

        assert_eq!(fastest[1].bench, bench2);
        assert_eq!(fastest[1].config.to_string(), "build=x,host=h1");
        assert!((fastest[1].stable_stats.median_run_mean_ns - 1_700_000_210.0).abs() < 1e-4);
    }

    #[test]
    fn test_fastest_configs_errors_on_empty() {
        let (_dir, engine) = setup_storage("h1");
        let missing: BenchmarkId = "missing".try_into().unwrap();

        let err = engine
            .fastest_configs(Some(&missing), &Config::new())
            .unwrap_err();

        assert!(matches!(err, StatsEngineError::NoResults));
    }
}

use super::error::WebExportError;
use super::format::{
    HistoryRow, ResultRow, WebBenchmarkEntry, WebConfigKey, WebHostIndex, WebIndexedHistory,
    WebIndexedResults,
};
use crate::config::{Benchmark, BenchmarkId, Config, ConfigFile, Key, KeyValue};
use crate::storage::{HybridDiskStorage, MeasurementId, ResultsRowWithStats, StorageRead};
use ahash::{HashMap, HashMapExt as _};
use jiff::Timestamp;
use std::collections::BTreeMap;
use std::ops::ControlFlow;

/// Collect and build all web export data for a single host.
///
/// `on_history` is called once per benchmark (in config file order) with the benchmark's
/// history data. Each call completes before the next benchmark's history is loaded, so only
/// one benchmark's history is in memory at a time.
pub fn export_host<E>(
    config_file: &ConfigFile,
    host_name: &str,
    mut on_history: impl FnMut(&BenchmarkId, WebIndexedHistory) -> Result<(), E>,
) -> Result<HostExportData, E>
where
    E: From<WebExportError>,
{
    let storage = HybridDiskStorage::new(config_file.clone(), host_name)
        .map_err(|e| E::from(WebExportError::Storage(e)))?;

    let mut all_results = collect_all_results(&storage).map_err(E::from)?;
    all_results.retain(|row| {
        config_file
            .benchmark_by_id(&row.row.bench)
            .is_some_and(|benchmark| benchmark.valid_config(&row.row.config))
    });

    // Group results by benchmark (HashMap: only used for lookups, order comes from config file)
    let mut by_benchmark: HashMap<BenchmarkId, Vec<&ResultsRowWithStats>> = HashMap::new();
    for row in &all_results {
        by_benchmark
            .entry(row.row.bench.clone())
            .or_default()
            .push(row);
    }

    let host_key = config_file.host_key();
    let timeline_key = config_file.timeline_key();

    // Build benchmark list (ordered by config file order, filtering to those with results)
    let benchmarks: Vec<&Benchmark> = config_file
        .benchmarks()
        .iter()
        .filter(|b| by_benchmark.contains_key(b.id()))
        .collect();

    // Config indices are benchmark-local; the host table only supplies value ordering.
    let config_indexes: Vec<ConfigIndex> = benchmarks
        .iter()
        .map(|benchmark| ConfigIndex::from_benchmark(benchmark))
        .collect();
    let mut measurement_tokens = MeasurementTokens::default();

    // Build indexed results
    let results = build_indexed_results(
        &all_results,
        &benchmarks,
        &config_indexes,
        &mut measurement_tokens,
    );

    // Build latest results (same config index as everything else)
    let latest_results = timeline_key.as_ref().and_then(|tk| {
        build_indexed_latest(
            &all_results,
            tk,
            &benchmarks,
            &config_indexes,
            &mut measurement_tokens,
        )
    });

    let index = build_host_index(
        config_file,
        &all_results,
        &by_benchmark,
        host_key,
        latest_results,
    );

    let compact = WebIndexedResults { results };

    // Build and emit history one benchmark at a time to avoid holding all history in memory.
    for (benchmark, config_index) in benchmarks.iter().zip(&config_indexes) {
        let h = build_indexed_history(&storage, benchmark, config_index, &mut measurement_tokens)
            .map_err(E::from)?;
        on_history(benchmark.id(), h)?;
    }

    Ok(HostExportData { index, compact })
}

/// Build the list of host names from the config file.
#[must_use]
pub fn host_names(config_file: &ConfigFile) -> Vec<String> {
    config_file
        .host_key()
        .values()
        .map(|kv| kv.value_name().to_string())
        .collect()
}

/// All export data for a single host (excluding history, which is streamed via the `on_history`
/// callback in [`export_host`]).
pub struct HostExportData {
    pub index: WebHostIndex,
    pub compact: WebIndexedResults,
}

/// Maps one benchmark's config combinations to an integer using host-wide value ordinals.
struct ConfigIndex {
    keys: Vec<Key>,
    strides: Vec<usize>,
}

impl ConfigIndex {
    fn from_benchmark(benchmark: &Benchmark) -> Self {
        let keys: Vec<Key> = benchmark.config_keys().cloned().collect();
        let values: Vec<Vec<String>> = keys
            .iter()
            .map(|key| {
                key.values()
                    .map(|value| value.value_name().to_string())
                    .collect()
            })
            .collect();

        let strides = compute_strides(&values);
        ConfigIndex { keys, strides }
    }

    fn encode(&self, config: &Config) -> usize {
        let mut idx = 0;
        for (i, key) in self.keys.iter().enumerate() {
            let value_idx = config
                .get(key)
                .expect("config was validated against the current benchmark")
                .value_index();
            idx += value_idx * self.strides[i];
        }
        idx
    }
}

/// Opaque equality tokens for shared measurements in one host snapshot.
#[derive(Default)]
struct MeasurementTokens {
    by_id: HashMap<MeasurementId, u32>,
}

impl MeasurementTokens {
    fn token(&mut self, measurement_id: MeasurementId, is_shared: bool) -> u32 {
        if !is_shared {
            return 0;
        }
        if let Some(token) = self.by_id.get(&measurement_id) {
            return *token;
        }
        let token = u32::try_from(self.by_id.len() + 1)
            .expect("one host export has more than u32::MAX shared measurements");
        self.by_id.insert(measurement_id, token);
        token
    }
}

fn compute_strides(values: &[Vec<String>]) -> Vec<usize> {
    let n = values.len();
    let mut strides = vec![1; n];
    for i in (0..n.saturating_sub(1)).rev() {
        strides[i] = strides[i + 1] * values[i + 1].len().max(1);
    }
    strides
}

fn build_host_index(
    config_file: &ConfigFile,
    all_results: &[ResultsRowWithStats],
    by_benchmark: &HashMap<BenchmarkId, Vec<&ResultsRowWithStats>>,
    host_key: &Key,
    latest_results: Option<Vec<ResultRow>>,
) -> WebHostIndex {
    let host_config_keys: Vec<&Key> = config_file
        .config_keys()
        .iter()
        .filter(|key| *key != host_key)
        .collect();
    let key_indexes: HashMap<&Key, usize> = host_config_keys
        .iter()
        .enumerate()
        .map(|(index, key)| (*key, index))
        .collect();
    let benchmarks: Vec<WebBenchmarkEntry> = config_file
        .benchmarks()
        .iter()
        .filter_map(|b| {
            let rows = by_benchmark.get(b.id())?;
            Some(WebBenchmarkEntry {
                name: b.id().clone(),
                result_count: rows.len(),
                config_keys: b.config_keys().map(|key| key_indexes[key]).collect(),
            })
        })
        .collect();

    let last_updated = all_results
        .iter()
        .map(|r| r.row.last_measurement_timestamp.as_second())
        .max()
        .unwrap_or_else(|| Timestamp::now().as_second());

    WebHostIndex {
        last_updated,
        description: None,
        config_keys: host_config_keys
            .iter()
            .map(|k| {
                let annotations: BTreeMap<String, String> = k
                    .annotations()
                    .map(|(kv, ann)| (kv.value_name().to_string(), ann.to_string()))
                    .collect();
                (
                    k.name().to_string(),
                    WebConfigKey {
                        values: k
                            .values()
                            .map(|value| value.value_name().to_string())
                            .collect(),
                        annotations,
                    },
                )
            })
            .collect(),
        benchmarks,
        timeline_key: config_file.timeline_key().map(|k| k.name().to_string()),
        latest_results,
    }
}

fn build_indexed_results(
    all_results: &[ResultsRowWithStats],
    benchmarks: &[&Benchmark],
    config_indexes: &[ConfigIndex],
    measurement_tokens: &mut MeasurementTokens,
) -> Vec<ResultRow> {
    let bench_idx_map: HashMap<&str, usize> = benchmarks
        .iter()
        .enumerate()
        .map(|(i, b)| (b.id().as_str(), i))
        .collect();

    all_results
        .iter()
        .filter_map(|r| {
            let bench_idx = *bench_idx_map.get(r.row.bench.as_str())?;
            let config_idx = config_indexes[bench_idx].encode(&r.row.config);
            Some(ResultRow {
                bench_idx,
                config_idx,
                measurement_token: measurement_tokens.token(r.stable_measurement_id, r.is_shared),
                mean_ns: round_ns(r.stable_stats.median_run_mean_ns),
                ci95_half_ns: round_ns(r.stable_stats.median_run_ci95_half_ns),
            })
        })
        .collect()
}

fn build_indexed_latest(
    all_results: &[ResultsRowWithStats],
    timeline_key: &Key,
    benchmarks: &[&Benchmark],
    config_indexes: &[ConfigIndex],
    measurement_tokens: &mut MeasurementTokens,
) -> Option<Vec<ResultRow>> {
    let latest_value = timeline_key.values().last()?;

    let bench_idx_map: HashMap<&str, usize> = benchmarks
        .iter()
        .enumerate()
        .map(|(i, b)| (b.id().as_str(), i))
        .collect();

    let results: Vec<ResultRow> = all_results
        .iter()
        .filter(|r| {
            r.row.config.get(timeline_key).map(KeyValue::value_name)
                == Some(latest_value.value_name())
        })
        .filter_map(|r| {
            let bench_idx = *bench_idx_map.get(r.row.bench.as_str())?;
            let config_idx = config_indexes[bench_idx].encode(&r.row.config);
            Some(ResultRow {
                bench_idx,
                config_idx,
                measurement_token: measurement_tokens.token(r.stable_measurement_id, r.is_shared),
                mean_ns: round_ns(r.stable_stats.median_run_mean_ns),
                ci95_half_ns: round_ns(r.stable_stats.median_run_ci95_half_ns),
            })
        })
        .collect();

    Some(results)
}

fn build_indexed_history(
    storage: &HybridDiskStorage,
    benchmark: &Benchmark,
    config_index: &ConfigIndex,
    measurement_tokens: &mut MeasurementTokens,
) -> Result<WebIndexedHistory, WebExportError> {
    let mut series: Vec<HistoryRow> = Vec::new();

    storage.read_transaction(|tx| {
        storage.for_each_measurement_history(tx, benchmark.id(), |rows| {
            series.extend(
                rows.iter()
                    .filter(|r| benchmark.valid_config(&r.config))
                    .map(|r| {
                        let config_idx = config_index.encode(&r.config);
                        HistoryRow {
                            config_idx,
                            measurement_token: measurement_tokens
                                .token(r.measurement_id, r.is_shared),
                            timestamp_s: r.timestamp.as_second(),
                            mean_ns: round_ns(r.median_run_mean_ns),
                            ci95_half_ns: round_ns(r.median_run_ci95_half_ns),
                            run_count: r.run_count,
                        }
                    }),
            );
            ControlFlow::Continue(())
        })
    })?;

    Ok(WebIndexedHistory { series })
}

fn collect_all_results(
    storage: &HybridDiskStorage,
) -> Result<Vec<ResultsRowWithStats>, WebExportError> {
    let mut all_results: Vec<ResultsRowWithStats> = Vec::new();
    storage.read_transaction(|tx| {
        storage.for_each_result_with_stats(tx, None, &Config::new(), |rows| {
            all_results.extend_from_slice(rows);
            ControlFlow::Continue(())
        })
    })?;
    Ok(all_results)
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "export JSON format uses integer nanoseconds"
)]
fn round_ns(ns: f64) -> i64 {
    ns.round() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigFile;
    use crate::run::Run;
    use crate::stats::{EstimationMode, Sample, StatsOptions, StatsResult};
    use crate::storage::{MeasurementRecord, MeasurementStats, ResultsRow, Storage, WorkloadState};
    use crate::workload::{GroupSpec, Sha256, WorkloadIdentity};
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn setup_storage(host: &str) -> (TempDir, ConfigFile, HybridDiskStorage) {
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

        (dir, config_file, storage)
    }

    fn setup_with_timeline(host: &str) -> (TempDir, ConfigFile, HybridDiskStorage) {
        let dir = TempDir::new().unwrap();

        let json = r#"{
            "config_keys": {
                "commit": { "values": ["aaa", "bbb"] }
            },
            "timeline_key": "commit",
            "benchmarks": [
                {
                    "benchmark": "bench",
                    "command": ["echo", "{commit}"],
                    "config": { "commit": ["aaa", "bbb"] }
                }
            ]
        }"#;

        let config_file = ConfigFile::from_str(dir.path(), Some(host), json).unwrap();
        let storage = HybridDiskStorage::new(config_file.clone(), host).unwrap();

        (dir, config_file, storage)
    }

    fn setup_with_non_commit_timeline(host: &str) -> (TempDir, ConfigFile, HybridDiskStorage) {
        let dir = TempDir::new().unwrap();

        let json = r#"{
            "config_keys": {
                "compiler": { "values": ["stable", "nightly"] }
            },
            "timeline_key": "compiler",
            "benchmarks": [
                {
                    "benchmark": "bench",
                    "command": ["echo", "{compiler}"],
                    "config": { "compiler": ["stable", "nightly"] }
                }
            ]
        }"#;

        let config_file = ConfigFile::from_str(dir.path(), Some(host), json).unwrap();
        let storage = HybridDiskStorage::new(config_file.clone(), host).unwrap();

        (dir, config_file, storage)
    }

    fn mk_series(
        config_file: &ConfigFile,
        host: &str,
        bench: &str,
        config_str: &str,
        mean: f64,
        ts: i64,
    ) -> crate::run::RunSeries {
        crate::run::RunSeries {
            schema: 1,
            bench: bench.try_into().unwrap(),
            config: config_file
                .config_from_string(&format!("{config_str},host={host}"))
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
        }
    }

    fn insert(storage: &HybridDiskStorage, series: &crate::run::RunSeries) {
        crate::storage::seed_measurement(storage, series);
        crate::storage::seed_result(
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
    }

    #[test]
    fn test_build_host_index() {
        let (_dir, config_file, storage) = setup_storage("h1");

        let s1 = mk_series(&config_file, "h1", "bench", "build=x", 100.0, 1000);
        let s2 = mk_series(&config_file, "h1", "bench", "build=y", 200.0, 2000);
        let s3 = mk_series(&config_file, "h1", "bench2", "build=x", 300.0, 3000);
        insert(&storage, &s1);
        insert(&storage, &s2);
        insert(&storage, &s3);

        let data = export_host(&config_file, "h1", |_, _| Ok::<(), WebExportError>(())).unwrap();

        assert_eq!(data.index.last_updated, 3000);
        assert_eq!(data.index.benchmarks.len(), 2);
        assert_eq!(data.index.benchmarks[0].name.as_str(), "bench");
        assert_eq!(data.index.benchmarks[0].result_count, 2);
        assert_eq!(data.index.benchmarks[0].config_keys, vec![0]);
        assert_eq!(data.index.benchmarks[1].name.as_str(), "bench2");
        assert_eq!(data.index.benchmarks[1].result_count, 1);
        assert_eq!(data.index.benchmarks[1].config_keys, vec![0]);
        assert!(data.index.config_keys.contains_key("build"));
        assert!(!data.index.config_keys.contains_key("host"));
        assert_eq!(data.index.timeline_key, None);
        assert!(data.index.latest_results.is_none());
    }

    #[test]
    fn test_export_annotations() {
        let dir = TempDir::new().unwrap();
        let json = r#"{
            "config_keys": {
                "build": {
                    "values": ["x", "y"],
                    "annotations": { "y": "optimized" }
                }
            },
            "benchmarks": [
                {
                    "benchmark": "bench",
                    "command": ["echo", "{build}"],
                    "config": { "build": ["x", "y"] }
                }
            ]
        }"#;
        let config_file = ConfigFile::from_str(dir.path(), Some("h1"), json).unwrap();
        let storage = HybridDiskStorage::new(config_file.clone(), "h1").unwrap();

        let s = mk_series(&config_file, "h1", "bench", "build=x", 100.0, 1000);
        insert(&storage, &s);

        let data = export_host(&config_file, "h1", |_, _| Ok::<(), WebExportError>(())).unwrap();

        let build_key = &data.index.config_keys["build"];
        assert_eq!(build_key.annotations.len(), 1);
        assert_eq!(build_key.annotations["y"], "optimized");
    }

    #[test]
    fn test_indexed_results_format() {
        let (_dir, config_file, storage) = setup_storage("h1");

        let s1 = mk_series(&config_file, "h1", "bench", "build=x", 100.5, 1000);
        let s2 = mk_series(&config_file, "h1", "bench", "build=y", 200.7, 2000);
        insert(&storage, &s1);
        insert(&storage, &s2);

        let data = export_host(&config_file, "h1", |_, _| Ok::<(), WebExportError>(())).unwrap();

        // Config keys/values are in index.json, not results.json
        assert!(data.index.config_keys.contains_key("build"));
        assert_eq!(data.compact.results.len(), 2);

        // First result: bench_idx=0, config_idx=0 (build=x), mean=101, ci=1
        let r0 = &data.compact.results[0];
        assert_eq!(r0.bench_idx, 0);
        assert_eq!(r0.config_idx, 0); // x=0
        assert_eq!(r0.measurement_token, 0); // isolated
        assert_eq!(r0.mean_ns, 101); // round(100.5)
        assert_eq!(r0.ci95_half_ns, 1); // round(1.0)

        // Second result: bench_idx=0, config_idx=1 (build=y), mean=201, ci=1
        let r1 = &data.compact.results[1];
        assert_eq!(r1.bench_idx, 0);
        assert_eq!(r1.config_idx, 1); // y=1
        assert_eq!(r1.measurement_token, 0); // isolated
        assert_eq!(r1.mean_ns, 201); // round(200.7)
    }

    #[test]
    fn test_indexed_latest_in_index() {
        let (_dir, config_file, storage) = setup_with_timeline("h1");

        let s1 = mk_series(&config_file, "h1", "bench", "commit=aaa", 100.0, 1000);
        let s2 = mk_series(&config_file, "h1", "bench", "commit=bbb", 200.0, 2000);
        insert(&storage, &s1);
        insert(&storage, &s2);

        let data = export_host(&config_file, "h1", |_, _| Ok::<(), WebExportError>(())).unwrap();
        let latest = data
            .index
            .latest_results
            .as_ref()
            .expect("should have latest");

        // Should only contain the latest commit's results
        assert_eq!(latest.len(), 1);
        let r0 = &latest[0];
        assert_eq!(r0.bench_idx, 0);
        assert_eq!(r0.config_idx, 1); // commit=bbb, same index space as results
        assert_eq!(r0.mean_ns, 200);
    }

    #[test]
    fn test_indexed_latest_uses_timeline_key_not_commit() {
        let (_dir, config_file, storage) = setup_with_non_commit_timeline("h1");

        let s1 = mk_series(&config_file, "h1", "bench", "compiler=stable", 100.0, 1000);
        let s2 = mk_series(&config_file, "h1", "bench", "compiler=nightly", 200.0, 2000);
        insert(&storage, &s1);
        insert(&storage, &s2);

        let data = export_host(&config_file, "h1", |_, _| Ok::<(), WebExportError>(())).unwrap();
        let latest = data
            .index
            .latest_results
            .as_ref()
            .expect("should have latest");

        assert_eq!(data.index.timeline_key.as_deref(), Some("compiler"));
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].bench_idx, 0);
        assert_eq!(latest[0].config_idx, 1); // compiler=nightly, same index space as results
        assert_eq!(latest[0].mean_ns, 200);
    }

    #[test]
    fn test_indexed_history() {
        let (_dir, config_file, storage) = setup_storage("h1");

        let s1 = mk_series(&config_file, "h1", "bench", "build=x", 100.0, 1000);
        let s2 = mk_series(&config_file, "h1", "bench", "build=x", 110.0, 2000);
        let s3 = mk_series(&config_file, "h1", "bench", "build=y", 200.0, 1500);
        insert(&storage, &s1);
        insert(&storage, &s2);
        insert(&storage, &s3);

        let mut history_map = BTreeMap::new();
        export_host(&config_file, "h1", |bench_id, history| {
            history_map.insert(bench_id.clone(), history);
            Ok::<(), WebExportError>(())
        })
        .unwrap();
        let history = history_map.get(&"bench".try_into().unwrap()).unwrap();

        assert_eq!(history.series.len(), 3);

        // Ordered by timestamp
        let s0 = &history.series[0];
        assert_eq!(s0.config_idx, 0); // build=x
        assert_eq!(s0.measurement_token, 0); // isolated
        assert_eq!(s0.timestamp_s, 1000);
        assert_eq!(s0.run_count, 1);

        let s1 = &history.series[1];
        assert_eq!(s1.config_idx, 1); // build=y
        assert_eq!(s1.timestamp_s, 1500);

        let s2 = &history.series[2];
        assert_eq!(s2.config_idx, 0); // build=x
        assert_eq!(s2.timestamp_s, 2000);
    }

    #[test]
    fn export_omits_cases_outside_the_current_benchmark_config() {
        let dir = TempDir::new().unwrap();
        let json = r#"{
            "config_keys": {
                "build": { "values": ["x", "y"] },
                "old": { "values": ["z"] }
            },
            "benchmarks": [
                {
                    "benchmark": "bench",
                    "command": ["echo", "{build}"],
                    "config": { "build": ["x"] }
                }
            ]
        }"#;
        let config_file = ConfigFile::from_str(dir.path(), Some("h1"), json).unwrap();
        let storage = HybridDiskStorage::new(config_file.clone(), "h1").unwrap();

        let valid = mk_series(&config_file, "h1", "bench", "build=x", 100.0, 1000);
        let missing_key = mk_series(&config_file, "h1", "bench", "old=z", 200.0, 2000);
        let extra_key = mk_series(&config_file, "h1", "bench", "build=x,old=z", 300.0, 3000);
        let excluded_value = mk_series(&config_file, "h1", "bench", "build=y", 400.0, 4000);
        for series in [&valid, &missing_key, &extra_key, &excluded_value] {
            insert(&storage, series);
        }

        let mut history = None;
        let data = export_host(&config_file, "h1", |_, rows| {
            history = Some(rows);
            Ok::<(), WebExportError>(())
        })
        .unwrap();

        assert_eq!(data.index.last_updated, 1000);
        assert_eq!(data.index.benchmarks.len(), 1);
        assert_eq!(data.index.benchmarks[0].result_count, 1);
        assert_eq!(data.compact.results.len(), 1);
        assert_eq!(data.compact.results[0].config_idx, 0);

        let history = history.unwrap();
        assert_eq!(history.series.len(), 1);
        assert_eq!(history.series[0].config_idx, 0);
        assert_eq!(history.series[0].timestamp_s, 1000);
    }

    #[test]
    fn test_export_host_produces_all_data() {
        let (_dir, config_file, storage) = setup_storage("h1");

        let s1 = mk_series(&config_file, "h1", "bench", "build=x", 100.0, 1000);
        insert(&storage, &s1);

        let mut history_count = 0usize;
        let data = export_host(&config_file, "h1", |_, _| {
            history_count += 1;
            Ok::<(), WebExportError>(())
        })
        .unwrap();

        assert_eq!(data.compact.results.len(), 1);
        assert!(data.index.latest_results.is_none());
        assert_eq!(history_count, 1);
    }

    #[test]
    fn measurement_tokens_reuse_shared_ids_and_reserve_zero_for_isolated() {
        let bench: BenchmarkId = "bench".try_into().unwrap();
        let first = MeasurementId::for_v1(&bench, "", Timestamp::from_second(1).unwrap());
        let second = MeasurementId::for_v1(&bench, "", Timestamp::from_second(2).unwrap());
        let mut tokens = MeasurementTokens::default();

        assert_eq!(tokens.token(first, false), 0);
        assert_eq!(tokens.token(first, true), 1);
        assert_eq!(tokens.token(first, true), 1);
        assert_eq!(tokens.token(second, true), 2);
        assert_eq!(tokens.token(second, false), 0);
    }

    #[test]
    fn shared_measurement_uses_one_token_across_results_and_history() {
        let (_dir, config_file, storage) = setup_storage("h1");
        let bench: BenchmarkId = "bench".try_into().unwrap();
        let configs = ["build=x", "build=y"].map(|value| {
            config_file
                .config_from_string(value)
                .unwrap()
                .without_host_key()
        });
        let identity = WorkloadIdentity::shared(
            bench.clone(),
            Sha256::hash_bytes(b"executable"),
            None,
            &GroupSpec::new(Vec::new(), None, StatsOptions::default()),
        );
        let timestamp = Timestamp::from_second(1234).unwrap();
        let measurement_id = MeasurementId::for_v1(&bench, "shared", timestamp);

        storage
            .write_transaction(|tx| {
                let workload = storage.intern_workload(tx, &identity)?;
                let cases: Vec<_> = configs
                    .iter()
                    .map(|config| {
                        storage.get_or_create_case(
                            tx,
                            bench.as_str(),
                            &serde_json::to_string(config).unwrap(),
                        )
                    })
                    .collect::<Result<_, _>>()?;
                storage.insert_measurement(
                    tx,
                    &MeasurementRecord {
                        measurement_id,
                        workload_id: workload,
                        timestamp,
                        schema_version: 2,
                        stats: MeasurementStats {
                            run_count: 1,
                            median_run_mean_ns: 10.0,
                            median_run_ci95_half_ns: 1.0,
                            median_run_outlier_count: 0,
                            median_run_sample_count: 1,
                        },
                        checksum: None,
                    },
                )?;
                storage.link_measurement_cases(tx, measurement_id, &cases)?;
                storage.set_workload_state(
                    tx,
                    &WorkloadState {
                        workload_id: workload,
                        stable_measurement_id: measurement_id,
                        last_measurement_id: measurement_id,
                        matched_count: 0,
                        suspicious_count: 0,
                        replaced_count: 0,
                    },
                )?;
                for case in cases {
                    storage.set_case_workload(tx, case, workload)?;
                }
                Ok(())
            })
            .unwrap();

        let mut history = None;
        let data = export_host(&config_file, "h1", |_, rows| {
            history = Some(rows);
            Ok::<(), WebExportError>(())
        })
        .unwrap();

        assert_eq!(
            data.compact
                .results
                .iter()
                .map(|row| row.measurement_token)
                .collect::<Vec<_>>(),
            vec![1, 1]
        );
        assert_eq!(
            history
                .unwrap()
                .series
                .iter()
                .map(|row| row.measurement_token)
                .collect::<Vec<_>>(),
            vec![1, 1]
        );
    }
}

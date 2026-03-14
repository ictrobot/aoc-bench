use super::error::WebExportError;
use super::format::{
    HistoryRow, ResultRow, WebBenchmarkEntry, WebConfigKey, WebHostIndex, WebIndexedHistory,
    WebIndexedResults,
};
use crate::config::{BenchmarkId, Config, ConfigFile, Key, KeyValue};
use crate::storage::{HybridDiskStorage, ResultsRowWithStats, StorageRead};
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

    let all_results = collect_all_results(&storage).map_err(E::from)?;

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
    let benchmarks: Vec<BenchmarkId> = config_file
        .benchmarks()
        .iter()
        .filter(|b| by_benchmark.contains_key(b.id()))
        .map(|b| b.id().clone())
        .collect();

    // Build config index excluding host key
    let config_index = ConfigIndex::from_config_file(config_file, host_key);

    // Build indexed results
    let results = build_indexed_results(&all_results, &benchmarks, &config_index);

    // Build latest results (same config index as everything else)
    let latest_results = timeline_key
        .as_ref()
        .and_then(|tk| build_indexed_latest(&all_results, tk, &benchmarks, &config_index));

    let index = build_host_index(
        config_file,
        &all_results,
        &by_benchmark,
        &config_index,
        latest_results,
    );

    let compact = WebIndexedResults { results };

    // Build and emit history one benchmark at a time to avoid holding all history in memory.
    for bench_id in &benchmarks {
        let h = build_indexed_history(&storage, bench_id, &config_index).map_err(E::from)?;
        on_history(bench_id, h)?;
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

/// Maps config key-value combinations to a single integer index using mixed-radix encoding.
struct ConfigIndex {
    keys: Vec<Key>,
    values: Vec<Vec<String>>,
    strides: Vec<usize>,
}

impl ConfigIndex {
    fn from_config_file(config_file: &ConfigFile, exclude: &Key) -> Self {
        let mut keys = Vec::new();
        let mut values = Vec::new();

        for k in config_file.config_keys() {
            if k == exclude {
                continue;
            }
            keys.push(k.clone());
            values.push(k.values().map(|v| v.value_name().to_string()).collect());
        }

        let strides = compute_strides(&values);
        ConfigIndex {
            keys,
            values,
            strides,
        }
    }

    fn encode(&self, config: &Config) -> usize {
        let mut idx = 0;
        for (i, key) in self.keys.iter().enumerate() {
            // Missing keys are encoded as 0. This supports benchmarks that don't vary over all keys.
            let value_idx = config.get(key).map_or(0, KeyValue::value_index);
            idx += value_idx * self.strides[i];
        }
        idx
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
    config_index: &ConfigIndex,
    latest_results: Option<Vec<ResultRow>>,
) -> WebHostIndex {
    let benchmarks: Vec<WebBenchmarkEntry> = config_file
        .benchmarks()
        .iter()
        .filter_map(|b| {
            let rows = by_benchmark.get(b.id())?;
            Some(WebBenchmarkEntry {
                name: b.id().clone(),
                result_count: rows.len(),
            })
        })
        .collect();

    let last_updated = all_results
        .iter()
        .map(|r| r.row.last_series_timestamp.as_second())
        .max()
        .unwrap_or_else(|| Timestamp::now().as_second());

    WebHostIndex {
        last_updated,
        description: None,
        config_keys: config_index
            .keys
            .iter()
            .zip(config_index.values.iter())
            .map(|(k, v)| {
                let annotations: BTreeMap<String, String> = k
                    .annotations()
                    .map(|(kv, ann)| (kv.value_name().to_string(), ann.to_string()))
                    .collect();
                (
                    k.name().to_string(),
                    WebConfigKey {
                        values: v.clone(),
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
    benchmarks: &[BenchmarkId],
    config_index: &ConfigIndex,
) -> Vec<ResultRow> {
    let bench_idx_map: HashMap<&str, usize> = benchmarks
        .iter()
        .enumerate()
        .map(|(i, b)| (b.as_str(), i))
        .collect();

    all_results
        .iter()
        .filter_map(|r| {
            let bench_idx = *bench_idx_map.get(r.row.bench.as_str())?;
            let config_idx = config_index.encode(&r.row.config);
            Some(ResultRow {
                bench_idx,
                config_idx,
                mean_ns: round_ns(r.stable_stats.median_run_mean_ns),
                ci95_half_ns: round_ns(r.stable_stats.median_run_ci95_half_ns),
            })
        })
        .collect()
}

fn build_indexed_latest(
    all_results: &[ResultsRowWithStats],
    timeline_key: &Key,
    benchmarks: &[BenchmarkId],
    config_index: &ConfigIndex,
) -> Option<Vec<ResultRow>> {
    let latest_value = timeline_key.values().last()?;

    let bench_idx_map: HashMap<&str, usize> = benchmarks
        .iter()
        .enumerate()
        .map(|(i, b)| (b.as_str(), i))
        .collect();

    let results: Vec<ResultRow> = all_results
        .iter()
        .filter(|r| {
            r.row.config.get(timeline_key).map(KeyValue::value_name)
                == Some(latest_value.value_name())
        })
        .filter_map(|r| {
            let bench_idx = *bench_idx_map.get(r.row.bench.as_str())?;
            let config_idx = config_index.encode(&r.row.config);
            Some(ResultRow {
                bench_idx,
                config_idx,
                mean_ns: round_ns(r.stable_stats.median_run_mean_ns),
                ci95_half_ns: round_ns(r.stable_stats.median_run_ci95_half_ns),
            })
        })
        .collect();

    Some(results)
}

fn build_indexed_history(
    storage: &HybridDiskStorage,
    bench_id: &BenchmarkId,
    config_index: &ConfigIndex,
) -> Result<WebIndexedHistory, WebExportError> {
    let mut series: Vec<HistoryRow> = Vec::new();

    storage.read_transaction(|tx| {
        storage.for_each_run_series(tx, bench_id, |rows| {
            series.extend(rows.iter().map(|r| {
                let config_idx = config_index.encode(&r.config);
                HistoryRow {
                    config_idx,
                    timestamp_s: r.timestamp.as_second(),
                    mean_ns: round_ns(r.median_run_mean_ns),
                    ci95_half_ns: round_ns(r.median_run_ci95_half_ns),
                    run_count: r.run_count,
                }
            }));
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
    use crate::stats::{EstimationMode, Sample, StatsResult};
    use crate::storage::{ResultsRow, Storage};
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
        storage.write_run_series_json(series.clone()).unwrap();
        storage
            .write_transaction(|tx| {
                storage.insert_run_series(tx, series)?;
                storage.upsert_results(
                    tx,
                    &ResultsRow {
                        bench: series.bench.clone(),
                        config: series.config.clone(),
                        stable_series_timestamp: series.timestamp,
                        last_series_timestamp: series.timestamp,
                        suspicious_count: 0,
                        matched_count: 0,
                        replaced_count: 0,
                    },
                )
            })
            .unwrap();
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
        assert_eq!(data.index.benchmarks[1].name.as_str(), "bench2");
        assert_eq!(data.index.benchmarks[1].result_count, 1);
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
        assert_eq!(r0.mean_ns, 101); // round(100.5)
        assert_eq!(r0.ci95_half_ns, 1); // round(1.0)

        // Second result: bench_idx=0, config_idx=1 (build=y), mean=201, ci=1
        let r1 = &data.compact.results[1];
        assert_eq!(r1.bench_idx, 0);
        assert_eq!(r1.config_idx, 1); // y=1
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
}

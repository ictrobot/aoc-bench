use crate::config::{BenchmarkId, Config, ConfigFile, KeyValue};
use crate::run::RunSeries;
use crate::storage::{PerHostStorage, ResultsRow, ResultsRowWithStats, StorageRead};
use jiff::Timestamp;
use std::ops::ControlFlow;

/// Read-only storage that fans out queries across all hosts.
#[derive(Debug)]
pub struct MultiHostStorage<S> {
    config_file: ConfigFile,
    // once_cell::sync::OnceCell is used instead of std for get_or_try_init
    host_storage: Vec<once_cell::sync::OnceCell<S>>,
}

impl<S: PerHostStorage> MultiHostStorage<S> {
    #[must_use]
    pub fn new(config_file: ConfigFile) -> Self {
        Self {
            host_storage: config_file
                .host_key()
                .values()
                .map(|_| once_cell::sync::OnceCell::new())
                .collect(),
            config_file,
        }
    }

    fn host_storage(&self, host: &KeyValue) -> Result<&S, MultiHostError<S>> {
        assert_eq!(host.key(), self.config_file.host_key());

        self.host_storage[host.value_index()]
            .get_or_try_init(|| S::new_for_host(self.config_file.clone(), host.value_name()))
            .map_err(|e| MultiHostError::BackendError {
                host: host.value_name().to_string(),
                source: e,
            })
    }

    fn host_storage_from_config(&self, config: &Config) -> Result<&S, MultiHostError<S>> {
        let host_kv = config
            .get(self.config_file.host_key())
            .ok_or_else(|| MultiHostError::ConfigMissingHostKey(config.to_string()))?;
        self.host_storage(host_kv)
    }

    fn for_each_host(
        &self,
        config_filter: &Config,
        mut f: impl FnMut(&S, &Config) -> Result<ControlFlow<()>, S::Error>,
    ) -> Result<(), MultiHostError<S>> {
        if let Some(host_kv) = config_filter.get(self.config_file.host_key()) {
            let storage = self.host_storage(host_kv)?;
            f(storage, config_filter)
                .map(|_| ())
                .map_err(|e| MultiHostError::BackendError {
                    host: host_kv.value_name().to_string(),
                    source: e,
                })
        } else {
            for host_kv in self.config_file.host_key().values() {
                let with_host = config_filter.with(host_kv.clone());
                if let ControlFlow::Break(()) = f(self.host_storage(&host_kv)?, &with_host)
                    .map_err(|e| MultiHostError::BackendError {
                        host: host_kv.value_name().to_string(),
                        source: e,
                    })?
                {
                    break;
                }
            }
            Ok(())
        }
    }
}

impl<S: PerHostStorage> StorageRead for MultiHostStorage<S> {
    type Tx<'a> = ();
    type Error = MultiHostError<S>;

    fn read_run_series_json(
        &self,
        bench: &BenchmarkId,
        config: &Config,
        timestamp: Timestamp,
    ) -> Result<RunSeries, Self::Error> {
        let storage = self.host_storage_from_config(config)?;

        storage
            .read_run_series_json(bench, config, timestamp)
            .map_err(|e| MultiHostError::BackendError {
                host: storage.host().value_name().to_string(),
                source: e,
            })
    }

    fn read_transaction<F, T>(&self, f: F) -> Result<T, Self::Error>
    where
        F: FnOnce(&Self::Tx<'_>) -> Result<T, Self::Error>,
    {
        f(&())
    }

    fn get_result_with_stats(
        &self,
        _tx: &Self::Tx<'_>,
        bench: &BenchmarkId,
        config: &Config,
    ) -> Result<Option<ResultsRowWithStats>, Self::Error> {
        let storage = self.host_storage_from_config(config)?;

        storage
            .read_transaction(|tx| storage.get_result_with_stats(tx, bench, config))
            .map_err(|e| MultiHostError::BackendError {
                host: storage.host().value_name().to_string(),
                source: e,
            })
    }

    fn for_each_result_with_stats(
        &self,
        _tx: &Self::Tx<'_>,
        benchmark_filter: Option<&BenchmarkId>,
        config_filter: &Config,
        mut f: impl FnMut(&[ResultsRowWithStats]) -> ControlFlow<()>,
    ) -> Result<(), Self::Error> {
        self.for_each_host(config_filter, |storage, config| {
            let mut control_flow = ControlFlow::Continue(());
            storage.read_transaction(|tx| {
                storage.for_each_result_with_stats(tx, benchmark_filter, config, |rows| {
                    // If f returns Break, stop iterating over this storage as well as any others
                    control_flow = f(rows);
                    control_flow
                })
            })?;
            Ok(control_flow)
        })
    }

    fn oldest_results(
        &self,
        _tx: &Self::Tx<'_>,
        benchmark_filter: Option<&BenchmarkId>,
        config_filter: &Config,
        limit: usize,
    ) -> Result<Vec<ResultsRow>, Self::Error> {
        let mut results = Vec::new();

        self.for_each_host(config_filter, |storage, config| {
            let mut host_rows = storage.read_transaction(|tx| {
                storage.oldest_results(tx, benchmark_filter, config, limit)
            })?;
            results.append(&mut host_rows);
            Ok(ControlFlow::Continue(()))
        })?;

        results.sort_unstable_by_key(|row| row.last_series_timestamp);
        results.truncate(limit);

        Ok(results)
    }

    fn missing_results(
        &self,
        _tx: &Self::Tx<'_>,
        benchmark_filter: Option<&BenchmarkId>,
        config_filter: &Config,
        limit: usize,
    ) -> Result<Vec<(BenchmarkId, Config)>, Self::Error> {
        let mut results = Vec::new();

        self.for_each_host(config_filter, |storage, config| {
            let mut host_rows = storage.read_transaction(|tx| {
                storage.missing_results(tx, benchmark_filter, config, limit)
            })?;
            results.append(&mut host_rows);

            Ok(if results.len() >= limit {
                results.truncate(limit);
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            })
        })?;

        Ok(results)
    }
}

/// [`MultiHostStorage`] errors.
#[derive(Debug, thiserror::Error)]
pub enum MultiHostError<S: PerHostStorage> {
    #[error("config missing host key: {0}")]
    ConfigMissingHostKey(String),
    #[error("error querying host {host}: {source}")]
    BackendError {
        host: String,
        #[source]
        source: S::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BenchmarkId, ConfigFile};
    use crate::run::Run;
    use crate::stats::{EstimationMode, Sample, StatsResult};
    use crate::storage::{HybridDiskStorage, Storage};
    use tempfile::TempDir;

    fn make_config(config_file: &ConfigFile, host: Option<&str>) -> Config {
        let build_key = config_file
            .config_keys()
            .iter()
            .find(|k| k.name() == "build")
            .unwrap();
        let build_kv = build_key.value_from_name("x").unwrap();
        let cfg = Config::new().with(build_kv);
        match host {
            Some(h) => cfg.with(config_file.host_key().value_from_name(h).unwrap()),
            None => cfg,
        }
    }

    fn setup_config(hosts: &[&str]) -> (TempDir, ConfigFile) {
        let dir = TempDir::new().unwrap();
        let results_dir = dir.path().join("results");
        for host in hosts {
            std::fs::create_dir_all(results_dir.join(host)).unwrap();
        }

        // Minimal benchmark with one config key to make configs non-empty
        let json = r#"{
            "config_keys": {
                "build": { "values": ["x", "y"] }
            },
            "benchmarks": [
                {
                    "benchmark": "bench",
                    "command": ["echo", "{build}"],
                    "config": { "build": ["x", "y"] }
                }
            ]
        }"#;

        let cfg = ConfigFile::from_str(dir.path(), None, json).unwrap();
        (dir, cfg)
    }

    fn sample_series(bench: BenchmarkId, config: Config, ts: i64) -> RunSeries {
        let timestamp = Timestamp::from_second(ts).unwrap();
        let run_ts = Timestamp::from_second(ts + 1).unwrap();
        let stats = StatsResult {
            mean_ns_per_iter: 10.0,
            ci95_half_width_ns: 1.0,
            mode: EstimationMode::PerIter,
            intercept_ns: None,
            outlier_count: 0,
            temporal_correlation: 0.0,
            samples: vec![Sample {
                iters: 10,
                total_ns: 100,
            }],
        };

        RunSeries {
            schema: 1,
            bench,
            config,
            timestamp,
            runs: vec![Run {
                timestamp: run_ts,
                stats,
            }],
            checksum: None,
        }
    }

    fn insert_row(storage: &HybridDiskStorage, series: &RunSeries) {
        let bench = series.bench.clone();
        let config = series.config.clone();
        let ts = series.timestamp;

        storage.write_run_series_json(series.clone()).unwrap();
        storage
            .write_transaction(|tx| {
                storage.insert_run_series(tx, series)?;
                storage.upsert_results(
                    tx,
                    &ResultsRow {
                        bench,
                        config: config.clone(),
                        stable_series_timestamp: ts,
                        last_series_timestamp: ts,
                        suspicious_count: 0,
                        matched_count: 0,
                        replaced_count: 0,
                    },
                )
            })
            .unwrap();
    }

    #[test]
    fn test_read_run_series_json_reads_back_series() {
        let (_dir, config_file) = setup_config(&["h1"]);
        let backend = HybridDiskStorage::new(config_file.clone(), "h1").unwrap();
        let cfg = make_config(&config_file, Some("h1"));
        let bench: BenchmarkId = "bench".try_into().unwrap();
        let ts = Timestamp::from_second(42).unwrap();

        let mut series = sample_series(bench.clone(), cfg.clone(), ts.as_second());
        series.timestamp = ts;
        backend.write_run_series_json(series.clone()).unwrap();

        let storage: MultiHostStorage<HybridDiskStorage> = MultiHostStorage::new(config_file);
        let loaded = storage
            .read_run_series_json(&bench, &cfg, ts)
            .expect("read series");
        assert_eq!(loaded.bench, bench);
        assert_eq!(loaded.config, cfg);
        assert_eq!(loaded.timestamp, ts);
    }

    #[test]
    fn test_get_result_with_stats_returns_joined_stats() {
        let (_dir, config_file) = setup_config(&["h1"]);
        let backend = HybridDiskStorage::new(config_file.clone(), "h1").unwrap();
        let cfg = make_config(&config_file, Some("h1"));
        let bench: BenchmarkId = "bench".try_into().unwrap();
        let ts = 100;
        let series = sample_series(bench.clone(), cfg.clone(), ts);
        insert_row(&backend, &series);

        let storage: MultiHostStorage<HybridDiskStorage> =
            MultiHostStorage::new(config_file.clone());
        let row = storage
            .get_result_with_stats(&(), &bench, &cfg)
            .expect("query")
            .expect("present");

        assert_eq!(row.row.bench, bench);
        assert_eq!(row.row.config, cfg);
        assert_eq!(row.stable_stats.run_count, 1);
        assert_eq!(row.last_stats.run_count, 1);
    }

    #[test]
    fn test_missing_host_key_is_reported() {
        let (_dir, config_file) = setup_config(&["h1"]);
        let storage: MultiHostStorage<HybridDiskStorage> = MultiHostStorage::new(config_file);

        let bench: BenchmarkId = "bench".try_into().unwrap();
        let err = storage
            .get_result_with_stats(&(), &bench, &Config::new())
            .unwrap_err();
        assert!(matches!(err, MultiHostError::ConfigMissingHostKey(_)));
    }

    #[test]
    fn test_backend_errors_include_host_name() {
        let (dir, config_file) = setup_config(&["h1"]);
        let host_dir = dir.path().join("results").join("h1");
        let db_path = host_dir.join("metadata.db");
        std::fs::create_dir_all(&db_path).unwrap(); // make path a directory so SQLite open fails

        let storage: MultiHostStorage<HybridDiskStorage> =
            MultiHostStorage::new(config_file.clone());
        let config = make_config(&config_file, Some("h1"));
        let bench: BenchmarkId = "bench".try_into().unwrap();

        let err = storage
            .get_result_with_stats(&(), &bench, &config)
            .unwrap_err();

        assert!(matches!(err, MultiHostError::BackendError { host, .. } if host == "h1"));
    }

    #[test]
    fn test_for_each_result_with_stats_iterates_hosts_and_respects_break() {
        let (_dir, config_file) = setup_config(&["h1", "h2"]);

        // Insert for both hosts
        for (host, ts) in [("h1", 5), ("h2", 10)] {
            let backend = HybridDiskStorage::new(config_file.clone(), host).unwrap();
            let cfg = make_config(&config_file, Some(host));
            let series = sample_series("bench".try_into().unwrap(), cfg, ts);
            insert_row(&backend, &series);
        }

        let storage: MultiHostStorage<HybridDiskStorage> =
            MultiHostStorage::new(config_file.clone());

        // Collect all rows
        let mut collected = Vec::new();
        storage
            .read_transaction(|tx| {
                storage.for_each_result_with_stats(tx, None, &Config::new(), |rows| {
                    collected.extend_from_slice(rows);
                    ControlFlow::Continue(())
                })
            })
            .unwrap();
        assert_eq!(collected.len(), 2);

        // Verify Break stops early
        let mut seen = 0usize;
        storage
            .read_transaction(|tx| {
                storage.for_each_result_with_stats(tx, None, &Config::new(), |rows| {
                    seen += rows.len();
                    ControlFlow::Break(())
                })
            })
            .unwrap();
        assert_eq!(seen, 1);
    }

    #[test]
    fn test_oldest_results_sorted_and_limited() {
        let (_dir, config_file) = setup_config(&["h1", "h2"]);
        let storage: MultiHostStorage<HybridDiskStorage> =
            MultiHostStorage::new(config_file.clone());

        // Host h1, later timestamp
        {
            let cfg = make_config(&config_file, Some("h1"));
            let series = sample_series("bench".try_into().unwrap(), cfg, 20);
            let backend = HybridDiskStorage::new(config_file.clone(), "h1").unwrap();
            insert_row(&backend, &series);
        }
        // Host h2, earlier timestamp
        {
            let cfg = make_config(&config_file, Some("h2"));
            let series = sample_series("bench".try_into().unwrap(), cfg, 10);
            let backend = HybridDiskStorage::new(config_file.clone(), "h2").unwrap();
            insert_row(&backend, &series);
        }

        let rows = storage
            .oldest_results(&(), None, &Config::new(), 1)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].last_series_timestamp.as_second(), 10);

        let rows = storage
            .oldest_results(&(), None, &Config::new(), 10)
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].last_series_timestamp.as_second(), 10);
        assert_eq!(rows[1].last_series_timestamp.as_second(), 20);
    }

    #[test]
    fn test_missing_results_respects_limit_and_breaks() {
        let (_dir, config_file) = setup_config(&["h1", "h2"]);
        let storage: MultiHostStorage<HybridDiskStorage> =
            MultiHostStorage::new(config_file.clone());

        // Insert one row for h1
        {
            let cfg = make_config(&config_file, Some("h1"));
            let series = sample_series("bench".try_into().unwrap(), cfg, 20);
            let backend = HybridDiskStorage::new(config_file.clone(), "h1").unwrap();
            insert_row(&backend, &series);
        }

        let rows = storage
            .missing_results(&(), None, &Config::new(), 10)
            .unwrap();
        assert_eq!(rows.len(), 3); // 1 for h1, 2 for h2
    }
}

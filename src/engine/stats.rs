use crate::config::{BenchmarkId, Config, ConfigFile, Key};
use crate::storage::{
    HybridDiskStorage, MultiHostError, MultiHostStorage, ResultsRowWithStats, StorageRead,
};
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
}

#[derive(Debug, thiserror::Error)]
pub enum StatsEngineError {
    #[error("failed to read storage: {0}")]
    StorageError(#[from] MultiHostError<HybridDiskStorage>),
    #[error("error writing output: {0}")]
    OutputError(#[source] io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run::Run;
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
    fn export_tsv_writes_header_and_rows() {
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
}

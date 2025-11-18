use crate::config::{BenchmarkId, Config, ConfigError, ConfigFile};
use crate::stats::StatsResult;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Result from a single benchmark run
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Run {
    /// Unix timestamp (seconds since epoch) when this run started
    #[serde(with = "jiff::fmt::serde::timestamp::second::required")]
    pub timestamp: Timestamp,
    #[serde(flatten)]
    pub stats: StatsResult,
}

/// A complete run series containing multiple individual runs
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunSeries {
    /// Schema version for format compatibility
    pub schema: u32,
    /// Benchmark name/identifier
    pub bench: BenchmarkId,
    /// Configuration key-value pairs (canonically sorted)
    pub config: Config,
    /// Unix timestamp when this run series started
    #[serde(with = "jiff::fmt::serde::timestamp::second::required")]
    pub timestamp: Timestamp,
    /// Individual run results (sorted by `mean_ns_per_iter`)
    pub runs: Vec<Run>,
    /// Mean from the median run (representative value)
    pub median_mean_ns_per_iter: f64,
    /// CI half-width from the median run
    pub median_ci95_half_width_ns: f64,
    /// Output validation checksum (if provided)
    pub checksum: Option<String>,
}

impl RunSeries {
    /// Format the run series result for display
    ///
    /// Returns a string like "30.92 µs/iter ±0.10% (median of 7 runs)"
    #[must_use]
    pub fn display_result(&self) -> String {
        let mean_us = self.median_mean_ns_per_iter / 1000.0;
        let ci_percent = (self.median_ci95_half_width_ns / self.median_mean_ns_per_iter) * 100.0;

        format!(
            "{:.2} µs/iter ±{:.2}% (median of {} runs)",
            mean_us,
            ci_percent,
            self.runs.len()
        )
    }
}

/// [`RunSeries`] definition that can be deserialized.
///
/// This is required as constructing [`Config`] requires access to the [`ConfigFile`] instances.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSeriesDef {
    pub schema: u32,
    pub bench: BenchmarkId,
    pub config: BTreeMap<String, String>,
    #[serde(with = "jiff::fmt::serde::timestamp::second::required")]
    pub timestamp: Timestamp,
    pub runs: Vec<Run>,
    pub median_mean_ns_per_iter: f64,
    pub median_ci95_half_width_ns: f64,
    pub checksum: Option<String>,
}

impl RunSeriesDef {
    pub fn try_to_run_series(self, config_file: &ConfigFile) -> Result<RunSeries, ConfigError> {
        Ok(RunSeries {
            schema: self.schema,
            bench: self.bench,
            config: config_file.config_from_map(&self.config)?,
            timestamp: self.timestamp,
            runs: self.runs,
            median_mean_ns_per_iter: self.median_mean_ns_per_iter,
            median_ci95_half_width_ns: self.median_ci95_half_width_ns,
            checksum: self.checksum,
        })
    }
}

impl From<RunSeries> for RunSeriesDef {
    fn from(value: RunSeries) -> Self {
        RunSeriesDef {
            schema: value.schema,
            bench: value.bench,
            config: value.config.into(),
            timestamp: value.timestamp,
            runs: value.runs,
            median_mean_ns_per_iter: value.median_mean_ns_per_iter,
            median_ci95_half_width_ns: value.median_ci95_half_width_ns,
            checksum: value.checksum,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::stats::{EstimationMode, Sample, StatsResult};
    use jiff::Timestamp;
    use tempfile::TempDir;

    #[test]
    fn test_run_series_display() {
        let series = RunSeries {
            schema: 1,
            bench: "test-bench".try_into().unwrap(),
            config: Config::new(),
            timestamp: Timestamp::from_second(1000).unwrap(),
            runs: vec![
                Run {
                    timestamp: Timestamp::from_second(1000).unwrap(),
                    stats: StatsResult {
                        mean_ns_per_iter: 30920.0, // 30.92 µs
                        ci95_half_width_ns: 310.0, // ±1%
                        mode: EstimationMode::PerIter,
                        intercept_ns: None,
                        outlier_count: 0,
                        samples: vec![],
                        temporal_correlation: 0.0,
                    }
                };
                7
            ],
            median_mean_ns_per_iter: 30920.0,
            median_ci95_half_width_ns: 310.0,
            checksum: None,
        };

        let display = series.display_result();
        assert!(display.contains("30.92 µs/iter"));
        assert!(display.contains("±1.00%"));
        assert!(display.contains("median of 7 runs"));
    }

    #[test]
    fn test_run_series_json_round_trip() {
        let json = r#"{
            "config_keys": {
                "commit": { "values": ["abc1234", "def5678"] },
                "threads": { "values": ["1", "n"] }
            },
            "benchmarks": []
        }"#;

        let tmp_dir = TempDir::new().unwrap();
        let config_file = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap();
        let config = config_file
            .config_from_string("commit=abc1234,threads=n")
            .unwrap();

        let series = RunSeries {
            schema: 1,
            bench: "2015-04".try_into().unwrap(),
            config: config.clone(),
            timestamp: Timestamp::from_second(1_763_287_200).unwrap(),
            runs: vec![Run {
                timestamp: Timestamp::from_second(1_763_287_201).unwrap(),
                stats: StatsResult {
                    mean_ns_per_iter: 30_920_000.0,
                    ci95_half_width_ns: 31_000.0,
                    mode: EstimationMode::PerIter,
                    intercept_ns: None,
                    outlier_count: 0,
                    samples: vec![Sample {
                        iters: 10_000_000,
                        total_ns: 30_920_000_000,
                    }],
                    temporal_correlation: 0.0,
                },
            }],
            median_mean_ns_per_iter: 30_920_000.0,
            median_ci95_half_width_ns: 31_000.0,
            checksum: Some("8f024a8e".to_string()),
        };

        // Serialize RunSeries to JSON
        let json = serde_json::to_string_pretty(&series).unwrap();
        assert!(json.contains("\"schema\": 1"));
        assert!(json.contains("\"bench\": \"2015-04\""));
        assert!(json.contains("\"commit\": \"abc1234\""));
        assert!(json.contains("\"mode\": \"per_iter\"")); // Verify snake_case serialization
        assert!(!json.contains("\"stats\":")); // Verify stats is flattened
        assert!(json.contains("\"timestamp\": 1763287200")); // Verify timestamp encoded in seconds

        // Deserialize back to RunSeriesDef
        let deserialized: RunSeriesDef = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.schema, 1);
        assert_eq!(deserialized.bench, "2015-04".try_into().unwrap());
        assert_eq!(deserialized.config, BTreeMap::from(config));
        assert_eq!(deserialized.runs.len(), 1);
        assert_eq!(deserialized.runs[0].stats.mode, EstimationMode::PerIter);
        assert!((deserialized.median_mean_ns_per_iter - 30_920_000.0).abs() < 0.001);
        assert_eq!(deserialized.checksum, Some("8f024a8e".to_string()));

        // Check that deserialized RunSeriesDef can be converted back to RunSeries
        let deserialized_series = deserialized.try_to_run_series(&config_file).unwrap();
        assert_eq!(deserialized_series, series);

        // Serialize RunResult to JSON
        let json = serde_json::to_string_pretty(&series.runs[0]).unwrap();
        assert!(json.contains("\"timestamp\": 1763287201")); // Verify timestamp encoded in seconds
    }
}

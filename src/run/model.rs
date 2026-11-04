use crate::config::{BenchmarkId, Config};
use crate::stats::{RunSummary, StatsResult};
use crate::warmup::WarmupSummary;
use jiff::Timestamp;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};

/// Result from a single benchmark run
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Run {
    /// Unix timestamp (seconds since epoch) when this run started
    #[serde(with = "jiff::fmt::serde::timestamp::second::required")]
    pub timestamp: Timestamp,
    #[serde(flatten)]
    pub stats: StatsResult,
    /// Absent in run files written before it was recorded
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warmup: Option<WarmupSummary>,
    /// Present when the host's package energy counter is readable
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub power: Option<PowerSummary>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PowerSummary {
    /// Mean over warmup, from the batch that opened it
    pub warmup_watts: f64,
    pub measured_watts: f64,
}

/// A complete run series containing multiple individual runs
///
/// Serializes with a `summary` field holding [`RunSeries::summary`], so any JSON view of a
/// series carries the headline numbers and they can never disagree with the runs.
#[derive(Debug, Clone, PartialEq)]
pub struct RunSeries {
    /// Schema version for format compatibility
    pub schema: u32,
    /// Benchmark name/identifier
    pub bench: BenchmarkId,
    /// Configuration key-value pairs (canonically sorted)
    pub config: Config,
    /// Unix timestamp when this run series started
    pub timestamp: Timestamp,
    /// Individual run results (sorted by `mean_ns_per_iter`)
    pub runs: Vec<Run>,
    /// Output validation checksum (if provided)
    pub checksum: Option<String>,
}

impl Serialize for RunSeries {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("RunSeries", 7)?;
        state.serialize_field("schema", &self.schema)?;
        state.serialize_field("bench", &self.bench)?;
        state.serialize_field("config", &self.config)?;
        state.serialize_field("timestamp", &self.timestamp.as_second())?;
        state.serialize_field("summary", &self.summary())?;
        state.serialize_field("runs", &self.runs)?;
        state.serialize_field("checksum", &self.checksum)?;
        state.end()
    }
}

impl RunSeries {
    /// The measurement summary: mean and 95% t-interval over the runs.
    #[must_use]
    pub fn summary(&self) -> RunSummary {
        let means: Vec<f64> = self.runs.iter().map(|r| r.stats.mean_ns_per_iter).collect();
        crate::stats::run_summary(&means)
    }

    /// Format the run series result for display
    ///
    /// Returns a string like "30.92 µs/iter ±0.10% (9 runs)"
    #[must_use]
    pub fn display_result(&self) -> String {
        let summary = self.summary();
        let mean_us = summary.mean_ns / 1000.0;
        let runs = summary.run_count;

        match summary.ci95_half_ns {
            Some(half) if summary.mean_ns > 0.0 => {
                let ci_percent = half / summary.mean_ns * 100.0;
                format!("{mean_us:.2} µs/iter ±{ci_percent:.2}% ({runs} runs)")
            }
            _ => format!("{mean_us:.2} µs/iter ({runs} run, no interval)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, ConfigFile};
    use crate::stats::{EstimationMode, Sample, StatsResult};
    use jiff::Timestamp;
    use tempfile::TempDir;

    fn run(mean_ns_per_iter: f64) -> Run {
        Run {
            timestamp: Timestamp::from_second(1000).unwrap(),
            stats: StatsResult {
                mean_ns_per_iter,
                mode: EstimationMode::PerIter,
                intercept_ns: None,
                se_ns: None,
                samples: vec![],
            },
            warmup: None,
            power: None,
        }
    }

    #[test]
    fn test_run_series_display() {
        // Mean 30.92 µs, sample sd 244.9 ns over 9 runs: half = 2.306 * 244.9 / 3 = 188.3 ns
        let runs = [
            30_520.0, 30_720.0, 30_720.0, 30_920.0, 30_920.0, 30_920.0, 31_120.0, 31_120.0,
            31_320.0,
        ]
        .map(run)
        .to_vec();
        let series = RunSeries {
            schema: 1,
            bench: "test-bench".try_into().unwrap(),
            config: Config::new(),
            timestamp: Timestamp::from_second(1000).unwrap(),
            runs,
            checksum: None,
        };

        let display = series.display_result();
        assert_eq!(display, "30.92 µs/iter ±0.61% (9 runs)");

        let single = RunSeries {
            runs: vec![run(30_920.0)],
            ..series
        };
        assert_eq!(
            single.display_result(),
            "30.92 µs/iter (1 run, no interval)"
        );
    }

    #[test]
    fn test_run_series_json_serialization() {
        let json = r#"{
            "config_keys": {
                "commit": { "values": ["abc1234", "def5678"] },
                "threads": { "values": ["1", "n"] }
            },
            "benchmarks": [{ "benchmark": "b", "command": ["true"], "config": {} }]
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
                    mode: EstimationMode::PerIter,
                    intercept_ns: None,
                    se_ns: Some(31_000.0),
                    samples: vec![Sample {
                        iters: 10_000_000,
                        total_ns: 30_920_000_000,
                    }],
                },
                warmup: None,
                power: None,
            }],
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
        assert!(json.contains("\"summary\": {")); // The run summary travels with the series
        assert!(json.contains("\"run_count\": 1"));

        // Serialize RunResult to JSON
        let json = serde_json::to_string_pretty(&series.runs[0]).unwrap();
        assert!(json.contains("\"timestamp\": 1763287201")); // Verify timestamp encoded in seconds
    }
}

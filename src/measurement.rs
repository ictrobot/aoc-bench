//! Schema-5 measurement JSON: the immutable per-execution sample file.
//!
//! One measurement backs every case covered by a recorded workload run series. The file records
//! the workload identity, the measured executable/stdin artifact digests, the executed case
//! (JSON-only provenance — never surfaced or pointed at from the database), the covered-case
//! snapshot, the run summary, the series' wall time, and the raw runs and samples. Cases that
//! inherit the workload later are stored as database links rather than by mutating this
//! immutable file.
//!
//! Schema 4 and older files lack the wall times and sort `runs` by mean. Schema 3 files also carry
//! `summary.multimodal`, which attempted to flag runs that fell into multiple clusters. Schema 2
//! files lack the `summary` block (their database summary was the median run's own statistics).

use crate::config::{BenchmarkId, Config};
use crate::run::Run;
use crate::stats::RunSummary;
use crate::storage::MeasurementId;
use crate::workload::{GroupSpec, Sha256};
use jiff::Timestamp;
use serde::Serialize;

/// The current measurement JSON schema version.
pub const MEASUREMENT_SCHEMA: u32 = 5;

/// A schema-5 measurement, ready to serialize to its immutable JSON file.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Measurement {
    /// Schema version (always [`MEASUREMENT_SCHEMA`]).
    pub schema: u32,
    /// This execution's opaque, time-sortable id.
    pub measurement_id: MeasurementId,
    /// Owning benchmark.
    pub bench: BenchmarkId,
    /// The workload identity this measurement backs.
    pub workload_sha256: Sha256,
    /// The non-artifact invocation fields (absent for isolated workloads).
    pub group_spec: Option<GroupSpec>,
    /// The measured executable's content digest (absent for isolated workloads).
    pub executable_sha256: Option<Sha256>,
    /// The measured stdin's content digest, if any.
    pub stdin_sha256: Option<Sha256>,
    /// The case used to run the workload (provenance only).
    pub executed_case: Config,
    /// Every case this execution covered when it ran.
    pub covered_cases: Vec<Config>,
    /// When the run series started.
    #[serde(with = "jiff::fmt::serde::timestamp::second::required")]
    pub timestamp: Timestamp,
    /// Wall time from `timestamp` to the end of the last run.
    pub series_wall_ns: u64,
    /// Expected checksum, if any.
    pub checksum: Option<String>,
    /// The headline numbers, computed from `runs` alone.
    pub summary: RunSummary,
    /// Raw runs in the order they ran, each carrying its samples.
    pub runs: Vec<Run>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigFile;
    use crate::stats::{EstimationMode, Sample, StatsOptions, StatsResult};
    use crate::workload::GroupSpec;
    use tempfile::TempDir;

    fn run(mean: f64) -> Run {
        Run {
            timestamp: Timestamp::from_second(10).unwrap(),
            stats: StatsResult {
                mean_ns_per_iter: mean,
                mode: EstimationMode::PerIter,
                intercept_ns: None,
                se_ns: None,
                samples: vec![Sample {
                    iters: 1,
                    total_ns: 1000,
                }],
            },
            warmup: None,
            power: None,
            wall_ns: None,
        }
    }

    #[test]
    fn serializes_with_identity_fields() {
        let dir = TempDir::new().unwrap();
        let json = r#"{
            "config_keys": { "commit": { "values": ["abc"] } },
            "benchmarks": [
                { "benchmark": "b", "command": ["cmd", "{commit}"], "config": { "commit": ["abc"] } }
            ]
        }"#;
        let cf = ConfigFile::from_str(dir.path(), None, json).unwrap();
        let config = cf.config_from_string("commit=abc").unwrap();

        let measurement = Measurement {
            schema: MEASUREMENT_SCHEMA,
            measurement_id: MeasurementId::new_v7(),
            bench: "b".try_into().unwrap(),
            workload_sha256: Sha256::hash_bytes(b"w"),
            group_spec: Some(GroupSpec::new(
                vec!["abc".into()],
                None,
                StatsOptions::default(),
            )),
            executable_sha256: Some(Sha256::hash_bytes(b"exe")),
            stdin_sha256: None,
            executed_case: config.clone(),
            covered_cases: vec![config],
            timestamp: Timestamp::from_second(100).unwrap(),
            series_wall_ns: 40,
            checksum: Some("deadbeef".into()),
            summary: crate::stats::run_summary(&[10.0, 20.0, 30.0]),
            runs: vec![run(10.0), run(20.0), run(30.0)],
        };

        let text = serde_json::to_string(&measurement).unwrap();
        assert!(text.contains("\"schema\":5"));
        assert!(text.contains("\"series_wall_ns\":40,"));
        assert!(text.contains("\"summary\":{\"run_count\":3,\"mean_ns\":20.0,"));
        assert!(text.contains("\"workload_sha256\":"));
        assert!(text.contains("\"executable_sha256\":"));
        assert!(text.contains("\"covered_cases\":"));
    }
}

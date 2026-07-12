//! Schema-2 measurement JSON: the immutable per-execution sample file.
//!
//! One measurement backs every case covered by a recorded workload run series. The file records
//! the workload identity, the measured executable/stdin artifact digests, the executed case
//! (JSON-only provenance — never surfaced or pointed at from the database), the covered-case
//! snapshot, and the raw runs and samples. Cases that inherit the workload later are stored as
//! database links rather than by mutating this immutable file.

use crate::config::{BenchmarkId, Config};
use crate::run::Run;
use crate::storage::MeasurementId;
use crate::workload::{GroupSpec, Sha256};
use jiff::Timestamp;
use serde::Serialize;

/// The current measurement JSON schema version.
pub const MEASUREMENT_SCHEMA: u32 = 2;

/// A schema-2 measurement, ready to serialize to its immutable JSON file.
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
    /// Expected checksum, if any.
    pub checksum: Option<String>,
    /// Raw runs (sorted by mean), each carrying its samples.
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
                ci95_half_width_ns: 1.0,
                mode: EstimationMode::PerIter,
                intercept_ns: None,
                outlier_count: 0,
                samples: vec![Sample {
                    iters: 1,
                    total_ns: 1000,
                }],
                temporal_correlation: 0.0,
            },
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
            checksum: Some("deadbeef".into()),
            runs: vec![run(10.0), run(20.0), run(30.0)],
        };

        let text = serde_json::to_string(&measurement).unwrap();
        assert!(text.contains("\"schema\":2"));
        assert!(text.contains("\"workload_sha256\":"));
        assert!(text.contains("\"executable_sha256\":"));
        assert!(text.contains("\"covered_cases\":"));
    }
}

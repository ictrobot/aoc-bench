use crate::config::BenchmarkId;
use serde::ser::SerializeSeq;
use serde::{Serialize, Serializer};
use std::collections::BTreeMap;

/// A row in the indexed results file.
///
/// Serializes as `[bench_idx, config_idx, measurement_token, mean_ns, ci95_half_ns]`.
/// Decoded using the host's `config_keys` and `benchmarks` from `index.json`.
#[derive(Debug, PartialEq)]
pub struct ResultRow {
    pub bench_idx: usize,
    pub config_idx: usize,
    /// Host-snapshot-local shared-measurement token; zero means isolated.
    pub measurement_token: u32,
    pub mean_ns: i64,
    pub ci95_half_ns: i64,
}

impl Serialize for ResultRow {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(5))?;
        seq.serialize_element(&self.bench_idx)?;
        seq.serialize_element(&self.config_idx)?;
        seq.serialize_element(&self.measurement_token)?;
        seq.serialize_element(&self.mean_ns)?;
        seq.serialize_element(&self.ci95_half_ns)?;
        seq.end()
    }
}

/// A row in the indexed history file.
///
/// Serializes as
/// `[config_idx, measurement_token, timestamp_s, mean_ns, ci95_half_ns, run_count]`.
/// Decoded using the host's `config_keys` from `index.json`.
#[derive(Debug, PartialEq)]
pub struct HistoryRow {
    pub config_idx: usize,
    /// Host-snapshot-local shared-measurement token; zero means isolated.
    pub measurement_token: u32,
    pub timestamp_s: i64,
    pub mean_ns: i64,
    pub ci95_half_ns: i64,
    pub run_count: usize,
}

impl Serialize for HistoryRow {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(6))?;
        seq.serialize_element(&self.config_idx)?;
        seq.serialize_element(&self.measurement_token)?;
        seq.serialize_element(&self.timestamp_s)?;
        seq.serialize_element(&self.mean_ns)?;
        seq.serialize_element(&self.ci95_half_ns)?;
        seq.serialize_element(&self.run_count)?;
        seq.end()
    }
}

#[derive(Serialize, Debug, PartialEq)]
pub struct WebHostIndex {
    pub last_updated: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub config_keys: BTreeMap<String, WebConfigKey>,
    pub benchmarks: Vec<WebBenchmarkEntry>,
    pub timeline_key: Option<String>,
    /// Latest stable results for the most recent timeline key value.
    /// Each row: `[bench_idx, config_idx, measurement_token, mean_ns, ci95_half_ns]`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_results: Option<Vec<ResultRow>>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct WebConfigKey {
    pub values: Vec<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
    /// URL template containing `{value}`, e.g. a commit link
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct WebBenchmarkEntry {
    pub name: BenchmarkId,
    pub result_count: usize,
    /// Indices into the host's canonically ordered `config_keys` table.
    pub config_keys: Vec<usize>,
}

/// Stable results for all benchmarks on this host.
/// Each row: `[bench_idx, config_idx, measurement_token, mean_ns, ci95_half_ns]`
#[derive(Serialize, Debug, PartialEq)]
pub struct WebIndexedResults {
    pub results: Vec<ResultRow>,
}

/// Measurement history for a single benchmark.
/// Each row: `[config_idx, measurement_token, timestamp_s, mean_ns, ci95_half_ns, run_count]`
#[derive(Serialize, Debug, PartialEq)]
pub struct WebIndexedHistory {
    pub series: Vec<HistoryRow>,
}

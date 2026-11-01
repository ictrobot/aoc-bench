use crate::config::BenchmarkId;
use serde::ser::SerializeSeq;
use serde::{Serialize, Serializer};
use std::collections::BTreeMap;

/// A row of the indexed results file, or of `latest_results` in the host index.
///
/// In `latest_results` it serializes as `[bench_idx, config_idx, measurement_token, mean_ns,
/// ci95_half_ns]`; `results.json` stores rows as columns instead, see [`WebIndexedResults`].
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

/// A row of an indexed history part file, stored as columns, see [`WebIndexedHistory`].
///
/// Decoded using the host's `config_keys` from `index.json`. Rows are ordered by `config_idx`,
/// then by timestamp.
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
    /// First `config_idx` covered by each history part file, in part order. Always starts at 0,
    /// strictly increasing; part `n` is `history/{name}.{n}.json` and holds configs from
    /// `history_parts[n]` up to (excluding) `history_parts[n + 1]`.
    pub history_parts: Vec<usize>,
}

/// Stable results for all benchmarks on this host, ordered by `bench_idx` then `config_idx`.
///
/// Serializes column-wise as
/// `{"results": {"bench_idx_delta": [..], "config_idx_delta": [..], "measurement_token": [..],
/// "mean_ns": [..], "ci95_half_ns": [..]}}` where every column has one entry per row. Grouping
/// values by column lets generic transfer compression find the long runs in the low-entropy
/// columns; `*_delta` columns hold the difference from the previous row's value (from zero for
/// the first row), which turns the sorted index columns into mostly zeros and small steps.
#[derive(Debug, PartialEq)]
pub struct WebIndexedResults {
    pub results: Vec<ResultRow>,
}

impl Serialize for WebIndexedResults {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Columns<'a> {
            bench_idx_delta: DeltaColumn<'a, ResultRow>,
            config_idx_delta: DeltaColumn<'a, ResultRow>,
            measurement_token: Column<'a, ResultRow>,
            mean_ns: Column<'a, ResultRow>,
            ci95_half_ns: Column<'a, ResultRow>,
        }
        #[derive(Serialize)]
        struct Payload<'a> {
            results: Columns<'a>,
        }

        let rows = &self.results;
        Payload {
            results: Columns {
                bench_idx_delta: DeltaColumn(rows, |r| to_i64(r.bench_idx)),
                config_idx_delta: DeltaColumn(rows, |r| to_i64(r.config_idx)),
                measurement_token: Column(rows, |r| i64::from(r.measurement_token)),
                mean_ns: Column(rows, |r| r.mean_ns),
                ci95_half_ns: Column(rows, |r| r.ci95_half_ns),
            },
        }
        .serialize(serializer)
    }
}

/// One part of the measurement history for a single benchmark, covering a contiguous
/// `config_idx` range (see `WebBenchmarkEntry::history_parts`). Rows are ordered by
/// `config_idx` then timestamp.
///
/// Serializes column-wise in the same style as [`WebIndexedResults`], as
/// `{"series": {"config_idx_delta": [..], "measurement_token": [..], "timestamp_s_delta": [..],
/// "mean_ns": [..], "ci95_half_ns": [..], "run_count": [..]}}`. Deltas restart from zero in
/// every part file.
#[derive(Debug, PartialEq)]
pub struct WebIndexedHistory {
    pub series: Vec<HistoryRow>,
}

impl Serialize for WebIndexedHistory {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Columns<'a> {
            config_idx_delta: DeltaColumn<'a, HistoryRow>,
            measurement_token: Column<'a, HistoryRow>,
            timestamp_s_delta: DeltaColumn<'a, HistoryRow>,
            mean_ns: Column<'a, HistoryRow>,
            ci95_half_ns: Column<'a, HistoryRow>,
            run_count: Column<'a, HistoryRow>,
        }
        #[derive(Serialize)]
        struct Payload<'a> {
            series: Columns<'a>,
        }

        let rows = &self.series;
        Payload {
            series: Columns {
                config_idx_delta: DeltaColumn(rows, |r| to_i64(r.config_idx)),
                measurement_token: Column(rows, |r| i64::from(r.measurement_token)),
                timestamp_s_delta: DeltaColumn(rows, |r| r.timestamp_s),
                mean_ns: Column(rows, |r| r.mean_ns),
                ci95_half_ns: Column(rows, |r| r.ci95_half_ns),
                run_count: Column(rows, |r| to_i64(r.run_count)),
            },
        }
        .serialize(serializer)
    }
}

/// One field of every row, serialized as a JSON array without materializing it.
struct Column<'a, R>(&'a [R], fn(&R) -> i64);

impl<R> Serialize for Column<'_, R> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(self.1))
    }
}

/// Like [`Column`], but each entry is the difference from the previous row's value, starting
/// from zero.
struct DeltaColumn<'a, R>(&'a [R], fn(&R) -> i64);

impl<R> Serialize for DeltaColumn<'_, R> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(self.1).scan(0i64, |prev, value| {
            let delta = value - *prev;
            *prev = value;
            Some(delta)
        }))
    }
}

fn to_i64(value: usize) -> i64 {
    i64::try_from(value).expect("index fits in i64")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn results_serialize_as_delta_columns() {
        let row = |bench_idx, config_idx, measurement_token, mean_ns, ci95_half_ns| ResultRow {
            bench_idx,
            config_idx,
            measurement_token,
            mean_ns,
            ci95_half_ns,
        };
        let data = WebIndexedResults {
            results: vec![
                row(0, 3, 0, 100, 5),
                row(0, 7, 2, 110, 6),
                row(1, 1, 2, 120, 7),
            ],
        };
        let json: serde_json::Value = serde_json::to_value(&data).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"results": {
                "bench_idx_delta": [0, 0, 1],
                "config_idx_delta": [3, 4, -6],
                "measurement_token": [0, 2, 2],
                "mean_ns": [100, 110, 120],
                "ci95_half_ns": [5, 6, 7],
            }})
        );
    }

    #[test]
    fn history_serializes_as_delta_columns() {
        let row = |config_idx, measurement_token, timestamp_s, mean_ns, ci95_half_ns, run_count| {
            HistoryRow {
                config_idx,
                measurement_token,
                timestamp_s,
                mean_ns,
                ci95_half_ns,
                run_count,
            }
        };
        let data = WebIndexedHistory {
            series: vec![
                row(2, 1, 1000, 100, 5, 3),
                row(2, 0, 1500, 101, 4, 1),
                row(5, 0, 900, 200, 9, 3),
            ],
        };
        let json: serde_json::Value = serde_json::to_value(&data).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"series": {
                "config_idx_delta": [2, 0, 3],
                "measurement_token": [1, 0, 0],
                "timestamp_s_delta": [1000, 500, -600],
                "mean_ns": [100, 101, 200],
                "ci95_half_ns": [5, 4, 9],
                "run_count": [3, 1, 3],
            }})
        );
    }

    #[test]
    fn empty_payloads_have_empty_columns() {
        let results: serde_json::Value =
            serde_json::to_value(WebIndexedResults { results: vec![] }).unwrap();
        assert_eq!(results["results"]["bench_idx_delta"], serde_json::json!([]));
        let history: serde_json::Value =
            serde_json::to_value(WebIndexedHistory { series: vec![] }).unwrap();
        assert_eq!(history["series"]["run_count"], serde_json::json!([]));
    }
}

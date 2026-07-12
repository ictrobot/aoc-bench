//! Versioned measurement-file locations.
//!
//! V1 files retain their historical case-oriented layout. V2 files are addressed by measurement
//! id. Current result and history queries use SQLite summaries; recovery/reanalysis tooling can
//! add a complete, database-independent decoder when it needs the immutable raw samples.

use crate::storage::MeasurementId;
use std::path::{Path, PathBuf};

#[cfg(test)]
use crate::config::{BenchmarkId, Config};
#[cfg(test)]
use jiff::Timestamp;

const BY_MEASUREMENT_DIR: &str = "by-measurement";
#[cfg(test)]
const EMPTY_CONFIG_DIR: &str = "__default__";

/// Return the immutable V2 path, sharded on the measurement id's random tail.
pub(super) fn current_path(runs_dir: &Path, measurement_id: MeasurementId) -> PathBuf {
    let (h1, h2) = measurement_id.shard();
    runs_dir
        .join(BY_MEASUREMENT_DIR)
        .join(h1)
        .join(h2)
        .join(format!("{measurement_id}.json"))
}

/// Reconstruct the immutable V1 path from its logical identity.
#[cfg(test)]
pub(super) fn v1_path(
    runs_dir: &Path,
    bench: &BenchmarkId,
    config: &Config,
    timestamp: Timestamp,
) -> PathBuf {
    let mut path = runs_dir.join(bench.as_str());
    let hostless = config.without_host_key();
    if hostless.is_empty() {
        path.push(EMPTY_CONFIG_DIR);
    } else {
        for key_value in hostless.iter() {
            path.push(key_value.to_string());
        }
    }
    path.join(format!("{}.json", timestamp.strftime("%Y-%m-%dT%H-%M-%S")))
}

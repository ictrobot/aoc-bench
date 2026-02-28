use super::build::{export_host, host_names};
use super::error::WebSnapshotExportError;
use super::format::WebHostIndex;
use crate::config::ConfigFile;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use tempfile::{Builder, NamedTempFile, TempDir};
use tracing::info;

const WEB_INDEX_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, PartialEq, Eq)]
pub struct WebSnapshotExport {
    pub snapshot_id: String,
    pub host_count: usize,
    pub snapshot_created: bool,
}

#[derive(Serialize)]
struct SnapshotIndex {
    schema_version: u32,
    snapshot_id: String,
    hosts: BTreeMap<String, SnapshotHostIndex>,
}

#[derive(Serialize)]
struct SnapshotHostIndex {
    #[serde(flatten)]
    index: WebHostIndex,
    results_path: String,
    history_dir: String,
}

#[derive(Serialize, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct SnapshotManifestEntry {
    path: String,
    file_size: u64,
    file_hash_sha256: String,
}

/// Export all hosts into a snapshot directory and atomically publish `index.json`.
///
/// Returns `Ok(None)` when the config has no hosts.
pub fn export_web_snapshot(
    config_file: &ConfigFile,
    output_dir: &Path,
) -> Result<Option<WebSnapshotExport>, WebSnapshotExportError> {
    let hosts = host_names(config_file);
    if hosts.is_empty() {
        return Ok(None);
    }

    let snapshots_dir = output_dir.join("snapshots");
    fs::create_dir_all(&snapshots_dir).map_err(|error| WebSnapshotExportError::Io {
        path: snapshots_dir.clone(),
        error,
    })?;

    let staging_snapshot_dir =
        Builder::new()
            .tempdir_in(&snapshots_dir)
            .map_err(|error| WebSnapshotExportError::Io {
                path: snapshots_dir.clone(),
                error,
            })?;

    let (snapshot_id, host_index_map) =
        write_snapshot_payload(config_file, staging_snapshot_dir.path(), &hosts)?;
    let final_snapshot_dir = snapshots_dir.join(&snapshot_id);
    let snapshot_created = publish_snapshot_dir(staging_snapshot_dir, &final_snapshot_dir)?;

    let global_index = SnapshotIndex {
        schema_version: WEB_INDEX_SCHEMA_VERSION,
        snapshot_id: snapshot_id.clone(),
        hosts: build_snapshot_hosts(&snapshot_id, host_index_map),
    };
    write_json_atomic(&output_dir.join("index.json"), &global_index)?;

    Ok(Some(WebSnapshotExport {
        snapshot_id,
        host_count: hosts.len(),
        snapshot_created,
    }))
}

fn write_snapshot_payload(
    config_file: &ConfigFile,
    staging_snapshot_dir: &Path,
    hosts: &[String],
) -> Result<(String, BTreeMap<String, WebHostIndex>), WebSnapshotExportError> {
    let mut host_index_map: BTreeMap<String, WebHostIndex> = BTreeMap::new();
    let mut manifest_entries: Vec<SnapshotManifestEntry> = Vec::new();

    for host_name in hosts {
        info!(host = host_name, "exporting host data");

        let host_dir = staging_snapshot_dir.join(host_name);
        let history_dir = host_dir.join("history");
        let mut history_count = 0usize;

        let data = export_host(config_file, host_name, |bench_id, history| {
            history_count += 1;
            let file_name = format!("{}.json", bench_id.as_str());
            let relative_path = format!("{host_name}/history/{file_name}");
            let entry = write_json_hashed(&history_dir.join(file_name), &relative_path, &history)?;
            manifest_entries.push(entry);
            Ok::<(), WebSnapshotExportError>(())
        })?;
        info!(
            host = host_name,
            count = history_count,
            "wrote history files"
        );

        let results_relative_path = format!("{host_name}/results.json");
        let results_entry = write_json_hashed(
            &host_dir.join("results.json"),
            &results_relative_path,
            &data.compact,
        )?;
        manifest_entries.push(results_entry);
        info!(
            host = host_name,
            count = data.compact.results.len(),
            "wrote results.json"
        );

        host_index_map.insert(host_name.clone(), data.index);
    }

    let snapshot_id = snapshot_id_from_manifest(&manifest_entries)?;
    Ok((snapshot_id, host_index_map))
}

fn publish_snapshot_dir(
    staging_snapshot_dir: TempDir,
    final_snapshot_dir: &Path,
) -> Result<bool, WebSnapshotExportError> {
    let rename_result = fs::rename(staging_snapshot_dir.path(), final_snapshot_dir);
    drop(staging_snapshot_dir);

    match rename_result {
        Ok(()) => Ok(true),
        Err(error)
            if (error.kind() == io::ErrorKind::AlreadyExists
                || error.kind() == io::ErrorKind::DirectoryNotEmpty)
                && final_snapshot_dir.is_dir() =>
        {
            Ok(false)
        }
        Err(error) => Err(WebSnapshotExportError::Io {
            path: final_snapshot_dir.to_path_buf(),
            error,
        }),
    }
}

fn build_snapshot_hosts(
    snapshot_id: &str,
    host_index_map: BTreeMap<String, WebHostIndex>,
) -> BTreeMap<String, SnapshotHostIndex> {
    host_index_map
        .into_iter()
        .map(|(host, index)| {
            let results_path = format!("snapshots/{snapshot_id}/{host}/results.json");
            let history_dir = format!("snapshots/{snapshot_id}/{host}/history");
            (
                host,
                SnapshotHostIndex {
                    index,
                    results_path,
                    history_dir,
                },
            )
        })
        .collect()
}

fn write_json_atomic<T: Serialize>(path: &Path, data: &T) -> Result<(), WebSnapshotExportError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));

    fs::create_dir_all(parent).map_err(|error| WebSnapshotExportError::Io {
        path: parent.to_path_buf(),
        error,
    })?;

    let mut tmp = NamedTempFile::new_in(parent).map_err(|error| WebSnapshotExportError::Io {
        path: parent.to_path_buf(),
        error,
    })?;
    serde_json::to_writer(tmp.as_file_mut(), data).map_err(WebSnapshotExportError::Json)?;
    tmp.as_file_mut()
        .sync_all()
        .map_err(|error| WebSnapshotExportError::Io {
            path: tmp.path().to_path_buf(),
            error,
        })?;
    tmp.persist(path).map_err(|e| WebSnapshotExportError::Io {
        path: path.to_path_buf(),
        error: e.error,
    })?;

    Ok(())
}

fn write_json_hashed<T: Serialize>(
    path: &Path,
    relative_path: &str,
    data: &T,
) -> Result<SnapshotManifestEntry, WebSnapshotExportError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));

    fs::create_dir_all(parent).map_err(|error| WebSnapshotExportError::Io {
        path: parent.to_path_buf(),
        error,
    })?;

    let mut file = fs::File::create(path).map_err(|error| WebSnapshotExportError::Io {
        path: path.to_path_buf(),
        error,
    })?;
    let (file_size, file_hash_sha256) = {
        let mut writer = HashingWriter::new(&mut file);
        let mut buffered = BufWriter::new(&mut writer);
        serde_json::to_writer(&mut buffered, data).map_err(WebSnapshotExportError::Json)?;
        buffered
            .flush()
            .map_err(|error| WebSnapshotExportError::Io {
                path: path.to_path_buf(),
                error,
            })?;
        drop(buffered);
        writer.finish()
    };

    Ok(SnapshotManifestEntry {
        path: relative_path.to_string(),
        file_size,
        file_hash_sha256,
    })
}

fn snapshot_id_from_manifest(
    entries: &[SnapshotManifestEntry],
) -> Result<String, WebSnapshotExportError> {
    let mut manifest = entries.to_vec();
    manifest.sort_unstable();
    let manifest_json = serde_json::to_vec(&manifest).map_err(WebSnapshotExportError::Json)?;
    Ok(hash_bytes_sha256(&manifest_json))
}

fn hash_bytes_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

struct HashingWriter<'a, W> {
    inner: &'a mut W,
    hasher: Sha256,
    written_bytes: u64,
}

impl<'a, W: Write> HashingWriter<'a, W> {
    fn new(inner: &'a mut W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            written_bytes: 0,
        }
    }

    fn finish(self) -> (u64, String) {
        (self.written_bytes, format!("{:x}", self.hasher.finalize()))
    }
}

impl<W: Write> Write for HashingWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        if written > 0 {
            self.hasher.update(&buf[..written]);
            self.written_bytes += written as u64;
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigFile;
    use crate::run::Run;
    use crate::stats::{EstimationMode, Sample, StatsResult};
    use crate::storage::{HybridDiskStorage, ResultsRow, Storage};
    use jiff::Timestamp;

    fn setup_storage(host: &str) -> (TempDir, ConfigFile, HybridDiskStorage) {
        let dir = TempDir::new().unwrap();

        let json = r#"{
            "config_keys": {
                "build": { "values": ["x"] }
            },
            "benchmarks": [
                {
                    "benchmark": "bench",
                    "command": ["echo", "{build}"],
                    "config": { "build": ["x"] }
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
    fn test_snapshot_id_from_manifest_is_deterministic() {
        let entries = vec![
            SnapshotManifestEntry {
                path: "h1/results.json".to_string(),
                file_size: 10,
                file_hash_sha256: "aa".repeat(32),
            },
            SnapshotManifestEntry {
                path: "h1/history/a.json".to_string(),
                file_size: 20,
                file_hash_sha256: "bb".repeat(32),
            },
        ];
        let mut reversed = entries.clone();
        reversed.reverse();

        let h1 = snapshot_id_from_manifest(&entries).unwrap();
        let h2 = snapshot_id_from_manifest(&reversed).unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_snapshot_id_from_manifest_changes_when_content_changes() {
        let before = vec![SnapshotManifestEntry {
            path: "h/results.json".to_string(),
            file_size: 10,
            file_hash_sha256: "11".repeat(32),
        }];
        let after = vec![SnapshotManifestEntry {
            path: "h/results.json".to_string(),
            file_size: 11,
            file_hash_sha256: "22".repeat(32),
        }];

        let before_hash = snapshot_id_from_manifest(&before).unwrap();
        let after_hash = snapshot_id_from_manifest(&after).unwrap();
        assert_ne!(before_hash, after_hash);
    }

    #[test]
    fn test_write_json_hashed_records_expected_hash_and_size() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("x.json");

        let entry = write_json_hashed(&path, "x.json", &serde_json::json!({"a": 1})).unwrap();
        let bytes = fs::read(&path).unwrap();

        assert_eq!(entry.path, "x.json");
        assert_eq!(entry.file_size, bytes.len() as u64);
        assert_eq!(entry.file_hash_sha256, hash_bytes_sha256(&bytes));
    }

    #[test]
    fn test_hashing_writer_tracks_size_and_hash() {
        let mut sink = Vec::<u8>::new();
        let mut writer = HashingWriter::new(&mut sink);
        writer.write_all(b"hello").unwrap();
        writer.write_all(b" world").unwrap();
        let (size, hash) = writer.finish();

        assert_eq!(size, 11);
        assert_eq!(hash, hash_bytes_sha256(b"hello world"));
    }

    #[test]
    fn test_publish_snapshot_dir_reuses_existing_dir_and_cleans_staging() {
        let root = TempDir::new().unwrap();
        let staging = Builder::new().tempdir_in(root.path()).unwrap();
        let staging_path = staging.path().to_path_buf();
        fs::write(staging_path.join("payload.json"), b"{}").unwrap();

        let final_snapshot_dir = root.path().join("existing");
        fs::create_dir(&final_snapshot_dir).unwrap();
        fs::write(final_snapshot_dir.join("existing.json"), b"{}").unwrap();

        let created = publish_snapshot_dir(staging, &final_snapshot_dir).unwrap();
        assert!(!created);
        assert!(final_snapshot_dir.exists());
        assert!(!staging_path.exists());
    }

    #[test]
    fn test_publish_snapshot_dir_cleans_staging_on_error() {
        let root = TempDir::new().unwrap();
        let staging = Builder::new().tempdir_in(root.path()).unwrap();
        let staging_path = staging.path().to_path_buf();
        fs::write(staging_path.join("payload.json"), b"{}").unwrap();

        let final_snapshot_dir = root.path().join("missing-parent").join("snapshot");
        let error = publish_snapshot_dir(staging, &final_snapshot_dir).unwrap_err();

        match error {
            WebSnapshotExportError::Io { path, .. } => assert_eq!(path, final_snapshot_dir),
            _ => panic!("unexpected error type"),
        }
        assert!(!staging_path.exists());
    }

    #[test]
    fn test_export_web_snapshot_writes_index_and_reuses_snapshot() {
        let (dir, config_file, storage) = setup_storage("h1");
        let s1 = mk_series(&config_file, "h1", "bench", "build=x", 100.0, 1000);
        insert(&storage, &s1);

        let output_dir = dir.path().join("web-data");
        let first = export_web_snapshot(&config_file, &output_dir)
            .unwrap()
            .expect("expected hosts");
        assert_eq!(first.host_count, 1);
        assert!(first.snapshot_created);

        let index: serde_json::Value =
            serde_json::from_slice(&fs::read(output_dir.join("index.json")).unwrap()).unwrap();

        assert_eq!(index["schema_version"], WEB_INDEX_SCHEMA_VERSION);
        assert_eq!(index["snapshot_id"], first.snapshot_id);
        assert_eq!(
            index["hosts"]["h1"]["results_path"],
            format!("snapshots/{}/h1/results.json", first.snapshot_id)
        );
        assert_eq!(
            index["hosts"]["h1"]["history_dir"],
            format!("snapshots/{}/h1/history", first.snapshot_id)
        );

        let results_path =
            output_dir.join(format!("snapshots/{}/h1/results.json", first.snapshot_id));
        let history_path = output_dir.join(format!(
            "snapshots/{}/h1/history/bench.json",
            first.snapshot_id
        ));
        assert!(results_path.exists());
        assert!(history_path.exists());

        let second = export_web_snapshot(&config_file, &output_dir)
            .unwrap()
            .expect("expected hosts");
        assert_eq!(second.snapshot_id, first.snapshot_id);
        assert!(!second.snapshot_created);
    }
}

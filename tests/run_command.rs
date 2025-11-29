use rusqlite::Connection;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const BENCH_EXECUTABLE: &str = env!("CARGO_BIN_EXE_aoc-bench");

fn write_config(dir: &Path) {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let config = r#"{
  "config_keys": {
    "build": { "values": ["opt"] }
  },
  "benchmarks": [
    {
      "benchmark": "bench",
      "command": ["yes", "SAMPLE\t123\t34000000\tdata={build}"],
      "config": { "build": ["opt"] }
    }
  ]
}"#;
    fs::write(data_dir.join("config.json"), config).unwrap();
}

#[test]
fn run_command_stores_run_series() {
    let tmp = tempfile::tempdir().unwrap();

    write_config(tmp.path());
    let data_dir = tmp.path().join("data");

    let output = Command::new(BENCH_EXECUTABLE)
        .arg("run")
        .arg("--data-dir")
        .arg(&data_dir)
        .env("BENCH_HOST", "testhost")
        .env("LOG_FORMAT", "json")
        .output()
        .expect("spawn run command");

    assert!(
        output.status.success(),
        "run command failed: status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(output.stdout.is_empty());

    let mut run_series_path: Option<PathBuf> = None;
    for line in String::from_utf8_lossy(&output.stderr).lines() {
        if let Ok(v) = serde_json::from_str::<Value>(line)
            && let Some(fields) = v.get("fields")
            && let Some(path) = fields.get("path")
            && let Some(path) = path.as_str()
        {
            run_series_path = Some(PathBuf::from(path));
            break;
        }
    }

    let json_path = run_series_path.expect("run series path from logs");
    let data = fs::read_to_string(&json_path).unwrap();
    let v: Value = serde_json::from_str(&data).unwrap();

    assert_eq!(v["bench"], "bench");
    assert_eq!(v["config"]["build"], "opt");

    for run in v["runs"].as_array().unwrap() {
        assert!((run["mean_ns_per_iter"].as_f64().unwrap() - 276_422.764).abs() < 0.001);
    }
}

#[test]
fn run_command_dry_run_does_not_persist() {
    let tmp = tempfile::tempdir().unwrap();

    write_config(tmp.path());
    let data_dir = tmp.path().join("data");

    let output = Command::new(BENCH_EXECUTABLE)
        .arg("run")
        .arg("--dry-run")
        .arg("--data-dir")
        .arg(&data_dir)
        .env("BENCH_HOST", "testhost")
        .output()
        .expect("spawn run command");

    assert!(
        output.status.success(),
        "dry-run command failed: status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    // No JSON run series should have been written.
    let runs_dir = data_dir
        .join("results")
        .join("testhost")
        .join("runs")
        .join("bench");
    if runs_dir.exists() {
        let mut entries = fs::read_dir(&runs_dir).unwrap();
        assert!(entries.next().is_none(), "dry-run wrote run series files");
    }

    // If metadata DB exists, ensure run_series table is empty.
    let db_path = data_dir
        .join("results")
        .join("testhost")
        .join("metadata.db");
    if db_path.exists() {
        let conn = Connection::open(db_path).unwrap();
        let count: u64 = conn
            .query_row("SELECT COUNT(*) FROM run_series", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "dry-run inserted rows into run_series table");
    }
}

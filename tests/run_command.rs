use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

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
      "command": ["yes", "SAMPLE\t1000\t50000\tdata={build}"],
      "config": { "build": ["opt"] }
    }
  ]
}"#;
    fs::write(data_dir.join("config.json"), config).unwrap();
}

#[test]
fn run_command_stores_run_series() {
    let tmp = tempfile::tempdir().unwrap();
    let bin = env!("CARGO_BIN_EXE_aoc-bench");

    write_config(tmp.path());
    let data_dir = tmp.path().join("data");

    let output = Command::new(bin)
        .arg("run")
        .arg("--data-dir")
        .arg(&data_dir)
        .env("BENCH_HOST", "testhost")
        .env("RUST_LOG_FORMAT", "json")
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
    assert_eq!(v["median_mean_ns_per_iter"], 50.0);
}

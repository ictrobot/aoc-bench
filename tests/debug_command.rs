use serde_json::Value;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const BENCH_EXECUTABLE: &str = env!("CARGO_BIN_EXE_aoc-bench");

fn debug_command(extra_args: &[&str], json: Option<&Path>) -> Output {
    let mut command = Command::new(BENCH_EXECUTABLE);
    command.arg("debug").args(extra_args);
    if let Some(path) = json {
        command.arg("--json").arg(path);
    }
    let output = command
        .arg("--")
        .arg("/usr/bin/yes")
        .arg("SAMPLE\t123\t34000000")
        .output()
        .expect("spawn debug command");
    assert!(
        output.status.success(),
        "debug command failed: status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn read_series(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

#[test]
fn debug_json_replaces_stdout() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("series.json");

    let output = debug_command(&[], Some(&path));

    assert_eq!(output.stdout, b"");
    assert_eq!(read_series(&path)["summary"]["run_count"], 9);
}

#[test]
fn debug_json_with_summary_writes_both() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("series.json");

    let output = debug_command(&["--summary", "--runs"], Some(&path));

    // Every sample reports the same time, so the summary is exact
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "\
276.42 µs/iter ±0.00% (9 runs)
runs: 276.42 276.42 276.42 276.42 276.42 276.42 276.42 276.42 276.42 µs
run 1: 276.42 µs/iter se ±0.00% (59 samples)
run 2: 276.42 µs/iter se ±0.00% (59 samples)
run 3: 276.42 µs/iter se ±0.00% (59 samples)
run 4: 276.42 µs/iter se ±0.00% (59 samples)
run 5: 276.42 µs/iter se ±0.00% (59 samples)
run 6: 276.42 µs/iter se ±0.00% (59 samples)
run 7: 276.42 µs/iter se ±0.00% (59 samples)
run 8: 276.42 µs/iter se ±0.00% (59 samples)
run 9: 276.42 µs/iter se ±0.00% (59 samples)
"
    );
    assert_eq!(read_series(&path)["runs"].as_array().unwrap().len(), 9);
}

#[test]
fn debug_without_json_prints_json() {
    let output = debug_command(&[], None);

    let series: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(series["summary"]["run_count"], 9);
}

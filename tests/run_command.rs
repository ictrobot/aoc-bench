use rusqlite::Connection;
use serde_json::Value;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const BENCH_EXECUTABLE: &str = env!("CARGO_BIN_EXE_aoc-bench");

fn run_command(data_dir: &Path) -> Output {
    Command::new(BENCH_EXECUTABLE)
        .arg("run")
        .arg("--data-dir")
        .arg(data_dir)
        .env("BENCH_HOST", "testhost")
        .output()
        .expect("spawn run command")
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "run command failed: status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn collect_files(dir: &Path, files: &mut Vec<PathBuf>) {
    if !dir.exists() {
        return;
    }
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_files(&path, files);
        } else {
            files.push(path);
        }
    }
}

fn measurement_json_files(data_dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_files(
        &data_dir.join("results/testhost/runs/by-measurement"),
        &mut files,
    );
    files.sort();
    files
}

fn read_measurement(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

fn database(data_dir: &Path) -> Connection {
    Connection::open(data_dir.join("results/testhost/metadata.db")).unwrap()
}

fn database_measurement_id(conn: &Connection) -> String {
    let bytes: Vec<u8> = conn
        .query_row("SELECT measurement_id FROM measurements", [], |row| {
            row.get(0)
        })
        .unwrap();
    Uuid::from_slice(&bytes).unwrap().hyphenated().to_string()
}

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
      "command": ["/usr/bin/yes", "SAMPLE\t123\t34000000\tdata={build}"],
      "config": { "build": ["opt"] }
    }
  ]
}"#;
    fs::write(data_dir.join("config.json"), config).unwrap();
}

#[test]
fn run_command_stores_isolated_measurement() {
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

    assert_eq!(output.stdout, b"");

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

    let json_path = run_series_path.expect("measurement path from logs");
    let data = fs::read_to_string(&json_path).unwrap();
    let v: Value = serde_json::from_str(&data).unwrap();

    // Isolated benchmarks (no dedupe strategy) are recorded as schema-3 isolated measurements.
    assert_eq!(v["schema"], 3);
    assert_eq!(v["summary"]["run_count"], 9);
    assert_eq!(v["bench"], "bench");
    assert_eq!(v["executed_case"]["build"], "opt");
    // Isolated workloads carry no executable/group-spec identity.
    assert!(v["executable_sha256"].is_null());
    assert!(v["group_spec"].is_null());

    for run in v["runs"].as_array().unwrap() {
        assert!((run["mean_ns_per_iter"].as_f64().unwrap() - 276_422.764).abs() < 0.001);
    }
}

#[cfg(unix)]
fn write_sampler(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        path,
        "#!/bin/sh\nwhile true; do printf 'SAMPLE\\t1000\\t20000000\\n'; done\n",
    )
    .unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

#[cfg(unix)]
fn write_dedupe_config(data_dir: &Path, commits: &[&str]) {
    let config = serde_json::json!({
        "config_keys": { "commit": { "values": commits } },
        "benchmarks": [{
            "benchmark": "bench",
            "command": ["builds/{commit}/bin"],
            "config": { "commit": commits },
            "dedupe": "inode-content",
            "stats": {
                "min_samples": 2,
                "min_time_ns": 1,
                "runs_per_series": 1,
                "min_warmup_samples": 1,
                "min_warmup_time_ns": 1
            }
        }]
    });
    fs::write(
        data_dir.join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
}

#[cfg(unix)]
#[test]
fn run_command_stores_single_case_shared_measurement() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    write_sampler(&data_dir.join("builds/a/bin"));
    write_dedupe_config(&data_dir, &["a"]);
    assert_success(&run_command(&data_dir));

    let conn = database(&data_dir);
    let counts: (u64, u64, u64, u64, u64, u64) = conn
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM workloads),
                 (SELECT COUNT(*) FROM measurements),
                 (SELECT COUNT(*) FROM cases),
                 (SELECT COUNT(*) FROM measurement_cases),
                 (SELECT COUNT(workload_id) FROM cases),
                 (SELECT COUNT(*) FROM workloads
                  WHERE stable_measurement_id IS NOT NULL
                    AND stable_measurement_id = last_measurement_id)",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(counts, (1, 1, 1, 1, 1, 1));

    let (workload_sha256, executable_sha256, group_spec): (String, String, String) = conn
        .query_row(
            "SELECT w.workload_sha256, w.executable_sha256, w.group_spec
             FROM measurements m JOIN workloads w ON w.workload_id = m.workload_id",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();

    let files = measurement_json_files(&data_dir);
    assert_eq!(files.len(), 1, "one execution must write one JSON file");
    let measurement = read_measurement(&files[0]);
    assert_eq!(measurement["schema"], 3);
    assert_eq!(
        measurement["measurement_id"],
        database_measurement_id(&conn)
    );
    assert_eq!(measurement["bench"], "bench");
    assert_eq!(measurement["workload_sha256"], workload_sha256);
    assert_eq!(measurement["executable_sha256"], executable_sha256);
    assert_eq!(
        measurement["group_spec"],
        serde_json::from_str::<Value>(&group_spec).unwrap()
    );
    assert_eq!(
        measurement["executed_case"],
        serde_json::json!({ "commit": "a" })
    );
    assert_eq!(
        measurement["covered_cases"],
        serde_json::json!([{ "commit": "a" }])
    );
}

#[cfg(unix)]
#[test]
fn run_command_dedupe_executes_once_for_hardlinked_commits() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    // One sampler, hardlinked under commit a and b -> a single group.
    let real = data_dir.join("builds/real/bin");
    write_sampler(&real);
    for commit in ["a", "b"] {
        let dst = data_dir.join(format!("builds/{commit}/bin"));
        fs::create_dir_all(dst.parent().unwrap()).unwrap();
        fs::hard_link(&real, &dst).unwrap();
    }

    write_dedupe_config(&data_dir, &["a", "b"]);
    assert_success(&run_command(&data_dir));

    // Exactly one measurement covering both cases, both cases pointing at one workload.
    let conn = database(&data_dir);

    let measurements: u64 = conn
        .query_row("SELECT COUNT(*) FROM measurements", [], |r| r.get(0))
        .unwrap();
    assert_eq!(measurements, 1, "hardlinked commits must execute once");

    let cases: u64 = conn
        .query_row("SELECT COUNT(*) FROM cases", [], |r| r.get(0))
        .unwrap();
    assert_eq!(cases, 2);

    let covered: u64 = conn
        .query_row("SELECT COUNT(*) FROM measurement_cases", [], |r| r.get(0))
        .unwrap();
    assert_eq!(covered, 2, "the single measurement covers both cases");

    let workload_links: (u64, u64, u64) = conn
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM workloads),
                 COUNT(*),
                 COUNT(DISTINCT c.workload_id)
             FROM cases c
             JOIN workloads w ON w.workload_id = c.workload_id
             JOIN measurements m ON m.measurement_id = w.stable_measurement_id
             WHERE w.last_measurement_id = m.measurement_id
               AND m.workload_id = w.workload_id",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        workload_links,
        (1, 2, 1),
        "both cases share the sole initialized measured workload"
    );

    let files = measurement_json_files(&data_dir);
    assert_eq!(
        files.len(),
        1,
        "one shared execution must write one JSON file"
    );
    let measurement = read_measurement(&files[0]);
    assert_eq!(measurement["schema"], 3);
    assert_eq!(
        measurement["measurement_id"],
        database_measurement_id(&conn)
    );
    assert!(measurement["executable_sha256"].is_string());
    assert!(measurement["group_spec"].is_object());
    assert_eq!(
        measurement["executed_case"],
        serde_json::json!({ "commit": "a" })
    );
    assert_eq!(
        measurement["covered_cases"],
        serde_json::json!([{ "commit": "a" }, { "commit": "b" }])
    );
}

#[cfg(unix)]
#[test]
fn run_command_new_hardlinked_case_reuses_existing_measurement() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let real = data_dir.join("builds/real/bin");
    write_sampler(&real);
    for commit in ["a", "b"] {
        let dst = data_dir.join(format!("builds/{commit}/bin"));
        fs::create_dir_all(dst.parent().unwrap()).unwrap();
        fs::hard_link(&real, dst).unwrap();
    }

    // Record A first. B exists on disk but is not yet part of the benchmark configuration.
    write_dedupe_config(&data_dir, &["a"]);
    assert_success(&run_command(&data_dir));
    let original_files = measurement_json_files(&data_dir);
    assert_eq!(original_files.len(), 1);
    let original_json = fs::read(&original_files[0]).unwrap();
    let original_measurement = read_measurement(&original_files[0]);

    // Adding hardlinked B makes the group new, but its content identity already has a measurement.
    write_dedupe_config(&data_dir, &["a", "b"]);
    assert_success(&run_command(&data_dir));

    let conn = database(&data_dir);
    let counts: (u64, u64, u64, u64, u64, u64, u64) = conn
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM workloads),
                 (SELECT COUNT(*) FROM measurements),
                 (SELECT COUNT(*) FROM cases),
                 (SELECT COUNT(*) FROM measurement_cases),
                 (SELECT COUNT(workload_id) FROM cases),
                 (SELECT COUNT(DISTINCT workload_id) FROM cases WHERE workload_id IS NOT NULL),
                 (SELECT COUNT(*) FROM workloads
                  WHERE stable_measurement_id IS NOT NULL
                    AND stable_measurement_id = last_measurement_id)",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(counts, (1, 1, 2, 2, 2, 1, 1));

    let b_history: u64 = conn
        .query_row(
            "SELECT COUNT(*)
             FROM measurement_cases mc
             JOIN cases c ON c.case_id = mc.case_id
             WHERE c.config = json_object('commit', 'b')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(b_history, 1, "B must inherit the existing measurement");

    let files = measurement_json_files(&data_dir);
    assert_eq!(
        files, original_files,
        "reuse must not create another JSON file"
    );
    assert_eq!(
        fs::read(&files[0]).unwrap(),
        original_json,
        "reuse must not modify JSON"
    );
    assert_eq!(
        original_measurement["measurement_id"],
        database_measurement_id(&conn)
    );
    assert_eq!(
        original_measurement["covered_cases"],
        serde_json::json!([{ "commit": "a" }]),
        "immutable provenance must not claim inherited B was executed"
    );
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

    // No measurement JSON should have been written.
    let by_measurement = data_dir
        .join("results")
        .join("testhost")
        .join("runs")
        .join("by-measurement");
    if by_measurement.exists() {
        let mut entries = fs::read_dir(&by_measurement).unwrap();
        assert!(entries.next().is_none(), "dry-run wrote measurement files");
    }

    // If metadata DB exists, ensure no measurement/case rows were written.
    let db_path = data_dir
        .join("results")
        .join("testhost")
        .join("metadata.db");
    if db_path.exists() {
        let conn = Connection::open(db_path).unwrap();
        let measurements: u64 = conn
            .query_row("SELECT COUNT(*) FROM measurements", [], |r| r.get(0))
            .unwrap();
        assert_eq!(measurements, 0, "dry-run inserted measurement rows");
        let cases: u64 = conn
            .query_row("SELECT COUNT(*) FROM cases", [], |r| r.get(0))
            .unwrap();
        assert_eq!(cases, 0, "dry-run inserted case rows");
    }
}

/// SIGTERM lets the in-progress group finish and be recorded, then skips the remaining groups.
#[cfg(unix)]
#[test]
fn run_command_sigterm_finishes_current_group_then_stops() {
    use std::os::unix::fs::PermissionsExt as _;

    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    // The sampler signals its parent (aoc-bench) as soon as it starts, so the stop request always
    // arrives while the first group is in progress, then keeps producing samples.
    let sampler = data_dir.join("sampler.sh");
    fs::write(
        &sampler,
        "#!/bin/sh\nkill -TERM $PPID\nwhile true; do printf 'SAMPLE\\t1000\\t20000000\\n'; done\n",
    )
    .unwrap();
    fs::set_permissions(&sampler, fs::Permissions::from_mode(0o755)).unwrap();

    let config = serde_json::json!({
        "config_keys": { "build": { "values": ["a", "b", "c"] } },
        "benchmarks": [{
            "benchmark": "bench",
            "command": [sampler.to_str().unwrap(), "{build}"],
            "config": { "build": ["a", "b", "c"] },
            "stats": {
                "min_samples": 2,
                "min_time_ns": 1,
                "runs_per_series": 1,
                "min_warmup_samples": 1,
                "min_warmup_time_ns": 1
            }
        }]
    });
    fs::write(
        data_dir.join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();

    let output = run_command(&data_dir);
    assert_success(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("stop requested, skipping remaining groups"),
        "stderr={stderr}"
    );

    // The group that was running when the signal arrived was recorded; the other two never ran.
    assert_eq!(
        measurement_json_files(&data_dir).len(),
        1,
        "stderr={stderr}"
    );
}

/// Accept pushes on a local port, answering 204, and collect every request body in arrival order.
fn metrics_sink() -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!(
        "http://127.0.0.1:{}/import?keep=query",
        listener.local_addr().unwrap().port()
    );
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let collected = Arc::clone(&bodies);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&request);
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let len: usize = head
                        .lines()
                        .find_map(|l| l.strip_prefix("Content-Length: "))
                        .unwrap()
                        .parse()
                        .unwrap();
                    if body.len() >= len {
                        break;
                    }
                }
            }
            let text = String::from_utf8(request).unwrap();
            assert!(text.starts_with("POST /import?keep=query HTTP/1.1\r\n"));
            let body = text.split_once("\r\n\r\n").unwrap().1.to_string();
            // Recorded before the response so the body is visible once the client has its answer
            collected.lock().unwrap().push(body);
            stream
                .write_all(
                    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        }
    });
    (url, bodies)
}

/// The value of the series matching `prefix` in the last body that carries it.
fn last_metric(bodies: &[String], prefix: &str) -> Option<f64> {
    bodies.iter().rev().find_map(|body| {
        body.lines()
            .find(|line| line.starts_with(prefix))
            .map(|line| {
                line[prefix.len()..]
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap()
            })
    })
}

#[test]
#[allow(clippy::too_many_lines)]
fn run_command_pushes_metrics() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let config = serde_json::json!({
        "config_keys": {
            "build": { "values": ["opt"], "metric_label": true },
            "commit": { "values": ["abc"] }
        },
        "benchmarks": [{
            "benchmark": "bench",
            "command": ["/usr/bin/yes", "SAMPLE\t123\t34000000\tdata={build}{commit}"],
            "config": { "build": ["opt"], "commit": ["abc"] },
            "stats": {
                "min_samples": 2,
                "min_time_ns": 1,
                "runs_per_series": 3,
                "min_warmup_samples": 1,
                "min_warmup_time_ns": 1
            }
        }]
    });
    fs::write(
        data_dir.join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();

    let (url, bodies) = metrics_sink();
    let output = Command::new(BENCH_EXECUTABLE)
        .arg("run")
        .arg("--data-dir")
        .arg(&data_dir)
        .env("BENCH_HOST", "testhost")
        .env("AOC_BENCH_METRICS_PUSH_URL", &url)
        .output()
        .expect("spawn run command");
    assert_success(&output);

    let bodies = bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 2, "opening and closing pushes: {bodies:?}");
    let opening = &bodies[0];
    let closing = &bodies[1];

    // The opening push carries the batch state and the dataset view before anything ran
    for line in [
        "aoc_bench_active{host=\"testhost\"} 1 ",
        "aoc_bench_batch_groups_selected{host=\"testhost\",class=\"new\"} 1 ",
        "aoc_bench_batch_groups_remaining{host=\"testhost\",class=\"new\"} 1 ",
        "aoc_bench_dataset_groups{host=\"testhost\",class=\"new\"} 1 ",
        "aoc_bench_dataset_groups{host=\"testhost\",class=\"rerun\"} 0 ",
        "aoc_bench_dataset_cases{host=\"testhost\",class=\"new\"} 1 ",
        "aoc_bench_dataset_workloads_suspicious{host=\"testhost\"} 0 ",
        "aoc_bench_dataset_measurements{host=\"testhost\"} 0 ",
        "aoc_bench_dataset_db_bytes{host=\"testhost\"} ",
    ] {
        assert!(opening.contains(line), "{line} in {opening}");
    }
    // No rerun, so no sweep age. Increments only appear once a group checkpoints them.
    assert!(!opening.contains("oldest_measurement"), "{opening}");
    assert!(!opening.contains("aoc_bench_time_seconds"), "{opening}");

    // The closing push carries the group's increments, labelled by the opted-in key only, all
    // stamped with the group's end, plus the batch totals and the final state
    let case = "{host=\"testhost\",bench=\"bench\",config_build=\"opt\"";
    let group_lines: Vec<&str> = closing.lines().filter(|l| l.contains(case)).collect();
    assert!(group_lines.len() >= 5, "{closing}");
    let stamps: std::collections::BTreeSet<&str> = group_lines
        .iter()
        .map(|l| l.rsplit(' ').next().unwrap())
        .collect();
    assert_eq!(
        stamps.len(),
        1,
        "one checkpoint for the group: {group_lines:?}"
    );
    assert_eq!(
        last_metric(&bodies, &format!("aoc_bench_runs{case}}} ")),
        Some(3.0)
    );
    assert!(last_metric(&bodies, &format!("aoc_bench_samples{case}}} ")).unwrap() >= 6.0);
    for phase in ["warmup", "measurement"] {
        let value = last_metric(
            &bodies,
            &format!("aoc_bench_time_seconds{case},phase=\"{phase}\"}} "),
        );
        assert!(value.is_some_and(|v| v > 0.0), "phase {phase}: {closing}");
    }
    // The sampler reports made-up sample times longer than its wall time, so the group's
    // remainder is clamped to zero and dropped as an empty increment
    assert!(!closing.contains(&format!("aoc_bench_time_seconds{case},phase=\"other\"}}")));
    assert_eq!(
        last_metric(
            &bodies,
            &format!("aoc_bench_series_outcomes{case},outcome=\"initial\"}} ")
        ),
        Some(1.0)
    );
    assert_eq!(
        last_metric(
            &bodies,
            &format!("aoc_bench_groups{case},class=\"new\",outcome=\"executed\"}} ")
        ),
        Some(1.0)
    );
    // The batch's time outside groups has no bench, and the totals mirror the increments
    assert!(
        last_metric(
            &bodies,
            "aoc_bench_time_seconds{host=\"testhost\",phase=\"other\"} "
        )
        .is_some()
    );
    assert_eq!(
        last_metric(&bodies, "aoc_bench_batch_runs{host=\"testhost\"} "),
        Some(3.0)
    );
    assert_eq!(
        last_metric(
            &bodies,
            "aoc_bench_batch_groups{host=\"testhost\",class=\"new\",outcome=\"executed\"} "
        ),
        Some(1.0)
    );
    assert_eq!(
        last_metric(
            &bodies,
            "aoc_bench_batch_groups_remaining{host=\"testhost\",class=\"new\"} "
        ),
        Some(0.0)
    );
    assert!(
        closing.contains("aoc_bench_active{host=\"testhost\"} 0 "),
        "{closing}"
    );
    assert!(!closing.contains("config_commit"), "{closing}");
    assert!(
        !closing.contains("aoc_bench_group_start_timestamp_seconds"),
        "{closing}"
    );
    assert!(!closing.contains("_total"), "{closing}");
}

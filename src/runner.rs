// Runner: process spawning, SAMPLE collection, run series execution

use crate::protocol::{
    parse_line, validate_checksum, validate_meta_version, ParseError, ProtocolLine,
};
use crate::stats::{StatsAccumulator, StatsError, StatsResult, StatsState};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tracing::{info, info_span, trace, trace_span, warn, Span};

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

const TIMEOUT_SECS: u64 = 120; // 2 minutes
const RUN_SERIES_COUNT: usize = 7; // Number of runs in a series
const MAX_RETRIES: usize = 5; // Maximum retries on failure

/// Result from a single benchmark run
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunResult {
    /// Unix timestamp (seconds since epoch) when this run started
    #[serde(with = "jiff::fmt::serde::timestamp::second::required")]
    pub timestamp: Timestamp,
    #[serde(flatten)]
    pub stats: StatsResult,
}

/// A complete run series containing multiple individual runs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSeries {
    /// Schema version for format compatibility
    pub schema: u32,
    /// Benchmark name/identifier
    pub bench: String,
    /// Configuration key-value pairs (canonically sorted)
    pub config: BTreeMap<String, String>,
    /// Unix timestamp when this run series started
    #[serde(with = "jiff::fmt::serde::timestamp::second::required")]
    pub timestamp: Timestamp,
    /// Individual run results (sorted by `mean_ns_per_iter`)
    pub runs: Vec<RunResult>,
    /// Mean from the median run (representative value)
    pub median_mean_ns_per_iter: f64,
    /// CI half-width from the median run
    pub median_ci95_half_width_ns: f64,
    /// Output validation checksum (if provided)
    pub checksum: Option<String>,
}

pub struct Runner {
    command: which::CanonicalPath,
    args: Vec<String>,
    expected_checksum: Option<String>,
    stdin_input: Option<Vec<u8>>,
}

impl Runner {
    pub fn new(command: which::CanonicalPath) -> Self {
        Runner {
            command,
            args: Vec::new(),
            expected_checksum: None,
            stdin_input: None,
        }
    }

    pub fn with_args(mut self, args: Vec<String>) -> Self {
        self.args = args;
        self
    }

    pub fn with_expected_checksum(mut self, checksum: String) -> Self {
        self.expected_checksum = Some(checksum);
        self
    }

    pub fn with_stdin_input(mut self, input: Vec<u8>) -> Self {
        self.stdin_input = Some(input);
        self
    }

    /// Execute a complete run series (default: 7 runs) with retry logic
    ///
    /// Returns a `RunSeries` containing all runs sorted by mean, with median statistics.
    #[tracing::instrument(skip(self))]
    pub fn run_series(
        &self,
        bench: String,
        config: BTreeMap<String, String>,
    ) -> Result<RunSeries, RunError> {
        let series_start = Timestamp::now();

        let mut runs = Vec::with_capacity(RUN_SERIES_COUNT);
        for run in 0..RUN_SERIES_COUNT {
            let span = info_span!("run", run);
            let _enter = span.enter();

            for retry in 0..MAX_RETRIES {
                let span = info_span!("retry", retry);
                let _enter = span.enter();

                match self.run_single() {
                    Ok(run_result) => {
                        info!(
                            samples = run_result.stats.samples.len(),
                            mean_ns = run_result.stats.mean_ns_per_iter,
                            "run successful"
                        );
                        runs.push(run_result);
                        break;
                    }
                    Err(e) if retry < MAX_RETRIES - 1 => {
                        warn!(
                            error = %e,
                            "run failed, retrying"
                        );
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        // Sort runs by mean_ns_per_iter
        runs.sort_by(|a, b| {
            a.stats
                .mean_ns_per_iter
                .partial_cmp(&b.stats.mean_ns_per_iter)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Extract median run statistics (middle run after sorting)
        let median_idx = runs.len() / 2;
        let median_mean_ns_per_iter = runs[median_idx].stats.mean_ns_per_iter;
        let median_ci95_half_width_ns = runs[median_idx].stats.ci95_half_width_ns;

        info!(
            median_mean_ns = median_mean_ns_per_iter,
            median_ci95_ns = median_ci95_half_width_ns,
            "completed run series"
        );

        Ok(RunSeries {
            schema: 1,
            bench,
            config,
            timestamp: series_start,
            runs,
            median_mean_ns_per_iter,
            median_ci95_half_width_ns,
            checksum: self.expected_checksum.clone(),
        })
    }

    /// Execute a single benchmark run
    #[tracing::instrument(skip(self), level = "trace")]
    fn run_single(&self) -> Result<RunResult, RunError> {
        let start_time = Timestamp::now();
        let start_instant = Instant::now();

        // Spawn the child process
        let (mut child, stdout) = self.spawn_child()?;

        // Collect samples from stdout
        let mut stats = StatsAccumulator::default();
        let reader = BufReader::new(stdout);
        let mut lines = reader.lines();
        loop {
            // Check timeout
            if start_instant.elapsed() > Duration::from_secs(TIMEOUT_SECS) {
                return Err(RunError::Timeout);
            }

            let Some(Ok(line)) = lines.next() else {
                return if let Ok(Some(code)) = child.child.try_wait()
                    && !code.success()
                {
                    Err(RunError::ProcessCrashed(code.code()))
                } else {
                    Err(RunError::PrematureEof)
                };
            };
            let line = line.trim();

            if line.is_empty() {
                continue;
            }

            // Parse protocol line
            let line = parse_line(line);
            trace!(?line, "protocol line");
            match line {
                Ok(ProtocolLine::Meta(meta)) => {
                    // Validate version if present
                    if let Err(e) = validate_meta_version(&meta) {
                        return Err(RunError::ParseError(e));
                    }
                }
                Ok(ProtocolLine::Sample(sample)) => {
                    // Validate checksum if expected
                    if let Some(ref expected) = self.expected_checksum
                        && let Err(e) = validate_checksum(&sample, expected)
                    {
                        return Err(RunError::InvalidChecksum(e));
                    }

                    // Add sample to accumulator
                    match stats.add_sample(sample.iters, sample.total_ns) {
                        StatsState::MoreSamplesNeeded => {}
                        StatsState::Abort(err) => {
                            return Err(RunError::StatsFailed(err));
                        }
                        StatsState::Done => break,
                    }
                }
                Err(e) => {
                    warn!(error = %e, "failed to parse protocol line");
                    // Continue on parse errors per design doc
                }
            }
        }

        Ok(RunResult {
            timestamp: start_time,
            stats: stats.finish(),
        })
    }

    fn spawn_child(&self) -> Result<(RunnerChild, ChildStdout), RunError> {
        let mut cmd = Command::new(&self.command);
        cmd.args(&self.args);
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        if self.stdin_input.is_some() {
            cmd.stdin(Stdio::piped());
        } else {
            cmd.stdin(Stdio::null());
        }

        // Create a new temp directory for this spawn
        let temp_dir = TempDir::with_prefix(env!("CARGO_PKG_NAME"))?;
        cmd.current_dir(temp_dir.path());

        // Disable ASLR on Linux for consistent measurements
        //
        // Particularly on AMD K8, ASLR can cause significant differences in performance run to run.
        // For example,  ~105us or ~175us for 2015 day 1 depending on memory layout.
        #[cfg(target_os = "linux")]
        #[allow(clippy::cast_sign_loss)]
        unsafe {
            cmd.pre_exec(|| {
                let current = libc::personality(0xffff_ffff);
                if current == -1 {
                    return Err(io::Error::last_os_error());
                }

                let ret = libc::personality((current | libc::ADDR_NO_RANDOMIZE) as libc::c_ulong);
                if ret == -1 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }

        let mut child = cmd.spawn()?;

        // Write input to stdin in a separate thread to avoid deadlock
        if let Some(input) = self.stdin_input.clone() {
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| io::Error::other("failed to capture stdin pipe"))?;

            let parent_span = Span::current();
            std::thread::spawn(move || {
                let _parent_enter = parent_span.enter();
                let span = trace_span!("stdin_write");
                let _enter = span.enter();

                let _ = stdin.write_all(&input);
                drop(stdin);

                trace!("child stdin closed");
            });
        }

        // Read and log stderr in a separate thread to avoid deadlock
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("failed to capture stderr pipe"))?;
        let parent_span = Span::current();
        std::thread::spawn(move || {
            let _parent_enter = parent_span.enter();
            let span = trace_span!("stdin_write");
            let _enter = span.enter();

            let reader = BufReader::new(stderr);
            for line in reader.lines() {
                let line = line.unwrap();
                warn!(%line, "stderr output from child process");
            }

            trace!("child stderr closed");
        });

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("failed to capture stdout pipe"))?;

        Ok((
            RunnerChild {
                child,
                temp_dir: Some(temp_dir),
            },
            stdout,
        ))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("Failed to spawn process: {0}")]
    SpawnFailed(#[from] io::Error),
    #[error("Process crashed with exit code: {0:?}")]
    ProcessCrashed(Option<i32>),
    #[error("Process timed out after {TIMEOUT_SECS} seconds")]
    Timeout,
    #[error("Failed to parse protocol line: {0}")]
    ParseError(ParseError),
    #[error("Process ended prematurely")]
    PrematureEof,
    #[error("Checksum validation failed: {0}")]
    InvalidChecksum(ParseError),
    #[error("Statistics collection failed: {0}")]
    StatsFailed(StatsError),
}

impl RunSeries {
    /// Format the run series result for display
    ///
    /// Returns a string like "30.92 µs/iter ±0.10% (median of 7 runs)"
    pub fn display_result(&self) -> String {
        let mean_us = self.median_mean_ns_per_iter / 1000.0;
        let ci_percent = (self.median_ci95_half_width_ns / self.median_mean_ns_per_iter) * 100.0;

        format!(
            "{:.2} µs/iter ±{:.2}% (median of {} runs)",
            mean_us,
            ci_percent,
            self.runs.len()
        )
    }
}

impl std::fmt::Display for RunSeries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_result())
    }
}

struct RunnerChild {
    child: Child,
    temp_dir: Option<TempDir>,
}

impl Drop for RunnerChild {
    fn drop(&mut self) {
        let pid = self.child.id();
        trace!(pid, "killing child process");
        if let Err(error) = self.child.kill() {
            warn!(pid, %error, "failed to kill child process");
        }
        trace!(pid, "waiting for child process to exit");
        match self.child.wait() {
            Ok(status) => trace!(pid, exit_code = status.code(), "child process exited"),
            Err(err) => warn!(pid, %err, "failed to wait for child process"),
        }

        if let Some(temp_dir) = self.temp_dir.take()
            && let Err(error) = temp_dir.close()
        {
            warn!(%error, "failed to close child temp directory");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::{EstimationMode, Sample};

    #[test]
    fn test_runner_with_yes() {
        // Create a simple test that uses yes to simulate benchmark output
        // This is a basic sanity test; more complex tests would use a mock binary
        let executable = which::CanonicalPath::new("yes").unwrap();
        let runner = Runner::new(executable).with_args(vec!["SAMPLE\t1000\t50000".to_string()]);

        let result = runner.run_single();
        assert!(result.is_ok());

        // Verify all fields are properly populated
        let mut result = result.unwrap();
        assert!((result.stats.mean_ns_per_iter - 50.0).abs() < 0.001);
        assert!((result.stats.ci95_half_width_ns - 0.0).abs() < 0.001); // Can be 0 for identical samples
        assert_eq!(result.stats.mode, EstimationMode::PerIter);
        assert!(result.stats.intercept_ns.is_none());
        assert!(result.stats.samples.iter().all(|s| matches!(
            s,
            Sample {
                iters: 1000,
                total_ns: 50000
            }
        )));
        assert!((result.stats.mean_ns_per_iter - 50.0).abs() < 0.001);

        // Check series result
        let mut series_result = runner
            .run_series("yes".to_string(), BTreeMap::new())
            .unwrap();
        assert_eq!(series_result.schema, 1);
        assert_eq!(series_result.bench, "yes");
        assert!((series_result.median_mean_ns_per_iter - 50.0).abs() < 0.001);
        assert!((series_result.median_ci95_half_width_ns - 0.0).abs() < 0.001);

        // Check runs match, ignoring timestamp
        result.timestamp = Timestamp::now();
        series_result
            .runs
            .iter_mut()
            .for_each(|r| r.timestamp = result.timestamp);
        assert_eq!(series_result.runs, vec![result; RUN_SERIES_COUNT]);
    }

    #[test]
    fn test_runner_with_cat() {
        // Create a simple test that uses cat to test input
        let executable = which::CanonicalPath::new("cat").unwrap();
        let runner = Runner::new(executable);
        let result = runner.run_single();
        assert!(matches!(result, Err(RunError::PrematureEof)));

        let result = runner
            .with_stdin_input("SAMPLE\t1000\t50000\n".to_string().repeat(100).into_bytes())
            .run_single();
        let result = result.unwrap();
        assert!((result.stats.mean_ns_per_iter - 50.0).abs() < 0.001);
    }

    #[test]
    fn test_runner_with_false() {
        let executable = which::CanonicalPath::new("false").unwrap();
        let runner = Runner::new(executable);

        assert!(matches!(
            runner.run_single(),
            Err(RunError::ProcessCrashed(Some(1)))
        ));

        assert!(matches!(
            runner.run_series("false".to_string(), BTreeMap::new()),
            Err(RunError::ProcessCrashed(Some(1)))
        ));
    }

    #[test]
    fn test_run_series_display() {
        let mut config = BTreeMap::new();
        config.insert("test".to_string(), "value".to_string());

        let series = RunSeries {
            schema: 1,
            bench: "test-bench".to_string(),
            config,
            timestamp: Timestamp::from_second(1000).unwrap(),
            runs: vec![
                RunResult {
                    timestamp: Timestamp::from_second(1000).unwrap(),
                    stats: StatsResult {
                        mean_ns_per_iter: 30920.0, // 30.92 µs
                        ci95_half_width_ns: 310.0, // ±1%
                        mode: EstimationMode::PerIter,
                        intercept_ns: None,
                        outlier_count: 0,
                        samples: vec![],
                        temporal_correlation: 0.0,
                    }
                };
                7
            ],
            median_mean_ns_per_iter: 30920.0,
            median_ci95_half_width_ns: 310.0,
            checksum: None,
        };

        let display = series.display_result();
        assert!(display.contains("30.92 µs/iter"));
        assert!(display.contains("±1.00%"));
        assert!(display.contains("median of 7 runs"));
    }

    #[test]
    fn test_run_series_json_round_trip() {
        let mut config = BTreeMap::new();
        config.insert("commit".to_string(), "abc1234".to_string());
        config.insert("host".to_string(), "pi5".to_string());

        let series = RunSeries {
            schema: 1,
            bench: "2015-04".to_string(),
            config,
            timestamp: Timestamp::from_second(1_763_287_200).unwrap(),
            runs: vec![RunResult {
                timestamp: Timestamp::from_second(1_763_287_201).unwrap(),
                stats: StatsResult {
                    mean_ns_per_iter: 30_920_000.0,
                    ci95_half_width_ns: 31_000.0,
                    mode: EstimationMode::PerIter,
                    intercept_ns: None,
                    outlier_count: 0,
                    samples: vec![Sample {
                        iters: 10_000_000,
                        total_ns: 30_920_000_000,
                    }],
                    temporal_correlation: 0.0,
                },
            }],
            median_mean_ns_per_iter: 30_920_000.0,
            median_ci95_half_width_ns: 31_000.0,
            checksum: Some("8f024a8e".to_string()),
        };

        // Serialize RunSeries to JSON
        let json = serde_json::to_string_pretty(&series).unwrap();
        assert!(json.contains("\"schema\": 1"));
        assert!(json.contains("\"bench\": \"2015-04\""));
        assert!(json.contains("\"commit\": \"abc1234\""));
        assert!(json.contains("\"mode\": \"per_iter\"")); // Verify snake_case serialization
        assert!(!json.contains("\"stats\":")); // Verify stats is flattened
        assert!(json.contains("\"timestamp\": 1763287200")); // Verify timestamp encoded in seconds

        // Deserialize back
        let deserialized: RunSeries = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.schema, 1);
        assert_eq!(deserialized.bench, "2015-04");
        assert_eq!(deserialized.runs.len(), 1);
        assert_eq!(deserialized.runs[0].stats.mode, EstimationMode::PerIter);
        assert!((deserialized.median_mean_ns_per_iter - 30_920_000.0).abs() < 0.001);
        assert_eq!(deserialized.checksum, Some("8f024a8e".to_string()));

        // Serialize RunResult to JSON
        let json = serde_json::to_string_pretty(&deserialized.runs[0]).unwrap();
        assert!(json.contains("\"timestamp\": 1763287201")); // Verify timestamp encoded in seconds
    }
}

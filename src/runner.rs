// Runner: process spawning, SAMPLE collection, run series execution

use crate::protocol::{
    parse_line, validate_checksum, validate_meta_version, ParseError, ProtocolLine,
};
use crate::stats::{EstimationMode, StatsAccumulator};
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const TIMEOUT_SECS: u64 = 120; // 2 minutes

#[derive(Debug)]
pub enum RunError {
    SpawnFailed(std::io::Error),
    ProcessCrashed(Option<i32>),
    Timeout,
    ParseError(ParseError),
    PrematureEof,
    InvalidChecksum(ParseError),
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::SpawnFailed(e) => write!(f, "Failed to spawn process: {e}"),
            RunError::ProcessCrashed(code) => {
                write!(f, "Process crashed with exit code: {code:?}")
            }
            RunError::Timeout => write!(f, "Process timed out after {TIMEOUT_SECS} seconds"),
            RunError::ParseError(e) => write!(f, "Failed to parse protocol line: {e}"),
            RunError::PrematureEof => write!(f, "Process ended prematurely"),
            RunError::InvalidChecksum(e) => write!(f, "Checksum validation failed: {e}"),
        }
    }
}

impl std::error::Error for RunError {}

#[derive(Debug, Clone)]
pub struct RunResult {
    pub mean_ns_per_iter: f64,
    pub mode: EstimationMode,
    pub intercept_ns: Option<f64>,
    pub sample_count: usize,
}

pub struct Runner {
    command: String,
    args: Vec<String>,
    working_dir: Option<String>,
    expected_checksum: Option<String>,
}

impl Runner {
    pub fn new(command: String) -> Self {
        Runner {
            command,
            args: Vec::new(),
            working_dir: None,
            expected_checksum: None,
        }
    }

    pub fn with_args(mut self, args: Vec<String>) -> Self {
        self.args = args;
        self
    }

    pub fn with_working_dir(mut self, dir: String) -> Self {
        self.working_dir = Some(dir);
        self
    }

    pub fn with_expected_checksum(mut self, checksum: String) -> Self {
        self.expected_checksum = Some(checksum);
        self
    }

    /// Spawn the benchmark command and collect samples
    pub fn run(&self) -> Result<RunResult, RunError> {
        // Spawn the child process
        let mut child = self.spawn_child()?;

        // Collect samples from stdout
        let mut stats = StatsAccumulator::new();
        let start_time = Instant::now();

        let stdout = child.stdout.take().expect("Failed to capture stdout");
        let reader = BufReader::new(stdout);

        for line in reader.lines() {
            // Check timeout
            if start_time.elapsed() > Duration::from_secs(TIMEOUT_SECS) {
                let _ = child.kill();
                return Err(RunError::Timeout);
            }

            let line = line.map_err(|_| RunError::PrematureEof)?;
            let line = line.trim();

            if line.is_empty() {
                continue;
            }

            // Parse protocol line
            match parse_line(line) {
                Ok(ProtocolLine::Meta(meta)) => {
                    // Validate version if present
                    if let Err(e) = validate_meta_version(&meta) {
                        let _ = child.kill();
                        return Err(RunError::ParseError(e));
                    }
                }
                Ok(ProtocolLine::Sample(sample)) => {
                    // Validate checksum if expected
                    if let Some(ref expected) = self.expected_checksum
                        && let Err(e) = validate_checksum(&sample, expected)
                    {
                        let _ = child.kill();
                        return Err(RunError::InvalidChecksum(e));
                    }

                    // Add sample to accumulator
                    stats.add_sample(sample.iters, sample.total_ns);
                }
                Err(e) => {
                    eprintln!("Warning: Failed to parse line: {line} - {e}");
                    // Continue on parse errors per design doc
                }
            }
        }

        // Wait for process to finish
        let status = child.wait().map_err(|_| RunError::PrematureEof)?;

        if !status.success() {
            return Err(RunError::ProcessCrashed(status.code()));
        }

        // Check that we got some samples
        if stats.sample_count() == 0 {
            return Err(RunError::PrematureEof);
        }

        // Compute results
        let mode = stats.detect_mode();
        let mean_ns_per_iter = stats.point_estimate(mode);
        let intercept_ns = if mode == EstimationMode::Regression {
            Some(stats.compute_wls().intercept)
        } else {
            None
        };

        Ok(RunResult {
            mean_ns_per_iter,
            mode,
            intercept_ns,
            sample_count: stats.sample_count(),
        })
    }

    fn spawn_child(&self) -> Result<Child, RunError> {
        let mut cmd = Command::new(&self.command);
        cmd.args(&self.args);
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::inherit());

        if let Some(ref dir) = self.working_dir {
            cmd.current_dir(dir);
        }

        cmd.spawn().map_err(RunError::SpawnFailed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runner_with_echo() {
        // Create a simple test that uses echo to simulate benchmark output
        // This is a basic sanity test; more complex tests would use a mock binary
        let runner = Runner::new("echo".to_string()).with_args(vec![
            "SAMPLE".to_string(),
            "1000".to_string(),
            "50000".to_string(),
        ]);

        let result = runner.run();
        assert!(result.is_ok());

        let result = result.unwrap();
        assert_eq!(result.sample_count, 1);
        assert!((result.mean_ns_per_iter - 50.0).abs() < 0.1);
    }
}

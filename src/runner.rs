// Runner: process spawning, SAMPLE collection, run series execution

use crate::config::{BenchmarkId, BenchmarkVariant, Config};
use crate::host_config::{CpuAffinity, HostConfig};
use crate::protocol::{
    ParseError, ProtocolLine, parse_line, validate_checksum, validate_meta_version,
};
use crate::run::{Run, RunSeries};
use crate::stats::{StatsAccumulator, StatsError, StatsOptions, StatsState};
use jiff::Timestamp;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};
use std::{fs, io};
use tempfile::TempDir;
use tracing::{Span, info, info_span, trace, trace_span, warn};

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

const MAX_RETRIES: usize = 5; // Maximum retries on failure

#[derive(Debug, Clone)]
pub struct Runner {
    executable: which::CanonicalPath,
    args: Vec<String>,
    expected_checksum: Option<String>,
    stdin_input: Option<Vec<u8>>,
    benchmark_id: BenchmarkId,
    benchmark_config: Config,
    host_config: HostConfig,
    stats: StatsOptions,
}

impl Runner {
    pub fn new(
        data_dir: &Path,
        benchmark: &BenchmarkVariant,
        config: Config,
        host_config: HostConfig,
    ) -> Result<Self, RunError> {
        let mut args = config.expand_templates(benchmark.command_template())?;

        let executable = args.remove(0);
        let executable =
            which::CanonicalPath::new_in(&executable, std::env::var_os("PATH"), data_dir)
                .map_err(|error| RunError::ExecutableNotFound { executable, error })?;

        let stdin_input = if let Some(path) = benchmark.input() {
            Some(
                fs::read(path).map_err(|error| RunError::ReadingInputFailed {
                    path: path.into(),
                    error,
                })?,
            )
        } else {
            None
        };

        Ok(Runner {
            executable,
            args,
            expected_checksum: benchmark.checksum().map(str::to_string),
            stdin_input,
            benchmark_id: benchmark.benchmark_id().clone(),
            benchmark_config: config,
            host_config,
            stats: benchmark.stats_options(),
        })
    }

    /// Execute a complete run series (fixed 3 runs) with no retries on timeout
    ///
    /// Returns a `RunSeries` containing all runs sorted by mean, with median statistics.
    #[tracing::instrument(skip(self))]
    pub fn run_series(&self) -> Result<RunSeries, RunError> {
        let series_start = Timestamp::now();

        let run_count = self.stats.runs_per_series.get();
        assert!(!run_count.is_multiple_of(2));

        let mut runs = Vec::with_capacity(run_count);
        for run in 0..run_count {
            let _span = info_span!("run", run).entered();

            for retry in 0..MAX_RETRIES {
                let _span = info_span!("retry", retry).entered();

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
                    Err(e @ RunError::Timeout { .. }) => {
                        warn!(error = %e, "run timed out, not retrying");
                        return Err(e);
                    }
                    Err(e) if retry < MAX_RETRIES - 1 => {
                        warn!(
                            error = %e,
                            "run failed, retrying"
                        );
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            "run failed, no retries left"
                        );
                        return Err(e);
                    }
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
            runs = run_count,
            "completed run series"
        );

        Ok(RunSeries {
            schema: 1,
            bench: self.benchmark_id.clone(),
            config: self.benchmark_config.clone(),
            timestamp: series_start,
            runs,
            median_mean_ns_per_iter,
            median_ci95_half_width_ns,
            checksum: self.expected_checksum.clone(),
        })
    }

    /// Execute a single benchmark run
    #[tracing::instrument(skip(self), level = "trace")]
    fn run_single(&self) -> Result<Run, RunError> {
        let start_time = Timestamp::now();
        let start_instant = Instant::now();

        // Spawn the child process
        let (mut child, stdout) = self.spawn_child()?;

        // Collect samples from stdout
        let mut stats = StatsAccumulator::with_options(self.stats);
        let reader = BufReader::new(stdout);
        let mut lines = reader.lines();
        loop {
            // Check timeout
            if start_instant.elapsed() > Duration::from_nanos(self.stats.run_timeout_ns.get()) {
                // Return the existing data if the accumulator has sufficient samples
                if let StatsState::MoreSamplesOptional = stats.state() {
                    break;
                }

                return Err(RunError::Timeout {
                    timeout: Duration::from_nanos(self.stats.run_timeout_ns.get()),
                });
            }

            let Some(Ok(line)) = lines.next() else {
                let mut result = child.child.try_wait();

                if let Ok(None) = result {
                    // Try to wait for the child to finish so its error code can be included in the
                    // error message
                    std::thread::sleep(Duration::from_millis(250));
                    result = child.child.try_wait();
                }

                return if let Ok(Some(code)) = result
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
                        StatsState::MoreSamplesRequired | StatsState::MoreSamplesOptional => {}
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

        Ok(Run {
            timestamp: start_time,
            stats: stats.finish(),
        })
    }

    fn spawn_child(&self) -> Result<(RunnerChild, ChildStdout), RunError> {
        let mut cmd = Command::new(&self.executable);
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

        if let CpuAffinity::Cpus(cpus) = &self.host_config.cpu_affinity {
            if let Err(e) = set_affinity(&mut cmd, cpus) {
                warn!(error = %e, "failed to set cpu affinity");
            } else {
                trace!(cpus = %&self.host_config.cpu_affinity, "cpu affinity set for child process");
            }
        }

        if self.host_config.disable_aslr {
            if let Err(e) = disable_aslr(&mut cmd) {
                warn!(error = %e, "failed to disable ASLR");
            } else {
                trace!("aslr disabled for child process");
            }
        }

        let mut child = cmd.spawn()?;
        trace!(pid = child.id(), "spawned child process");

        // Write input to stdin in a separate thread to avoid deadlock
        if let Some(input) = self.stdin_input.clone() {
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| io::Error::other("failed to capture stdin pipe"))?;

            let parent_span = Span::current();
            std::thread::spawn(move || {
                let _parent_enter = parent_span.enter();
                let _span = trace_span!("stdin_write").entered();

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
            let _span = trace_span!("stdin_write").entered();

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
    #[error("failed to build command from config: {0}")]
    ConfigError(#[from] crate::config::ConfigError),
    #[error("failed to find executable '{executable}': {error}")]
    ExecutableNotFound {
        executable: String,
        #[source]
        error: which::Error,
    },
    #[error("failed to read input file '{path:?}': {error}")]
    ReadingInputFailed {
        path: PathBuf,
        #[source]
        error: io::Error,
    },
    #[error("failed to spawn process: {0}")]
    SpawnFailed(#[from] io::Error),
    #[error("process crashed with exit code: {0:?}")]
    ProcessCrashed(Option<i32>),
    #[error("process timed out after {timeout:?}")]
    Timeout { timeout: Duration },
    #[error("failed to parse protocol line: {0}")]
    ParseError(ParseError),
    #[error("process ended prematurely")]
    PrematureEof,
    #[error("checksum validation failed: {0}")]
    InvalidChecksum(ParseError),
    #[error("statistics collection failed: {0}")]
    StatsFailed(StatsError),
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

#[cfg(target_os = "linux")]
fn set_affinity(cmd: &mut Command, cpus: &[usize]) -> io::Result<()> {
    let Some(&max) = cpus.iter().max() else {
        return Err(io::Error::other("empty cpu set"));
    };
    if max >= libc::CPU_SETSIZE as usize {
        return Err(io::Error::other("max cpu number >= CPU_SETSIZE"));
    }

    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe { libc::CPU_ZERO(&mut set) };

    for &cpu in cpus {
        unsafe { libc::CPU_SET(cpu, &mut set) };
    }

    let hook = move || {
        let result = unsafe { libc::sched_setaffinity(0, size_of_val(&set), &raw const set) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    };

    unsafe { cmd.pre_exec(hook) };

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn set_affinity(_: &mut Command, _: &[usize]) -> io::Result<()> {
    Err(io::Error::other("not supported on this platform"))
}

#[cfg(target_os = "linux")]
#[allow(clippy::cast_sign_loss, clippy::unnecessary_wraps)]
fn disable_aslr(cmd: &mut Command) -> io::Result<()> {
    let hook = || {
        let current = unsafe { libc::personality(0xffff_ffff) };
        if current == -1 {
            return Err(io::Error::last_os_error());
        }

        let ret =
            unsafe { libc::personality((current | libc::ADDR_NO_RANDOMIZE) as libc::c_ulong) };
        if ret == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    };

    unsafe { cmd.pre_exec(hook) };

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn disable_aslr(_: &mut Command) -> io::Result<()> {
    Err(io::Error::other("not supported on this platform"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Benchmark, ConfigProduct};
    use crate::stats::{EstimationMode, Sample, StatsOptions};
    use std::num::{NonZeroU64, NonZeroUsize};
    use tempfile::NamedTempFile;

    #[test]
    fn test_runner_with_yes() {
        // Create a simple test that uses yes to simulate benchmark output
        // This is a basic sanity test; more complex tests would use a mock binary
        let tmp_dir = TempDir::new().unwrap();
        let benchmark = Benchmark::new(
            "yes".try_into().unwrap(),
            ConfigProduct::default(),
            vec!["yes".into(), "SAMPLE\t1000\t20000000".into()],
            None,
            None,
            StatsOptions::default(),
        )
        .unwrap();
        let variant = &benchmark.variants()[0];
        let runner = Runner::new(
            tmp_dir.path(),
            variant,
            Config::default(),
            HostConfig::default(),
        )
        .unwrap();

        let result = runner.run_single();
        assert!(result.is_ok());

        // Verify all fields are properly populated
        let mut result = result.unwrap();
        assert!((result.stats.mean_ns_per_iter - 20_000.0).abs() < 0.001);
        assert!((result.stats.ci95_half_width_ns - 0.0).abs() < 0.001); // Can be 0 for identical samples
        assert_eq!(result.stats.mode, EstimationMode::PerIter);
        assert!(result.stats.intercept_ns.is_none());
        assert!(result.stats.samples.iter().all(|s| matches!(
            s,
            Sample {
                iters: 1000,
                total_ns: 20_000_000
            }
        )));
        assert!((result.stats.mean_ns_per_iter - 20_000.0).abs() < 0.001);

        // Check series result
        let mut series_result = runner.run_series().unwrap();
        assert_eq!(series_result.schema, 1);
        assert_eq!(series_result.bench, "yes".try_into().unwrap());
        assert!((series_result.median_mean_ns_per_iter - 20_000.0).abs() < 0.001);
        assert!((series_result.median_ci95_half_width_ns - 0.0).abs() < 0.001);

        let run_count = variant.stats_options().runs_per_series.get();

        // Check runs match, ignoring timestamp
        result.timestamp = Timestamp::now();
        series_result
            .runs
            .iter_mut()
            .for_each(|r| r.timestamp = result.timestamp);
        assert_eq!(series_result.runs, vec![result; run_count]);
    }

    #[test]
    fn test_runner_respects_runs_per_series_override() {
        for runs in [1, 5, 9] {
            let tmp_dir = TempDir::new().unwrap();
            let stats = StatsOptions {
                runs_per_series: NonZeroUsize::new(runs).unwrap(),
                ..StatsOptions::default()
            };

            let benchmark = Benchmark::new(
                "yes-5".try_into().unwrap(),
                ConfigProduct::default(),
                vec!["yes".into(), "SAMPLE\t1\t1000000000".into()],
                None,
                None,
                stats,
            )
            .unwrap();
            let variant = &benchmark.variants()[0];

            let runner = Runner::new(
                tmp_dir.path(),
                variant,
                Config::default(),
                HostConfig::default(),
            )
            .unwrap();

            let series = runner.run_series().unwrap();
            assert_eq!(series.runs.len(), runs);
        }
    }

    #[test]
    fn test_runner_respects_run_timeout_override() {
        // Use a tiny timeout so we hit it before sample minimums
        let tmp_dir = TempDir::new().unwrap();
        let stats = StatsOptions {
            run_timeout_ns: NonZeroU64::new(100).unwrap(), // 100ns
            ..StatsOptions::default()
        };

        let benchmark = Benchmark::new(
            "yes".try_into().unwrap(),
            ConfigProduct::default(),
            vec!["yes".into(), "SAMPLE\t1000\t20000".into()],
            None,
            None,
            stats,
        )
        .unwrap();

        let variant = &benchmark.variants()[0];
        let runner = Runner::new(
            tmp_dir.path(),
            variant,
            Config::default(),
            HostConfig::default(),
        )
        .unwrap();

        let err = runner.run_single().unwrap_err();
        assert!(matches!(err, RunError::Timeout { .. }));
    }

    #[test]
    fn test_runner_with_cat() {
        // Create a simple test that uses cat to test input
        let tmp_dir = TempDir::new().unwrap();
        let create_benchmark = |input| {
            Benchmark::new(
                "cat".try_into().unwrap(),
                ConfigProduct::default(),
                vec!["cat".into()],
                input,
                None,
                StatsOptions::default(),
            )
            .unwrap()
        };

        let benchmark = create_benchmark(None);
        let variant = &benchmark.variants()[0];
        let runner = Runner::new(
            tmp_dir.path(),
            variant,
            Config::default(),
            HostConfig::default(),
        )
        .unwrap();
        let result = runner.run_single();
        assert!(matches!(result, Err(RunError::PrematureEof)));

        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file
            .write_all(
                "SAMPLE\t1000\t50000000\n"
                    .to_string()
                    .repeat(100)
                    .as_bytes(),
            )
            .unwrap();

        let benchmark = create_benchmark(Some(tmp_file.path().to_owned()));
        let variant = &benchmark.variants()[0];
        let runner = Runner::new(
            tmp_dir.path(),
            variant,
            Config::default(),
            HostConfig::default(),
        )
        .unwrap();
        let result = runner.run_single().unwrap();
        assert!((result.stats.mean_ns_per_iter - 50000.0).abs() < 0.001);
    }

    #[test]
    fn test_runner_with_false() {
        let tmp_dir = TempDir::new().unwrap();
        let benchmark = Benchmark::new(
            "false".try_into().unwrap(),
            ConfigProduct::default(),
            vec!["false".into()],
            None,
            None,
            StatsOptions::default(),
        )
        .unwrap();
        let variant = &benchmark.variants()[0];
        let runner = Runner::new(
            tmp_dir.path(),
            variant,
            Config::default(),
            HostConfig::default(),
        )
        .unwrap();

        assert!(matches!(
            runner.run_single(),
            Err(RunError::ProcessCrashed(_))
        ));

        assert!(matches!(
            runner.run_series(),
            Err(RunError::ProcessCrashed(_))
        ));
    }
}

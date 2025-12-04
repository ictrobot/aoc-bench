use crate::cli::CliError;
use aoc_bench::config::{Benchmark, BenchmarkId, Config, ConfigError, ConfigFile};
use aoc_bench::engine::{RunEngine, RunEngineConfig, StatsEngine};
use aoc_bench::host_config::{CpuAffinity, HostConfig};
use clap::Args;
use std::path::PathBuf;

pub const DEFAULT_DATA_DIR: &str = "data";

#[derive(Clone, Debug, Args)]
pub struct CommonRunArgs {
    /// Path to the data directory
    #[arg(long, value_parser = clap::value_parser!(PathBuf), default_value = DEFAULT_DATA_DIR)]
    data_dir: PathBuf,

    /// Execute benchmarks but do not save the results
    #[arg(long)]
    dry_run: bool,

    /// Force the new run series to become the stable result
    #[arg(long)]
    force_update_stable: bool,
}

impl TryFrom<CommonRunArgs> for RunEngine {
    type Error = CliError;

    fn try_from(value: CommonRunArgs) -> Result<Self, Self::Error> {
        let host = get_host()?;
        let config_file = ConfigFile::new(&value.data_dir, Some(host.as_str()))?;

        let host_key = config_file.host_key().clone();
        let host_kv = host_key
            .value_from_name(&host)
            .expect("config file must include provided host");

        Ok(RunEngine::new(RunEngineConfig {
            config_file,
            host_kv,
            dry_run: value.dry_run,
            force_update_stable: value.force_update_stable,
        })?)
    }
}

#[derive(Clone, Debug, Args)]
pub struct CommonRunFilterArgs {
    /// Config filter (key=value,key=value format, host key not allowed)
    #[arg(long)]
    config: Option<String>,

    /// Benchmark filter
    #[arg(value_name = "BENCH")]
    benchmark: Option<String>,
}

impl CommonRunFilterArgs {
    pub fn get_filter<'a>(
        &self,
        config_file: &'a ConfigFile,
    ) -> Result<(Option<&'a Benchmark>, Config), CliError> {
        let benchmark = get_benchmark_filter(config_file, self.benchmark.as_deref())?;
        let config = get_config_filter(
            config_file,
            self.config.as_deref(),
            ConfigFile::config_without_host_from_string,
        )?;

        Ok((benchmark, config))
    }
}

#[derive(Clone, Debug, Args)]
pub struct CommonStatsArgs {
    /// Path to the data directory
    #[arg(long, value_parser = clap::value_parser!(PathBuf), default_value = DEFAULT_DATA_DIR)]
    data_dir: PathBuf,
}

impl TryFrom<CommonStatsArgs> for StatsEngine {
    type Error = CliError;

    fn try_from(value: CommonStatsArgs) -> Result<Self, Self::Error> {
        let config_file = ConfigFile::new(&value.data_dir, None)?;
        Ok(StatsEngine::new(config_file))
    }
}

#[derive(Clone, Debug, Args)]
pub struct CommonStatsFilterArgs {
    /// Config filter (key=value,key=value format)
    #[arg(long)]
    config: Option<String>,

    /// Host filter. Equivalent to `--config host=<host>`
    #[arg(long)]
    host: Option<String>,

    /// Benchmark filter
    #[arg(value_name = "BENCH")]
    benchmark: Option<String>,
}

impl CommonStatsFilterArgs {
    pub fn get_filter<'a>(
        &self,
        config_file: &'a ConfigFile,
    ) -> Result<(Option<&'a BenchmarkId>, Config), CliError> {
        let benchmark = get_benchmark_filter(config_file, self.benchmark.as_deref())?;
        let mut config = get_config_filter(
            config_file,
            self.config.as_deref(),
            ConfigFile::config_from_string,
        )?;

        if let Some(host) = self.host.as_deref() {
            let host_key = config_file.host_key();
            if config.get(host_key).is_some() {
                return Err(CliError::HostFilterConflict {});
            }

            let host_kv = host_key
                .value_from_name(host)
                .ok_or_else(|| CliError::InvalidHostFilter(host.to_string()))?;
            config = config.with(host_kv.clone());
        }

        Ok((benchmark.map(Benchmark::id), config))
    }
}

#[derive(Clone, Debug, Args)]
pub struct HostConfigArgs {
    /// Override CPU affinity for child processes
    #[arg(long)]
    cpu_affinity: Option<CpuAffinity>,
    /// Override whether ASLR is disabled for child processes
    #[arg(long)]
    disable_aslr: Option<bool>,
}

impl From<HostConfigArgs> for HostConfig {
    fn from(overrides: HostConfigArgs) -> HostConfig {
        let mut config = HostConfig::default();
        if let Some(cpu_affinity) = overrides.cpu_affinity {
            config.cpu_affinity = cpu_affinity;
        }
        if let Some(disable_aslr) = overrides.disable_aslr {
            config.disable_aslr = disable_aslr;
        }
        config
    }
}

pub fn get_host() -> Result<String, CliError> {
    std::env::var("BENCH_HOST").or_else(|_| {
        hostname::get()
            .map(|os| os.to_string_lossy().to_string())
            .map_err(CliError::CurrentHostError)
    })
}

pub fn get_benchmark_filter<'a>(
    config_file: &'a ConfigFile,
    value: Option<&str>,
) -> Result<Option<&'a Benchmark>, CliError> {
    match value {
        None => Ok(None),
        Some(s) => {
            let id = BenchmarkId::new(s).map_err(|error| CliError::InvalidBenchmarkFilter {
                value: s.to_string(),
                error,
            })?;

            if let Some(bench) = config_file.benchmark_by_id(&id) {
                Ok(Some(bench))
            } else {
                Err(CliError::UnknownBenchmark(id))
            }
        }
    }
}

fn get_config_filter(
    config_file: &ConfigFile,
    value: Option<&str>,
    parse_fn: fn(&ConfigFile, &str) -> Result<Config, ConfigError>,
) -> Result<Config, CliError> {
    match value {
        None => Ok(Config::new()),
        Some(s) => parse_fn(config_file, s).map_err(|error| CliError::InvalidConfigFilter {
            value: s.to_string(),
            error,
        }),
    }
}

mod debug;
mod export;
mod impact;
mod run;
mod sample;
mod timeline;

use aoc_bench::host_config::{CpuAffinity, HostConfig};
use clap::{Args, Subcommand};
use std::process::ExitCode;

pub const DEFAULT_DATA_DIR: &str = "data";

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Execute benchmarks by spawning commands from the config file
    Run(run::RunArgs),

    /// Periodically re-run benchmarks for drift detection
    Sample(sample::SampleArgs),

    /// Query and export benchmark results
    Export(export::ExportArgs),

    /// Show performance history across one config dimension
    Timeline(timeline::TimelineArgs),

    /// Show which benchmarks changed for a specific config value
    Impact(impact::ImpactArgs),

    /// Debug the runner independently with a raw command
    Debug(debug::DebugArgs),
}

impl Commands {
    pub fn execute(self) -> ExitCode {
        match self {
            Commands::Run(args) => run::execute(args),
            Commands::Sample(args) => sample::execute(args),
            Commands::Export(args) => export::execute(args),
            Commands::Timeline(args) => timeline::execute(args),
            Commands::Impact(args) => impact::execute(args),
            Commands::Debug(args) => debug::execute(args),
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct HostConfigOverrides {
    /// Override CPU affinity for child processes
    #[arg(long)]
    cpu_affinity: Option<CpuAffinity>,
    /// Override whether ASLR is disabled for child processes
    #[arg(long)]
    disable_aslr: Option<bool>,
}

impl From<HostConfigOverrides> for HostConfig {
    fn from(overrides: HostConfigOverrides) -> HostConfig {
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

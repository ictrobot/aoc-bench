mod debug;
mod export;
mod impact;
mod run;
mod sample;
mod timeline;

use clap::Subcommand;
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

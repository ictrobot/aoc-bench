mod debug;
mod export;
mod impact;
mod run;
mod run_all;
mod timeline;

use crate::cli::CliError;
use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run a subset of new and existing benchmarks
    Run(run::RunArgs),

    /// Execute all matching benchmarks
    RunAll(run_all::RunAllArgs),

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
    pub fn execute(self) -> Result<(), CliError> {
        match self {
            Commands::RunAll(args) => run_all::execute(args),
            Commands::Run(args) => run::execute(args),
            Commands::Export(args) => export::execute(args),
            Commands::Timeline(args) => timeline::execute(args),
            Commands::Impact(args) => impact::execute(args),
            Commands::Debug(args) => debug::execute(args),
        }
    }
}

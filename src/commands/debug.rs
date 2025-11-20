use crate::commands::HostConfigOverrides;
use aoc_bench::config::{Benchmark, Config, ConfigProduct};
use aoc_bench::runner;
use clap::Args;
use runner::Runner;
use std::path::Path;
use std::process::ExitCode;
use tracing::error;

#[derive(Args, Debug)]
pub struct DebugArgs {
    /// Input file path to provide to the runner
    #[arg(long, value_parser = clap::value_parser!(std::path::PathBuf))]
    pub input: Option<std::path::PathBuf>,

    /// Expected checksum for output validation
    #[arg(long)]
    pub checksum: Option<String>,

    #[command(flatten)]
    pub host_config: HostConfigOverrides,

    /// Command and arguments to run (after --)
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

pub fn execute(args: DebugArgs) -> ExitCode {
    if args.command.is_empty() {
        error!("no command specified");
        return ExitCode::FAILURE;
    }

    let benchmark = match Benchmark::new(
        "debug".try_into().unwrap(),
        ConfigProduct::default(),
        args.command,
        args.input,
        args.checksum,
    ) {
        Ok(benchmark) => benchmark,
        Err(error) => {
            error!(%error, "failed to construct benchmark instance");
            return ExitCode::FAILURE;
        }
    };
    let variant = &benchmark.variants()[0];

    let host_config = args.host_config.into();

    let runner = match Runner::new(Path::new("."), variant, Config::new(), host_config) {
        Ok(runner) => runner,
        Err(error) => {
            error!(%error, "failed to construct runner instance");
            return ExitCode::FAILURE;
        }
    };

    match runner.run_series() {
        Ok(series) => {
            println!("{}", serde_json::to_string_pretty(&series).unwrap());
            ExitCode::SUCCESS
        }
        Err(e) => {
            error!(error = %e, "benchmark run failed");
            ExitCode::FAILURE
        }
    }
}

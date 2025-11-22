use crate::cli::CliError;
use crate::cli::args::HostConfigArgs;
use aoc_bench::config::{Benchmark, Config, ConfigProduct};
use aoc_bench::runner;
use clap::Args;
use runner::Runner;
use std::path::Path;

#[derive(Args, Debug)]
pub struct DebugArgs {
    /// Input file path to provide to the runner
    #[arg(long, value_parser = clap::value_parser!(std::path::PathBuf))]
    pub input: Option<std::path::PathBuf>,

    /// Expected checksum for output validation
    #[arg(long)]
    pub checksum: Option<String>,

    #[command(flatten)]
    pub host_config: HostConfigArgs,

    /// Command and arguments to run (after --)
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

pub fn execute(args: DebugArgs) -> Result<(), CliError> {
    let benchmark = Benchmark::new(
        "debug".try_into().unwrap(),
        ConfigProduct::default(),
        args.command,
        args.input,
        args.checksum,
    )
    .map_err(CliError::BenchmarkConstructionError)?;
    let variant = &benchmark.variants()[0];

    let host_config = args.host_config.into();

    let runner = Runner::new(Path::new("."), variant, Config::new(), host_config)
        .map_err(CliError::RunnerConstructionError)?;

    let series = runner.run_series().map_err(CliError::BenchmarkRunError)?;

    serde_json::to_writer_pretty(std::io::stdout(), &series).unwrap();

    Ok(())
}

use crate::cli::CliError;
use crate::cli::args::HostConfigArgs;
use crate::cli::format::format_series_summary;
use aoc_bench::config::{Benchmark, Config, ConfigProduct};
use aoc_bench::host_config::HostConfig;
use aoc_bench::runner;
use aoc_bench::stats::StatsOptions;
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

    /// Print the measurement summary as text. Disables printing the run series JSON to stdout
    #[arg(long)]
    pub summary: bool,

    /// With --summary, also list each run in the order it ran
    #[arg(long, requires = "summary")]
    pub runs: bool,

    /// Write the run series JSON to this file. Disables printing it to stdout
    #[arg(long, value_name = "PATH", value_parser = clap::value_parser!(std::path::PathBuf))]
    pub json: Option<std::path::PathBuf>,

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
        StatsOptions::default(),
    )
    .map_err(CliError::BenchmarkConstructionError)?;
    let variant = &benchmark.variants()[0];

    let host_config: HostConfig = args.host_config.into();

    let runner = Runner::new(Path::new("."), variant, Config::new(), &host_config)
        .map_err(CliError::RunnerConstructionError)?;

    let series = runner.run_series().map_err(CliError::BenchmarkRunError)?;

    if let Some(path) = &args.json {
        let json = serde_json::to_vec_pretty(&series).unwrap();
        std::fs::write(path, json).map_err(|error| CliError::JsonWriteError {
            path: path.clone(),
            error,
        })?;
    }

    if args.summary {
        print!("{}", format_series_summary(&series, args.runs));
    } else if args.json.is_none() {
        serde_json::to_writer_pretty(std::io::stdout(), &series).unwrap();
    }

    Ok(())
}

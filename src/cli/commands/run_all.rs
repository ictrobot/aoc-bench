use crate::cli::CliError;
use crate::cli::args::{CommonRunArgs, CommonRunFilterArgs};
use aoc_bench::config::Benchmark;
use aoc_bench::engine::{RunEngine, RunMode};
use clap::Args;
use tracing::info;

#[derive(Args, Debug)]
pub struct RunAllArgs {
    #[command(flatten)]
    filter: CommonRunFilterArgs,

    #[command(flatten)]
    run: CommonRunArgs,
}

pub fn execute(args: RunAllArgs) -> Result<(), CliError> {
    let engine: RunEngine = args.run.try_into()?;
    let (benchmark, config_filter) = args.filter.get_filter(&engine.config_file)?;

    let benchmarks: Vec<&Benchmark> = engine
        .config_file
        .benchmarks_filtered(benchmark.map(Benchmark::id))
        .iter()
        .collect();

    let report = engine.run(&benchmarks, &config_filter, RunMode::All)?;

    if report.groups_selected == 0 {
        return Err(CliError::NoBenchmarksFound);
    }

    info!(
        groups = report.groups_selected,
        reused = report.reused,
        executed = report.executed,
        "complete"
    );
    Ok(())
}

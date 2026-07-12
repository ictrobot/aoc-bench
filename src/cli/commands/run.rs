use crate::cli::CliError;
use crate::cli::args::{CommonRunArgs, CommonRunFilterArgs};
use aoc_bench::config::Benchmark;
use aoc_bench::engine::{RunEngine, RunMode};
use clap::Args;
use tracing::info;

#[derive(Args, Debug)]
pub struct RunArgs {
    /// Maximum number of new groups to process
    #[arg(long, default_value = "16")]
    pub new_limit: usize,

    /// Maximum number of measured groups to re-run for stable checks, oldest first
    #[arg(long, default_value = "8")]
    pub rerun_limit: usize,

    #[command(flatten)]
    filter: CommonRunFilterArgs,

    #[command(flatten)]
    run: CommonRunArgs,
}

pub fn execute(args: RunArgs) -> Result<(), CliError> {
    let new_limit = args.new_limit;
    let rerun_limit = args.rerun_limit;
    let engine: RunEngine = args.run.try_into()?;

    let (benchmark, config_filter) = args.filter.get_filter(&engine.config_file)?;
    let benchmarks: Vec<&Benchmark> = engine
        .config_file
        .benchmarks_filtered(benchmark.map(Benchmark::id))
        .iter()
        .collect();

    let report = engine.run(
        &benchmarks,
        &config_filter,
        RunMode::Sample {
            new_limit,
            rerun_limit,
        },
    )?;

    info!(
        groups_selected = report.groups_selected,
        reused = report.reused,
        executed = report.executed,
        failed = report.failed,
        "sampling complete"
    );

    if report.groups_selected == 0 && report.failed == 0 {
        return Err(CliError::NoBenchmarksFound);
    }

    if let Ok(failed) = report.failed.try_into() {
        Err(CliError::BenchmarksFailed(failed))
    } else {
        Ok(())
    }
}

use crate::cli::CliError;
use crate::cli::args::{CommonRunArgs, CommonRunFilterArgs};
use aoc_bench::config::Benchmark;
use aoc_bench::engine::{RunEngine, RunMode};
use aoc_bench::run::{NewGroupOrder, RunScheduleConfig};
use clap::{Args, ValueEnum};
use tracing::info;

#[derive(Args, Debug)]
pub struct RunArgs {
    /// Maximum number of new groups to process
    #[arg(long, default_value = "16")]
    pub new_limit: usize,

    /// Maximum number of measured groups to re-run for stable checks, oldest first
    #[arg(long, default_value = "8")]
    pub rerun_limit: usize,

    /// Maximum reruns when no new groups are selected (defaults to --rerun-limit)
    #[arg(long)]
    rerun_only_limit: Option<usize>,

    /// Ordering applied before limiting new groups
    #[arg(long, value_enum, default_value_t)]
    new_order: NewOrderArg,

    #[command(flatten)]
    filter: CommonRunFilterArgs,

    #[command(flatten)]
    run: CommonRunArgs,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum NewOrderArg {
    #[default]
    Random,
    TimelineAsc,
    TimelineDesc,
}

impl From<NewOrderArg> for NewGroupOrder {
    fn from(value: NewOrderArg) -> Self {
        match value {
            NewOrderArg::Random => Self::Random,
            NewOrderArg::TimelineAsc => Self::TimelineAsc,
            NewOrderArg::TimelineDesc => Self::TimelineDesc,
        }
    }
}

pub fn execute(args: RunArgs) -> Result<(), CliError> {
    let new_limit = args.new_limit;
    let rerun_limit = args.rerun_limit;
    let rerun_only_limit = args.rerun_only_limit.unwrap_or(rerun_limit);
    let new_order = NewGroupOrder::from(args.new_order);
    let engine: RunEngine = args.run.try_into()?;

    if new_order != NewGroupOrder::Random && engine.config_file.timeline_key().is_none() {
        return Err(CliError::TimelineOrderWithoutTimelineKey);
    }

    let (benchmark, config_filter) = args.filter.get_filter(&engine.config_file)?;
    let benchmarks: Vec<&Benchmark> = engine
        .config_file
        .benchmarks_filtered(benchmark.map(Benchmark::id))
        .iter()
        .collect();

    let report = engine.run(
        &benchmarks,
        &config_filter,
        RunMode::Sample(RunScheduleConfig {
            new_limit,
            rerun_limit,
            rerun_only_limit,
            new_order,
        }),
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

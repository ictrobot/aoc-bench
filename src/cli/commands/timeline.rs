use crate::cli::CliError;
use crate::cli::args::{CommonStatsArgs, CommonStatsFilterArgs};
use crate::cli::format::{
    DELTA_WIDTH, DURATION_WIDTH, format_ci, format_delta, format_duration_ns,
};
use aoc_bench::config::BenchmarkId;
use aoc_bench::engine::{StatsEngine, TimelineSummary};
use clap::Args;
use std::io::{self, Write};

#[derive(Args, Debug)]
pub struct TimelineArgs {
    /// Relative change threshold percentage
    #[arg(long, default_value = "10")]
    pub threshold: f64,

    #[command(flatten)]
    stats: CommonStatsArgs,

    #[command(flatten)]
    filter: CommonStatsFilterArgs,
}

pub fn execute(args: TimelineArgs) -> Result<(), CliError> {
    let engine: StatsEngine = args.stats.try_into()?;

    if args.threshold <= 0.0 {
        return Err(CliError::InvalidThreshold);
    }

    let (benchmark_opt, config) = args.filter.get_filter(&engine.config_file)?;
    let benchmark: &BenchmarkId = if let Some(benchmark) = benchmark_opt {
        benchmark
    } else if let [benchmark] = engine.config_file.benchmarks() {
        benchmark.id()
    } else {
        return Err(CliError::BenchmarkRequired);
    };

    let timeline =
        engine.timeline_summary_with_threshold(benchmark, &config, args.threshold / 100.0)?;

    let _ = print_timeline(&timeline);

    Ok(())
}

fn print_timeline(timeline: &TimelineSummary) -> io::Result<()> {
    let mut out = io::BufWriter::new(io::stdout().lock());

    let max_comparison_key_len = timeline
        .changes
        .iter()
        .map(|(p, _)| p)
        .chain(std::iter::once(&timeline.initial))
        .map(|p| {
            p.config
                .get(&timeline.comparison_key)
                .unwrap()
                .value_name()
                .len()
        })
        .chain(std::iter::once(timeline.comparison_key.name().len()))
        .max()
        .unwrap();

    writeln!(out, "Bench:      {}", timeline.benchmark)?;
    writeln!(out, "Config:     {}", timeline.shared_config)?;
    writeln!(out, "Comparison: {}", timeline.comparison_key.name())?;
    writeln!(out)?;
    writeln!(
        out,
        "{:<max_comparison_key_len$} {:>DURATION_WIDTH$} {:>DURATION_WIDTH$} {:>DELTA_WIDTH$}",
        timeline.comparison_key.name(),
        "mean",
        "CI",
        "delta"
    )?;

    writeln!(
        out,
        "{:<max_comparison_key_len$} {:>DURATION_WIDTH$} {:>DURATION_WIDTH$} {:>DELTA_WIDTH$} INITIAL",
        timeline.initial.comparison_value.value_name(),
        format_duration_ns(timeline.initial.stats.median_run_mean_ns),
        format_ci(timeline.initial.stats.median_run_ci95_half_ns),
        "--",
    )?;

    for (point, change) in &timeline.changes {
        writeln!(
            out,
            "{:<max_comparison_key_len$} {:>DURATION_WIDTH$} {:>DURATION_WIDTH$} {:>DELTA_WIDTH$} {}",
            point.comparison_value.value_name(),
            format_duration_ns(point.stats.median_run_mean_ns),
            format_ci(point.stats.median_run_ci95_half_ns),
            format_delta(*change),
            change.direction,
        )?;
    }

    if timeline.omitted > 0 {
        writeln!(
            out,
            "\n({} entries with insignificant changes omitted)",
            timeline.omitted
        )?;
    }

    out.flush()
}

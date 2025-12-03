use crate::cli::CliError;
use crate::cli::args::{CommonStatsArgs, CommonStatsFilterArgs};
use aoc_bench::config::Benchmark;
use aoc_bench::engine::{StatsEngine, StatsEngineError, TimelineSummary};
use aoc_bench::stable::{Change, ChangeDirection};
use clap::Args;
use std::io::{self, Write};

#[derive(Args, Debug)]
pub struct TimelineArgs {
    /// Relative change threshold percentage
    #[arg(long, default_value = "15")]
    pub threshold: f64,

    #[command(flatten)]
    stats: CommonStatsArgs,

    #[command(flatten)]
    filter: CommonStatsFilterArgs,
}

pub fn execute(args: TimelineArgs) -> Result<(), CliError> {
    let engine: StatsEngine = args.stats.try_into()?;

    let (benchmark_opt, config) = args.filter.get_filter(&engine.config_file)?;
    let benchmark: &Benchmark = if let Some(benchmark) = benchmark_opt {
        benchmark
    } else if let [benchmark] = engine.config_file.benchmarks() {
        benchmark
    } else {
        return Err(CliError::BenchmarkRequired);
    };

    let timeline =
        engine.timeline_summary_with_threshold(benchmark.id(), &config, args.threshold / 100.0)?;
    print_timeline(&timeline)
        .map_err(|error| CliError::StatsEngineError(StatsEngineError::OutputError(error)))
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
        "{:<max_comparison_key_len$} {:>14} {:>14} {:>12}",
        timeline.comparison_key.name(),
        "mean",
        "CI",
        "delta"
    )?;

    writeln!(
        out,
        "{:<max_comparison_key_len$} {:>14} {:>14} {:>12} INITIAL",
        timeline.initial.comparison_value.value_name(),
        format_duration_ns(timeline.initial.stats.median_run_mean_ns),
        format_ci(timeline.initial.stats.median_run_ci95_half_ns),
        "--",
    )?;

    for (point, change) in &timeline.changes {
        writeln!(
            out,
            "{:<max_comparison_key_len$} {:>14} {:>14} {:>12} {}",
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

fn format_duration_ns(ns: f64) -> String {
    if ns >= 1_000_000_000.0 {
        format!("{:.2} s", ns / 1_000_000_000.0)
    } else if ns >= 1_000_000.0 {
        format!("{:.2} ms", ns / 1_000_000.0)
    } else if ns >= 1_000.0 {
        format!("{:.2} µs", ns / 1_000.0)
    } else {
        format!("{ns:.0} ns")
    }
}

fn format_ci(ns: f64) -> String {
    format!("±{}", format_duration_ns(ns))
}

fn format_delta(change: Change) -> String {
    let pct = change.rel_change * 100.0;
    match change.direction {
        ChangeDirection::Regression => format!("+{pct:.2}%"),
        ChangeDirection::Improvement => format!("-{pct:.2}%"),
    }
}

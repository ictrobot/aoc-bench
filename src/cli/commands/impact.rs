use crate::cli::CliError;
use crate::cli::args::{CommonStatsArgs, CommonStatsFilterArgs};
use crate::cli::format::{DELTA_WIDTH, DURATION_WIDTH, format_delta, format_duration_ns};
use aoc_bench::config::{BenchmarkId, Config};
use aoc_bench::engine::{ImpactSummary, StatsEngine};
use clap::Args;
use std::cmp::Ordering;
use std::io::{self, Write};

#[derive(Args, Debug)]
pub struct ImpactArgs {
    /// Config key=value to compare (e.g., commit=abc1234)
    #[arg(value_name = "KEY=VALUE")]
    pub comparison: String,

    /// Relative change threshold percentage
    #[arg(long, default_value = "10")]
    pub threshold: f64,

    #[command(flatten)]
    stats: CommonStatsArgs,

    #[command(flatten)]
    filter: CommonStatsFilterArgs,
}

pub fn execute(args: ImpactArgs) -> Result<(), CliError> {
    let engine: StatsEngine = args.stats.try_into()?;
    let (benchmark, base_filter) = args.filter.get_filter(&engine.config_file)?;

    if args.threshold <= 0.0 {
        return Err(CliError::InvalidThreshold);
    }

    let comparison_config = engine
        .config_file
        .config_from_string(&args.comparison)
        .map_err(|error| CliError::InvalidConfigFilter {
            value: args.comparison.clone(),
            error,
        })?;

    let mut iter = comparison_config.iter();
    let Some(comparison_value) = iter.next().cloned() else {
        return Err(CliError::InvalidImpactComparison(args.comparison.clone()));
    };
    if iter.next().is_some() {
        return Err(CliError::InvalidImpactComparison(args.comparison.clone()));
    }

    let summary = engine.impact(
        &comparison_value,
        benchmark,
        &base_filter,
        args.threshold / 100.0,
    )?;

    let _ = print_impact(summary, benchmark, &base_filter);

    Ok(())
}

fn print_impact(
    mut summary: ImpactSummary,
    benchmark_filter: Option<&BenchmarkId>,
    config_filter: &Config,
) -> io::Result<()> {
    let mut out = io::BufWriter::new(io::stdout().lock());

    write!(
        out,
        "Comparison from {key}={} to {key}={}",
        summary.previous_value.value_name(),
        summary.current_value.value_name(),
        key = summary.comparison_key.name(),
    )?;
    if benchmark_filter.is_some() || !config_filter.is_empty() {
        write!(out, ", filtered to")?;
        if let Some(benchmark) = benchmark_filter {
            write!(out, " {benchmark}")?;
        }
        if !config_filter.is_empty() {
            write!(out, " {config_filter}")?;

            // Remove filtered keys from printed configs
            summary
                .improvements
                .iter_mut()
                .chain(summary.regressions.iter_mut())
                .for_each(|entry| {
                    for kv in config_filter.iter() {
                        entry.config = entry.config.without_key(kv.key());
                    }
                });
        }
    }
    writeln!(out)?;
    writeln!(out)?;

    let (bench_width, config_width) = summary
        .regressions
        .iter()
        .chain(summary.improvements.iter())
        .fold((1, 1), |(bench_width, config_width), entry| {
            (
                bench_width.max(entry.bench.as_str().len()),
                config_width.max(entry.config.to_string().len()),
            )
        });

    for (title, mut entries) in [
        ("IMPROVEMENTS", summary.improvements),
        ("REGRESSIONS", summary.regressions),
    ] {
        writeln!(out, "{title}:")?;
        if entries.is_empty() {
            writeln!(out, "  (none)")?;
            writeln!(out)?;
            continue;
        }

        entries.sort_by(|a, b| {
            b.change
                .rel_change
                .partial_cmp(&a.change.rel_change)
                .unwrap_or(Ordering::Equal)
        });

        for entry in entries {
            writeln!(
                out,
                "   {:<bench_width$} [{:<config_width$}] {:>DURATION_WIDTH$} -> {:>DURATION_WIDTH$} {:>DELTA_WIDTH$}",
                entry.bench,
                entry.config.to_string(),
                format_duration_ns(entry.previous_stats.median_run_mean_ns),
                format_duration_ns(entry.current_stats.median_run_mean_ns),
                format_delta(entry.change),
            )?;
        }

        writeln!(out)?;
    }

    writeln!(out, "{} configs unchanged", summary.unchanged)?;
    if summary.missing_previous > 0 {
        writeln!(
            out,
            "{} {}={} configs missing results",
            summary.missing_previous,
            summary.comparison_key.name(),
            summary.previous_value.value_name()
        )?;
    }

    out.flush()
}

use crate::cli::CliError;
use crate::cli::args::{CommonStatsArgs, CommonStatsFilterArgs};
use crate::cli::format::format_duration_ns;
use aoc_bench::config::{Benchmark, Config};
use aoc_bench::engine::{FastestResult, StatsEngine, StatsEngineError};
use clap::Args;
use std::io::{self, Write};

#[derive(Args, Debug)]
pub struct FastestArgs {
    #[command(flatten)]
    stats: CommonStatsArgs,

    #[command(flatten)]
    filter: CommonStatsFilterArgs,
}

pub fn execute(args: FastestArgs) -> Result<(), CliError> {
    let engine: StatsEngine = args.stats.try_into()?;
    let (benchmark, config) = args.filter.get_filter(&engine.config_file)?;

    let fastest = engine.fastest_configs(benchmark.map(Benchmark::id), &config)?;

    print_fastest(&fastest, &config)
        .map_err(|error| CliError::StatsEngineError(StatsEngineError::OutputError(error)))
}

fn print_fastest(fastest: &[FastestResult], config_filter: &Config) -> io::Result<()> {
    let mut out = io::BufWriter::new(io::stdout().lock());

    let bench_header = "bench";
    let config_header = "config";
    let mean_header = "mean";

    let entries: Vec<(&str, String, f64)> = fastest
        .iter()
        .map(|r| {
            let mut config = r.config.clone();
            for kv in config_filter.iter() {
                config = config.without_key(kv.key());
            }

            (
                r.bench.as_str(),
                config.to_string(),
                r.stable_stats.median_run_mean_ns,
            )
        })
        .collect();

    let total_ns: f64 = entries.iter().map(|(_, _, ns)| *ns).sum();

    let bench_width = entries
        .iter()
        .map(|(bench, _, _)| bench.len())
        .chain(std::iter::once(bench_header.len()))
        .max()
        .unwrap_or(bench_header.len());

    let config_width = entries
        .iter()
        .map(|(_, config, _)| config.len())
        .chain(std::iter::once(config_header.len()))
        .max()
        .unwrap_or(config_header.len());

    writeln!(
        out,
        "{bench_header:<bench_width$} {config_header:<config_width$} {mean_header:>14}",
    )?;

    for (bench, config, mean_ns) in entries {
        writeln!(
            out,
            "{:<bench_width$} {:<config_width$} {:>14}",
            bench,
            config,
            format_duration_ns(mean_ns)
        )?;
    }

    writeln!(out)?;
    writeln!(
        out,
        "total {:>width$}",
        format_duration_ns(total_ns),
        width = 14
    )?;

    out.flush()
}

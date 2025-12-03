use crate::cli::CliError;
use crate::cli::args::{CommonStatsArgs, CommonStatsFilterArgs};
use aoc_bench::config::Benchmark;
use aoc_bench::engine::{StatsEngine, StatsEngineError};
use clap::Args;
use std::io;

#[derive(Args, Debug)]
pub struct ExportArgs {
    #[command(flatten)]
    filter: CommonStatsFilterArgs,

    #[command(flatten)]
    stats: CommonStatsArgs,
}

pub fn execute(args: ExportArgs) -> Result<(), CliError> {
    let engine: StatsEngine = args.stats.try_into()?;
    let (benchmark, config) = args.filter.get_filter(&engine.config_file)?;

    let mut stdout = io::BufWriter::new(io::stdout().lock());

    match engine.export_tsv(&mut stdout, benchmark.map(Benchmark::id), &config) {
        Ok(()) | Err(StatsEngineError::OutputError(_)) => {
            // Ignore any errors writing to stdout. This ensures that e.g. `| head -n10` doesn't error
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

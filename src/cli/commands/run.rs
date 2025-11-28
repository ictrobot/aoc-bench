use crate::cli::args::{CommonFilterArgs, CommonRunArgs};
use crate::cli::CliError;
use aoc_bench::config::Benchmark;
use aoc_bench::engine::RunEngine;
use aoc_bench::storage::Storage;
use clap::Args;
use rand::prelude::*;
use tracing::{error, info, info_span};

#[derive(Args, Debug)]
pub struct RunArgs {
    /// Maximum number of configs to run that have never been run before
    #[arg(long, default_value = "16")]
    pub new_limit: usize,

    /// Maximum number of existing configs to re-run for stable checks, oldest first
    #[arg(long, default_value = "8")]
    pub rerun_limit: usize,

    #[command(flatten)]
    filter: CommonFilterArgs,

    #[command(flatten)]
    run: CommonRunArgs,
}

pub fn execute(args: RunArgs) -> Result<(), CliError> {
    let engine: RunEngine = args.run.try_into()?;
    let storage = &engine.storage;

    let (benchmark, config_filter) = args.filter.get_filter(&engine.config_file)?;
    let benchmark_filter = benchmark.map(Benchmark::id);

    let (mut missing, mut oldest) = storage
        .read_transaction(|tx| {
            Ok((
                storage.missing_results(tx, benchmark_filter, &config_filter, args.new_limit)?,
                storage.oldest_results(tx, benchmark_filter, &config_filter, args.rerun_limit)?,
            ))
        })
        .map_err(CliError::BenchmarkSelectionError)?;

    if missing.is_empty() && oldest.is_empty() {
        return Err(CliError::NoBenchmarksFound);
    }

    info!(
        new_selected = missing.len(),
        existing_selected = oldest.len(),
        "sampling benchmarks"
    );

    // Shuffle the list of candidates within each group
    missing.shuffle(&mut rand::rng());
    oldest.shuffle(&mut rand::rng());

    // Run missing first, then oldest existing
    let candidates = missing
        .iter()
        .map(|(bench, config)| {
            let span = info_span!("bench", %bench, %config).entered();
            info!("running new benchmark config");
            (span, bench, config)
        })
        .chain(oldest.iter().map(|row| {
            let span = info_span!("bench", bench = %row.bench, config = %row.config).entered();
            info!(
                stable_ts = %row.stable_series_timestamp,
                last_ts = %row.last_series_timestamp,
                "rerunning benchmark config"
            );
            (span, &row.bench, &row.config)
        }));

    let mut success = 0;
    let mut failed = 0;
    for (_span, bench_id, config) in candidates {
        let variant = engine
            .config_file
            .benchmark_by_id(bench_id)
            .expect("benchmark returned by storage must exist")
            .variant_for_config(config)
            .expect("config returned by storage must be valid for benchmark");

        if let Err(error) = engine.run(variant, config) {
            error!(error = %error, "failed to run benchmark, skipping");
            failed += 1;
            // Unlike the run subcommand, don't exit immediately on failure
        } else {
            success += 1;
        }
    }

    info!(success, failed, "sampling complete");

    if let Ok(failed) = failed.try_into() {
        Err(CliError::BenchmarksFailed(failed))
    } else {
        Ok(())
    }
}

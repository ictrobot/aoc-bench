use crate::cli::CliError;
use crate::cli::args::{CommonFilterArgs, CommonRunArgs};
use aoc_bench::engine::RunEngine;
use clap::Args;
use tracing::{info, info_span, trace_span};

#[derive(Args, Debug)]
pub struct RunAllArgs {
    #[command(flatten)]
    filter: CommonFilterArgs,

    #[command(flatten)]
    run: CommonRunArgs,
}

pub fn execute(args: RunAllArgs) -> Result<(), CliError> {
    let engine: RunEngine = args.run.try_into()?;
    let (benchmark, config) = args.filter.get_filter(&engine.config_file)?;

    let benchmarks = if let Some(benchmark) = benchmark {
        std::slice::from_ref(benchmark)
    } else {
        engine.config_file.benchmarks()
    };

    let mut count = 0usize;
    for bench in benchmarks {
        let _span = info_span!("bench", bench = %bench.id()).entered();

        for (i, variant) in bench.variants().iter().enumerate() {
            let Some(product) = variant.config().filter(&config) else {
                continue;
            };

            let _span = trace_span!("variant", variant = i).entered();

            for config in &product {
                let span = info_span!("config", %config).entered();

                engine
                    .run(variant, &config)
                    .map_err(|error| CliError::within_span(span, error))?;

                count += 1;
            }
        }
    }

    if count == 0 {
        return Err(CliError::NoBenchmarksFound);
    }

    info!(count, "complete");
    Ok(())
}

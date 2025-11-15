mod cli;
mod config;
mod protocol;
mod runner;
mod stats;
mod storage;

use clap::Parser;
use cli::{Cli, Commands};

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            benchmark,
            config,
            config_json,
        } => {
            println!("Running benchmarks...");
            println!("  benchmark: {benchmark:?}");
            println!("  config: {config:?}");
            println!("  config_json: {config_json:?}");
            // TODO: Implement run command
        }
        Commands::Sample { limit } => {
            println!("Sampling {limit} benchmark(s)...");
            // TODO: Implement sample command
        }
        Commands::Export {
            host,
            config,
            format,
        } => {
            println!("Exporting results...");
            println!("  host: {host:?}");
            println!("  config: {config:?}");
            println!("  format: {format}");
            // TODO: Implement export command
        }
        Commands::Timeline { benchmark, config } => {
            println!("Timeline for benchmark: {benchmark}");
            println!("  config: {config:?}");
            // TODO: Implement timeline command
        }
        Commands::Impact { config } => {
            println!("Impact analysis for config: {config}");
            // TODO: Implement impact command
        }
    }
}

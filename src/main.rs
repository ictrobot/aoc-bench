mod cli;
mod config;
mod protocol;
mod runner;
mod stats;
mod storage;

use clap::Parser;
use cli::{Cli, Commands};
use std::collections::BTreeMap;

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
        Commands::Debug {
            input,
            checksum,
            command,
        } => {
            if command.is_empty() {
                eprintln!("Error: No command specified");
                std::process::exit(1);
            }

            let cmd = &command[0];
            let args = command[1..].to_vec();

            let mut runner = runner::Runner::new(cmd.clone()).with_args(args);

            if let Some(input_path) = input {
                match std::fs::read_to_string(&input_path) {
                    Ok(input_str) => {
                        runner = runner.with_stdin_input(input_str);
                    }
                    Err(e) => {
                        eprintln!("Error reading input file {:?}: {}", input_path, e);
                        std::process::exit(1);
                    }
                }
            }

            if let Some(checksum_str) = checksum {
                runner = runner.with_expected_checksum(checksum_str);
            }

            match runner.run_series("test".to_string(), BTreeMap::new()) {
                Ok(series) => {
                    println!("{}", serde_json::to_string_pretty(&series).unwrap());
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }
}

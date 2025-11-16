mod cli;
mod config;
mod protocol;
mod runner;
mod stats;
mod storage;

use clap::Parser;
use cli::{Cli, Commands};
use std::collections::BTreeMap;
use tracing::{error, info};

fn main() {
    // Initialize tracing subscriber
    let format = std::env::var("RUST_LOG_FORMAT").unwrap_or_default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        );
    if format == "json" {
        subscriber.json().init();
    } else {
        subscriber.init();
    }

    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            benchmark,
            config,
            config_json,
        } => {
            info!(
                benchmark = ?benchmark,
                config = ?config,
                config_json = ?config_json,
                "running benchmarks"
            );
            // TODO: Implement run command
        }
        Commands::Sample { limit } => {
            info!(limit = limit, "sampling benchmarks");
            // TODO: Implement sample command
        }
        Commands::Export {
            host,
            config,
            format,
        } => {
            info!(
                host = ?host,
                config = ?config,
                format = %format,
                "exporting results"
            );
            // TODO: Implement export command
        }
        Commands::Timeline { benchmark, config } => {
            info!(
                benchmark = %benchmark,
                config = ?config,
                "showing timeline"
            );
            // TODO: Implement timeline command
        }
        Commands::Impact { config } => {
            info!(config = %config, "analyzing impact");
            // TODO: Implement impact command
        }
        Commands::Debug {
            input,
            checksum,
            command,
        } => {
            if command.is_empty() {
                error!("no command specified");
                std::process::exit(1);
            }

            let cmd = &command[0];
            let args = command[1..].to_vec();

            let mut runner = runner::Runner::new(cmd.clone()).with_args(args);

            if let Some(input_path) = input {
                match std::fs::read(&input_path) {
                    Ok(input_bytes) => {
                        runner = runner.with_stdin_input(input_bytes);
                    }
                    Err(e) => {
                        error!(
                            path = ?input_path,
                            error = %e,
                            "failed to read input file"
                        );
                        std::process::exit(1);
                    }
                }
            }

            if let Some(checksum_str) = checksum {
                runner = runner.with_expected_checksum(checksum_str);
            }

            match runner.run_series("debug".to_string(), BTreeMap::new()) {
                Ok(series) => {
                    println!("{}", serde_json::to_string_pretty(&series).unwrap());
                }
                Err(e) => {
                    error!(error = %e, "benchmark run failed");
                    std::process::exit(1);
                }
            }
        }
    }
}

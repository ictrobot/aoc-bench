mod commands;

use clap::Parser;
use commands::Commands;

#[derive(Parser, Debug)]
#[command(name = "aoc-bench")]
#[command(about = "Benchmark runner", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

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
    cli.command.execute();
}

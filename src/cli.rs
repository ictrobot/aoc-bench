// CLI interface and argument parsing

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "aoc-bench")]
#[command(about = "Benchmark runner", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Execute benchmarks by spawning commands from the config file
    Run {
        /// Benchmark name to run
        #[arg(long)]
        benchmark: Option<String>,

        /// Config filter (key=value,key=value format)
        #[arg(long)]
        config: Option<String>,

        /// Config filter (JSON format)
        #[arg(long)]
        config_json: Option<String>,
    },

    /// Periodically re-run benchmarks for drift detection
    Sample {
        /// Number of samples to run
        #[arg(long, default_value = "10")]
        limit: usize,
    },

    /// Query and export benchmark results
    Export {
        /// Host to query
        #[arg(long)]
        host: Option<String>,

        /// Config filter (key=value,key=value format)
        #[arg(long)]
        config: Option<String>,

        /// Output format
        #[arg(long, default_value = "tsv")]
        format: String,
    },

    /// Show performance history across one config dimension
    Timeline {
        /// Benchmark name
        benchmark: String,

        /// Config filter (key=value,key=value format)
        #[arg(long)]
        config: Option<String>,
    },

    /// Show which benchmarks changed for a specific config value
    Impact {
        /// Config filter for comparison key (e.g., commit=abc1234)
        #[arg(long)]
        config: String,
    },
}

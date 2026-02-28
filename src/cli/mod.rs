mod args;
mod commands;
pub mod format;
pub mod logging;

use crate::cli::commands::Commands;
use aoc_bench::config::{BenchmarkId, ConfigError};
use aoc_bench::engine::{RunEngineError, StatsEngineError, WebSnapshotExportError};
use aoc_bench::runner::RunError;
use aoc_bench::storage::HybridDiskError;
use clap::Parser;
use std::io;
use std::num::NonZeroUsize;
use std::process::ExitCode;
use tracing::error;
use tracing::span::EnteredSpan;

#[derive(Parser, Debug)]
#[command(name = "aoc-bench")]
#[command(about = "Benchmark runner", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

impl Cli {
    pub fn run(self) -> ExitCode {
        if let Err(error) = self.command.execute() {
            error!(%error, "failed");
            ExitCode::FAILURE
        } else {
            ExitCode::SUCCESS
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("failed to determine current host name: {0}")]
    CurrentHostError(#[source] io::Error),
    #[error("failed to load config file: {0}")]
    ConfigFileError(#[from] ConfigError),
    #[error("invalid benchmark filter '{value}': {error}")]
    InvalidBenchmarkFilter {
        value: String,
        #[source]
        error: ConfigError,
    },
    #[error("unknown benchmark: '{0}'")]
    UnknownBenchmark(BenchmarkId),
    #[error("invalid config filter '{value}': {error}")]
    InvalidConfigFilter {
        value: String,
        #[source]
        error: ConfigError,
    },
    #[error("no matching benchmarks and configs found")]
    NoBenchmarksFound,
    #[error(transparent)]
    RunEngineError(#[from] RunEngineError),
    #[error(transparent)]
    StatsEngineError(#[from] StatsEngineError),
    #[error("{error}")]
    WithinSpan {
        // This keeps the span alive until the error is reported, so the error is logged within the
        // correct span
        span: EnteredSpan,
        #[source]
        error: Box<Self>,
    },

    // run command
    #[error("failed to select benchmarks: {0}")]
    BenchmarkSelectionError(#[source] HybridDiskError),
    #[error("running {0} benchmarks failed")]
    BenchmarksFailed(NonZeroUsize),

    // stats commands
    #[error("--host cannot be used with a --config filter that includes a host key")]
    HostFilterConflict,
    #[error("host '{0}' not found")]
    InvalidHostFilter(String),
    #[error("threshold must be positive")]
    InvalidThreshold,

    // timeline command
    #[error("benchmark argument is required")]
    BenchmarkRequired,

    // impact command
    #[error("comparison must be a single key=value pair (e.g. commit=abc1234); got '{0}'")]
    InvalidImpactComparison(String),

    // debug command
    #[error("failed to construct benchmark: {0}")]
    BenchmarkConstructionError(#[source] ConfigError),
    #[error("failed to construct runner: {0}")]
    RunnerConstructionError(#[source] RunError),
    #[error("failed to run benchmark: {0}")]
    BenchmarkRunError(#[source] RunError),

    // export-web command
    #[error(transparent)]
    WebSnapshotExport(#[from] WebSnapshotExportError),
}

impl CliError {
    pub fn within_span(span: EnteredSpan, error: impl Into<CliError>) -> Self {
        Self::WithinSpan {
            span,
            error: Box::new(error.into()),
        }
    }
}

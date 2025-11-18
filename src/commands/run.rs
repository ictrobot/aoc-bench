use clap::Args;
use std::process::ExitCode;
use tracing::info;

#[derive(Args, Debug)]
pub struct RunArgs {
    /// Benchmark name to run
    #[arg(long)]
    pub benchmark: Option<String>,

    /// Config filter (key=value,key=value format)
    #[arg(long)]
    pub config: Option<String>,

    /// Config filter (JSON format)
    #[arg(long)]
    pub config_json: Option<String>,
}

#[expect(clippy::needless_pass_by_value, reason = "not yet implemented")]
pub fn execute(args: RunArgs) -> ExitCode {
    info!(
        benchmark = ?args.benchmark,
        config = ?args.config,
        config_json = ?args.config_json,
        "running benchmarks"
    );
    // TODO: Implement run command
    ExitCode::FAILURE
}

use clap::Args;
use std::process::ExitCode;
use tracing::info;

#[derive(Args, Debug)]
pub struct TimelineArgs {
    /// Benchmark name
    pub benchmark: String,

    /// Config filter (key=value,key=value format)
    #[arg(long)]
    pub config: Option<String>,
}

#[expect(clippy::needless_pass_by_value, reason = "not yet implemented")]
pub fn execute(args: TimelineArgs) -> ExitCode {
    info!(
        benchmark = %args.benchmark,
        config = ?args.config,
        "showing timeline"
    );
    // TODO: Implement timeline command
    ExitCode::FAILURE
}

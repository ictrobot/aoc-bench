use clap::Args;
use std::process::ExitCode;
use tracing::info;

#[derive(Args, Debug)]
pub struct SampleArgs {
    /// Number of samples to run
    #[arg(long, default_value = "10")]
    pub limit: usize,
}

#[expect(clippy::needless_pass_by_value, reason = "not yet implemented")]
pub fn execute(args: SampleArgs) -> ExitCode {
    info!(limit = args.limit, "sampling benchmarks");
    // TODO: Implement sample command
    ExitCode::FAILURE
}

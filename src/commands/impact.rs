use clap::Args;
use std::process::ExitCode;
use tracing::info;

#[derive(Args, Debug)]
pub struct ImpactArgs {
    /// Config filter for comparison key (e.g., commit=abc1234)
    #[arg(long)]
    pub config: String,
}

#[expect(clippy::needless_pass_by_value, reason = "not yet implemented")]
pub fn execute(args: ImpactArgs) -> ExitCode {
    info!(config = %args.config, "analyzing impact");
    // TODO: Implement impact command
    ExitCode::FAILURE
}

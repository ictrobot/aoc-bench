use clap::Args;
use tracing::info;

#[derive(Args, Debug)]
pub struct ImpactArgs {
    /// Config filter for comparison key (e.g., commit=abc1234)
    #[arg(long)]
    pub config: String,
}

pub fn execute(args: ImpactArgs) {
    info!(config = %args.config, "analyzing impact");
    // TODO: Implement impact command
}

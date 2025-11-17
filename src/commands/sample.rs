use clap::Args;
use tracing::info;

#[derive(Args, Debug)]
pub struct SampleArgs {
    /// Number of samples to run
    #[arg(long, default_value = "10")]
    pub limit: usize,
}

pub fn execute(args: SampleArgs) {
    info!(limit = args.limit, "sampling benchmarks");
    // TODO: Implement sample command
}

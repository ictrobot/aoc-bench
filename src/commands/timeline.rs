use clap::Args;
use tracing::info;

#[derive(Args, Debug)]
pub struct TimelineArgs {
    /// Benchmark name
    pub benchmark: String,

    /// Config filter (key=value,key=value format)
    #[arg(long)]
    pub config: Option<String>,
}

pub fn execute(args: TimelineArgs) {
    info!(
        benchmark = %args.benchmark,
        config = ?args.config,
        "showing timeline"
    );
    // TODO: Implement timeline command
}

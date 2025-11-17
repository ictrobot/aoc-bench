use clap::Args;
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

pub fn execute(args: RunArgs) {
    info!(
        benchmark = ?args.benchmark,
        config = ?args.config,
        config_json = ?args.config_json,
        "running benchmarks"
    );
    // TODO: Implement run command
}

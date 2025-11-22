use crate::cli::CliError;
use clap::Args;

#[derive(Args, Debug)]
pub struct TimelineArgs {
    /// Benchmark name
    pub benchmark: String,

    /// Config filter (key=value,key=value format)
    #[arg(long)]
    pub config: Option<String>,
}

pub fn execute(_args: TimelineArgs) -> Result<(), CliError> {
    todo!()
}

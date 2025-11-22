use crate::cli::CliError;
use clap::Args;

#[derive(Args, Debug)]
pub struct ImpactArgs {
    /// Config filter for comparison key (e.g., commit=abc1234)
    #[arg(long)]
    pub config: String,
}

pub fn execute(_args: ImpactArgs) -> Result<(), CliError> {
    todo!()
}

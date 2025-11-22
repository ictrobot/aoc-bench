use crate::cli::CliError;
use clap::Args;

#[derive(Args, Debug)]
pub struct ExportArgs {
    /// Host to query
    #[arg(long)]
    pub host: Option<String>,

    /// Config filter (key=value,key=value format)
    #[arg(long)]
    pub config: Option<String>,

    /// Output format
    #[arg(long, default_value = "tsv")]
    pub format: String,
}

pub fn execute(_args: ExportArgs) -> Result<(), CliError> {
    todo!()
}

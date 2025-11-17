use clap::Args;
use tracing::info;

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

pub fn execute(args: ExportArgs) {
    info!(
        host = ?args.host,
        config = ?args.config,
        format = %args.format,
        "exporting results"
    );
    // TODO: Implement export command
}

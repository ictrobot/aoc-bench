use clap::Args;
use std::process::ExitCode;
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

#[expect(clippy::needless_pass_by_value, reason = "not yet implemented")]
pub fn execute(args: ExportArgs) -> ExitCode {
    info!(
        host = ?args.host,
        config = ?args.config,
        format = %args.format,
        "exporting results"
    );
    // TODO: Implement export command
    ExitCode::FAILURE
}

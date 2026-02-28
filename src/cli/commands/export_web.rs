use crate::cli::CliError;
use crate::cli::args::DEFAULT_DATA_DIR;
use aoc_bench::config::ConfigFile;
use aoc_bench::engine::export_web_snapshot;
use clap::Args;
use std::path::PathBuf;
use tracing::info;

#[derive(Args, Debug)]
pub struct ExportWebArgs {
    /// Output directory for web data
    #[arg(long)]
    output_dir: PathBuf,

    /// Path to the data directory
    #[arg(long, value_parser = clap::value_parser!(PathBuf), default_value = DEFAULT_DATA_DIR)]
    data_dir: PathBuf,
}

#[allow(clippy::needless_pass_by_value)]
pub fn execute(args: ExportWebArgs) -> Result<(), CliError> {
    let config_file = ConfigFile::new(&args.data_dir, None)?;
    info!(path = %args.output_dir.display(), "exporting web data");

    let Some(result) = export_web_snapshot(&config_file, &args.output_dir)? else {
        info!("no hosts found, nothing to export");
        return Ok(());
    };

    if result.snapshot_created {
        info!(
            snapshot_id = %result.snapshot_id,
            count = result.host_count,
            path = %args.output_dir.join("snapshots").join(&result.snapshot_id).display(),
            "created snapshot"
        );
    } else {
        info!(
            snapshot_id = %result.snapshot_id,
            count = result.host_count,
            path = %args.output_dir.join("snapshots").join(&result.snapshot_id).display(),
            "snapshot already exists, reused"
        );
    }
    info!(
        snapshot_id = %result.snapshot_id,
        count = result.host_count,
        "wrote index.json"
    );
    info!("export complete");
    Ok(())
}

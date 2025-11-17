use aoc_bench::runner;
use clap::Args;
use runner::Runner;
use std::collections::BTreeMap;
use tracing::error;

#[derive(Args, Debug)]
pub struct DebugArgs {
    /// Input file path to provide to the runner
    #[arg(long, value_parser = clap::value_parser!(std::path::PathBuf))]
    pub input: Option<std::path::PathBuf>,

    /// Expected checksum for output validation
    #[arg(long)]
    pub checksum: Option<String>,

    /// Command and arguments to run (after --)
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

pub fn execute(args: DebugArgs) {
    if args.command.is_empty() {
        error!("no command specified");
        std::process::exit(1);
    }

    let cmd = &args.command[0];
    let cmd_args = args.command[1..].to_vec();

    let executable = match which::CanonicalPath::new(cmd) {
        Ok(path) => path,
        Err(error) => {
            error!(%error, "failed to find executable");
            std::process::exit(1);
        }
    };

    let mut runner = Runner::new(executable).with_args(cmd_args);

    if let Some(input_path) = args.input {
        match std::fs::read(&input_path) {
            Ok(input_bytes) => {
                runner = runner.with_stdin_input(input_bytes);
            }
            Err(e) => {
                error!(
                    path = ?input_path,
                    error = %e,
                    "failed to read input file"
                );
                std::process::exit(1);
            }
        }
    }

    if let Some(checksum_str) = args.checksum {
        runner = runner.with_expected_checksum(checksum_str);
    }

    match runner.run_series("debug".to_string(), BTreeMap::new()) {
        Ok(series) => {
            println!("{}", serde_json::to_string_pretty(&series).unwrap());
        }
        Err(e) => {
            error!(error = %e, "benchmark run failed");
            std::process::exit(1);
        }
    }
}

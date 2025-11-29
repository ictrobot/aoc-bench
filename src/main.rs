mod cli;

use crate::cli::{Cli, logging};
use clap::Parser;
use std::process::ExitCode;

fn main() -> ExitCode {
    logging::init();
    Cli::parse().run()
}
